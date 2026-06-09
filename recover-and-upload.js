import 'dotenv/config';

import fs from 'node:fs';
import fsp from 'node:fs/promises';
import path from 'node:path';
import { pipeline } from 'node:stream/promises';
import { Readable } from 'node:stream';
import { parse } from 'csv-parse/sync';

const {
  BUNNY_LIBRARY_ID,
  BUNNY_STREAM_API_KEY,
  DOWNLOAD_DIR = 'downloads',
  RESULTS_CSV = 'results.csv',
  CONCURRENCY = '1',
  DELETE_AFTER_UPLOAD = 'false',
} = process.env;

if (!BUNNY_LIBRARY_ID || !BUNNY_STREAM_API_KEY) {
  console.error('Missing BUNNY_LIBRARY_ID or BUNNY_STREAM_API_KEY in .env');
  process.exit(1);
}

const inputCsv = process.argv[2];

if (!inputCsv) {
  console.error('Usage: npm start -- ./videos.csv');
  process.exit(1);
}

const concurrency = Math.max(1, Number.parseInt(CONCURRENCY, 10) || 1);
const deleteAfterUpload = DELETE_AFTER_UPLOAD.toLowerCase() === 'true';

const BUNNY_BASE_URL = 'https://video.bunnycdn.com';

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

function sanitizeFilename(value) {
  return String(value || '')
    .replace(/[^\w.-]+/g, '_')
    .replace(/^_+|_+$/g, '')
    .slice(0, 120) || 'video';
}

function csvEscape(value) {
  const s = value === undefined || value === null ? '' : String(value);
  if (/[",\n\r]/.test(s)) {
    return `"${s.replaceAll('"', '""')}"`;
  }
  return s;
}

async function ensureDirs() {
  await fsp.mkdir(DOWNLOAD_DIR, { recursive: true });

  if (!fs.existsSync(RESULTS_CSV)) {
    const header = [
      'Video ID',
      'Video Title',
      'Original Status',
      'Stream URL',
      'Result',
      'HTTP Status',
      'Downloaded File',
      'Downloaded Bytes',
      'Bunny Video ID',
      'Bunny Status',
      'Error',
      'Processed At',
    ].join(',');

    await fsp.writeFile(RESULTS_CSV, `${header}\n`);
  }
}

async function appendResult(row) {
  const line = [
    row.videoId,
    row.videoTitle,
    row.originalStatus,
    row.streamUrl,
    row.result,
    row.httpStatus,
    row.downloadedFile,
    row.downloadedBytes,
    row.bunnyVideoId,
    row.bunnyStatus,
    row.error,
    new Date().toISOString(),
  ].map(csvEscape).join(',');

  await fsp.appendFile(RESULTS_CSV, `${line}\n`);
}

function readCsv(filePath) {
  const raw = fs.readFileSync(filePath, 'utf8');

  return parse(raw, {
    columns: true,
    bom: true,
    skip_empty_lines: true,
    trim: true,
    relax_column_count: true,
  });
}

function normalizeRow(row, index) {
  return {
    rowNumber: index + 2,
    videoId: row['Video ID'] || row['video_id'] || row['VideoID'] || '',
    videoTitle: row['Video Title'] || row['title'] || row['Title'] || '',
    originalStatus: row['Status'] || row['status'] || '',
    streamUrl: row['Stream URL'] || row['stream_url'] || row['StreamURL'] || '',
  };
}

function readAlreadyUploaded(resultsPath) {
  if (!fs.existsSync(resultsPath)) return new Set();

  const raw = fs.readFileSync(resultsPath, 'utf8');
  if (!raw.trim()) return new Set();

  let rows = [];

  try {
    rows = parse(raw, {
      columns: true,
      bom: true,
      skip_empty_lines: true,
      trim: true,
      relax_column_count: true,
    });
  } catch {
    return new Set();
  }

  const uploaded = new Set();

  for (const row of rows) {
    if (row.Result === 'uploaded' && row['Video ID']) {
      uploaded.add(row['Video ID']);
    }
  }

  return uploaded;
}

async function fetchWithRetry(url, options = {}, attempts = 3) {
  let lastError;

  for (let i = 1; i <= attempts; i += 1) {
    try {
      const response = await fetch(url, options);

      if (response.status >= 500 && i < attempts) {
        await sleep(1000 * i);
        continue;
      }

      return response;
    } catch (err) {
      lastError = err;

      if (i < attempts) {
        await sleep(1000 * i);
        continue;
      }
    }
  }

  throw lastError;
}

async function downloadVideo(streamUrl, outputPath) {
  const response = await fetchWithRetry(streamUrl, {
    method: 'GET',
    redirect: 'follow',
    headers: {
      // Do not add cache-busting query params.
      // This asks for normal binary content from whatever Bunny edge the droplet reaches.
      'User-Agent': 'castify-bunny-recovery/1.0',
      'Accept': 'video/mp4,application/octet-stream,*/*',
    },
  });

  const contentType = response.headers.get('content-type') || '';
  const contentLength = response.headers.get('content-length') || '';

  if (!response.ok) {
    const body = await response.text().catch(() => '');
    throw new Error(
      `Download failed HTTP ${response.status}. Content-Type=${contentType}. Body=${body.slice(0, 500)}`
    );
  }

  if (contentType.includes('application/json') || contentType.includes('text/html')) {
    const body = await response.text().catch(() => '');
    throw new Error(
      `Download returned non-video response. HTTP ${response.status}. Content-Type=${contentType}. Body=${body.slice(0, 500)}`
    );
  }

  if (!response.body) {
    throw new Error('Download response body is empty');
  }

  await pipeline(
    Readable.fromWeb(response.body),
    fs.createWriteStream(outputPath)
  );

  const stat = await fsp.stat(outputPath);

  if (stat.size === 0) {
    throw new Error('Downloaded file is empty');
  }

  return {
    httpStatus: response.status,
    contentType,
    contentLength,
    bytes: stat.size,
  };
}

async function createBunnyVideo(title) {
  const response = await fetchWithRetry(
    `${BUNNY_BASE_URL}/library/${BUNNY_LIBRARY_ID}/videos`,
    {
      method: 'POST',
      headers: {
        'AccessKey': BUNNY_STREAM_API_KEY,
        'Accept': 'application/json',
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        title: title || 'Recovered video',
      }),
    }
  );

  const text = await response.text();
  let data;

  try {
    data = JSON.parse(text);
  } catch {
    throw new Error(`Create Bunny video returned non-JSON response HTTP ${response.status}: ${text.slice(0, 500)}`);
  }

  if (!response.ok) {
    throw new Error(`Create Bunny video failed HTTP ${response.status}: ${JSON.stringify(data)}`);
  }

  if (!data.guid) {
    throw new Error(`Create Bunny video succeeded but no guid was returned: ${JSON.stringify(data)}`);
  }

  return data;
}

async function uploadToBunny(videoGuid, filePath) {
  const stat = await fsp.stat(filePath);

  const response = await fetchWithRetry(
    `${BUNNY_BASE_URL}/library/${BUNNY_LIBRARY_ID}/videos/${videoGuid}`,
    {
      method: 'PUT',
      headers: {
        'AccessKey': BUNNY_STREAM_API_KEY,
        'Content-Type': 'application/octet-stream',
        'Content-Length': String(stat.size),
      },
      body: fs.createReadStream(filePath),
      duplex: 'half',
    }
  );

  const text = await response.text();

  if (!response.ok) {
    throw new Error(`Bunny upload failed HTTP ${response.status}: ${text.slice(0, 500)}`);
  }

  return {
    status: response.status,
    body: text,
  };
}

async function getBunnyVideo(videoGuid) {
  const response = await fetchWithRetry(
    `${BUNNY_BASE_URL}/library/${BUNNY_LIBRARY_ID}/videos/${videoGuid}`,
    {
      method: 'GET',
      headers: {
        'AccessKey': BUNNY_STREAM_API_KEY,
        'Accept': 'application/json',
      },
    }
  );

  const text = await response.text();

  try {
    return JSON.parse(text);
  } catch {
    return {
      raw: text,
    };
  }
}

async function processVideo(row) {
  if (!row.streamUrl) {
    throw new Error(`Row ${row.rowNumber} has no Stream URL`);
  }

  const baseName = sanitizeFilename(`${row.videoId || row.rowNumber}_${row.videoTitle || 'video'}`);
  const outputPath = path.join(DOWNLOAD_DIR, `${baseName}.mp4`);

  console.log(`\n[${row.rowNumber}] Downloading: ${row.videoTitle || row.videoId}`);
  console.log(`URL: ${row.streamUrl}`);

  const download = await downloadVideo(row.streamUrl, outputPath);

  console.log(`Downloaded ${download.bytes} bytes to ${outputPath}`);

  console.log('Creating Bunny Stream video...');
  const created = await createBunnyVideo(row.videoTitle || row.videoId || baseName);

  console.log(`Created Bunny video: ${created.guid}`);

  console.log('Uploading to Bunny Stream...');
  const upload = await uploadToBunny(created.guid, outputPath);

  console.log(`Upload complete. HTTP ${upload.status}`);

  const bunnyVideo = await getBunnyVideo(created.guid).catch(() => null);

  if (deleteAfterUpload) {
    await fsp.unlink(outputPath).catch(() => {});
  }

  await appendResult({
    videoId: row.videoId,
    videoTitle: row.videoTitle,
    originalStatus: row.originalStatus,
    streamUrl: row.streamUrl,
    result: 'uploaded',
    httpStatus: download.httpStatus,
    downloadedFile: deleteAfterUpload ? '' : outputPath,
    downloadedBytes: download.bytes,
    bunnyVideoId: created.guid,
    bunnyStatus: bunnyVideo?.status ?? '',
    error: '',
  });
}

async function worker(queue, uploadedSet) {
  while (queue.length > 0) {
    const row = queue.shift();

    if (!row) continue;

    if (row.videoId && uploadedSet.has(row.videoId)) {
      console.log(`[skip] Already uploaded Video ID ${row.videoId}`);
      continue;
    }

    try {
      await processVideo(row);
    } catch (err) {
      console.error(`[failed] Row ${row.rowNumber}: ${err.message}`);

      await appendResult({
        videoId: row.videoId,
        videoTitle: row.videoTitle,
        originalStatus: row.originalStatus,
        streamUrl: row.streamUrl,
        result: 'failed',
        httpStatus: '',
        downloadedFile: '',
        downloadedBytes: '',
        bunnyVideoId: '',
        bunnyStatus: '',
        error: err.message,
      });
    }
  }
}

async function main() {
  await ensureDirs();

  const uploadedSet = readAlreadyUploaded(RESULTS_CSV);
  const rows = readCsv(inputCsv).map(normalizeRow);

  console.log(`Loaded ${rows.length} rows from ${inputCsv}`);
  console.log(`Already uploaded from ${RESULTS_CSV}: ${uploadedSet.size}`);
  console.log(`Concurrency: ${concurrency}`);

  const queue = [...rows];
  const workers = Array.from({ length: concurrency }, () => worker(queue, uploadedSet));

  await Promise.all(workers);

  console.log('\nDone.');
  console.log(`Results written to ${RESULTS_CSV}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});