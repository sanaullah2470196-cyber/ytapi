const express = require('express');
const cors = require('cors');
const rateLimit = require('express-rate-limit');
const { spawn } = require('child_process');
const os = require('os');
const fs = require('fs');
const path = require('path');

const app = express();
app.use(express.json());
app.use(cors({ origin: '*', methods: ['GET','POST','OPTIONS'], allowedHeaders: ['Content-Type','X-API-Key'] }));

const limiter = rateLimit({ windowMs: 1000, max: 50 });
app.use(limiter);

// Helper to run a command and capture output
function run(cmd, args, opts={}) {
  return new Promise((resolve, reject) => {
    const p = spawn(cmd, args, { stdio: ['ignore','pipe','pipe'], ...opts });
    let out = '', err = '';
    p.stdout.on('data', d => out += d.toString());
    p.stderr.on('data', d => err += d.toString());
    p.on('error', reject);
    p.on('close', code => code === 0 ? resolve({ out, err }) : reject(new Error(err || `exit ${code}`)));
  });
}

// Simple logging helpers
function now() { return new Date().toISOString(); }
function jid() { return (Date.now().toString(36)+Math.random().toString(36).slice(2,6)); }
function info(jobId, evt, extra={}) {
  console.log(JSON.stringify({ ts: now(), level: 'info', jobId, evt, ...extra }));
}
function warn(jobId, evt, extra={}) {
  console.warn(JSON.stringify({ ts: now(), level: 'warn', jobId, evt, ...extra }));
}
function err(jobId, evt, extra={}) {
  console.error(JSON.stringify({ ts: now(), level: 'error', jobId, evt, ...extra }));
}

// Extract best audio URL via yt-dlp
async function getBestAudioUrl(url) {
  // minimal, fast flags; return direct URL (-g)
  const args = ['--ignore-config','--force-ipv4','-g','-f','bestaudio[acodec!=none]/bestaudio', url];
  const { out } = await run('yt-dlp', args, { timeout: 120000 });
  const line = out.split('\n').find(Boolean);
  if (!line) throw new Error('no audio url');
  return line.trim();
}

// Download bestaudio to a temp file using yt-dlp, return absolute file path
async function downloadBestAudioFile(url) {
  const tmpDir = await fs.promises.mkdtemp(path.join(os.tmpdir(), 'fast-node-api-'));
  const outTmpl = path.join(tmpDir, 'audio.%(ext)s');
  const N = process.env.YTDLP_N || '12';
  const args = [
    '--ignore-config','--force-ipv4','-f','bestaudio/best','-N', N,
    '-o', outTmpl, '--no-playlist','--no-warnings', url
  ];
  await run('yt-dlp', args, { timeout: 30 * 60 * 1000 });
  const files = await fs.promises.readdir(tmpDir);
  const src = files.find(f => f.startsWith('audio.'));
  if (!src) throw new Error('download produced no file');
  return { tmpDir, srcPath: path.join(tmpDir, src) };
}

// POST /extract { url }
app.post('/extract', async (req, res) => {
  try {
    const jobId = jid();
    const url = (req.body && req.body.url || '').trim();
    if (!url) return res.status(400).json({ error: 'missing url' });
    info(jobId, 'extract_received', { url });
    const audioUrl = await getBestAudioUrl(url);
    info(jobId, 'extract_resolved', { audioUrl });
    // instant response: stream-transcode endpoint
    return res.json({
      stream_transcode: `/stream?src=${encodeURIComponent(audioUrl)}`,
      direct_audio_url: audioUrl
    });
  } catch (e) {
    return res.status(500).json({ error: e.message });
  }
});

// GET /stream?src=...
// Streams remote audio through ffmpeg as MP3 on the fly (no temp file)
app.get('/stream', async (req, res) => {
  try {
    const jobId = jid();
    const src = req.query.src;
    if (!src) return res.status(400).send('missing src');
    info(jobId, 'stream_start', { src });
    res.setHeader('Content-Type', 'audio/mpeg');
    res.setHeader('Transfer-Encoding', 'chunked');

    const ffArgs = [
      '-loglevel','error','-nostdin','-progress','pipe:2',
      '-reconnect','1','-reconnect_streamed','1','-reconnect_on_network_error','1','-reconnect_delay_max','10',
      '-rw_timeout','60000000',
      '-i', src,
      '-vn','-c:a','libmp3lame','-ar','44100','-b:a','192k','-f','mp3','pipe:1'
    ];
    const ff = spawn('ffmpeg', ffArgs, { stdio: ['ignore','pipe','pipe'] });
    ff.stdout.pipe(res);
    let lastLog = 0;
    ff.stderr.on('data', d => {
      const lines = d.toString().split(/\r?\n/);
      const nowTs = Date.now();
      for (const line of lines) {
        if (line.startsWith('out_time_ms=')) {
          if (nowTs - lastLog > 1000) { // throttle 1s
            info(jobId, 'ffmpeg_progress', { out_time_ms: line.split('=')[1] });
            lastLog = nowTs;
          }
        }
      }
    });
    ff.on('close', code => {
      if (code !== 0 && !res.headersSent) {
        err(jobId, 'stream_failed', { code });
        res.status(500).end();
      } else {
        info(jobId, 'stream_done', { code });
      }
      res.end();
    });
  } catch {
    res.status(500).end();
  }
});

// GET /download?url=...
// Download-then-convert (temp file) and send as attachment
app.get('/download', async (req, res) => {
  let tmpDir; let srcPath; let ff; const jobId = jid();
  try {
    const url = (req.query.url || '').trim();
    if (!url) return res.status(400).send('missing url');

    // 1) Download bestaudio first (fast via -N concurrency)
    const t0 = Date.now();
    info(jobId, 'download_start', { url });
    const dl = await downloadBestAudioFile(url);
    tmpDir = dl.tmpDir; srcPath = dl.srcPath;
    info(jobId, 'download_done', { ms: Date.now() - t0, srcPath });

    // 2) Convert local file to MP3 and stream to client
    res.setHeader('Content-Type', 'audio/mpeg');
    res.setHeader('Content-Disposition', 'attachment; filename="audio.mp3"');
    const args = [
      '-loglevel','error','-nostdin','-progress','pipe:2','-i', srcPath,
      '-vn','-c:a','libmp3lame','-ar','44100','-b:a','192k','-f','mp3','pipe:1'
    ];
    ff = spawn('ffmpeg', args, { stdio: ['ignore','pipe','pipe'] });
    ff.stdout.pipe(res);
    let lastLog = 0; const t1 = Date.now();
    info(jobId, 'ffmpeg_start', {});
    ff.stderr.on('data', d => {
      const lines = d.toString().split(/\r?\n/);
      const nowTs = Date.now();
      for (const line of lines) {
        if (line.startsWith('out_time_ms=')) {
          if (nowTs - lastLog > 1000) {
            info(jobId, 'ffmpeg_progress', { out_time_ms: line.split('=')[1] });
            lastLog = nowTs;
          }
        }
      }
    });

    const cleanup = async () => {
      try { if (srcPath) await fs.promises.unlink(srcPath).catch(()=>{}); } catch {}
      try { if (tmpDir) await fs.promises.rm(tmpDir, { recursive: true, force: true }); } catch {}
    };

    res.on('close', async () => {
      try { if (ff && !ff.killed) ff.kill('SIGTERM'); } catch {}
      await cleanup();
    });

    ff.on('close', async (code) => {
      info(jobId, 'ffmpeg_done', { ms: Date.now() - t1, code });
      if (code !== 0 && !res.headersSent) res.status(500).end();
      await cleanup();
    });
  } catch (e) {
    try {
      if (ff && !ff.killed) ff.kill('SIGTERM');
      if (srcPath) await fs.promises.unlink(srcPath).catch(()=>{});
      if (tmpDir) await fs.promises.rm(tmpDir, { recursive: true, force: true });
    } catch {}
    err(jobId, 'download_failed', { error: e.message });
    res.status(500).json({ error: e.message });
  }
});

const PORT = process.env.PORT || 3001;
app.listen(PORT, () => console.log(`Fast Node API running on http://localhost:${PORT}`));
