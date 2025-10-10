const express = require('express');
const cors = require('cors');
const rateLimit = require('express-rate-limit');
const { spawn } = require('child_process');
const os = require('os');

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

// Extract best audio URL via yt-dlp
async function getBestAudioUrl(url) {
  // minimal, fast flags; return direct URL (-g)
  const args = ['--ignore-config','--force-ipv4','-g','-f','bestaudio[acodec!=none]/bestaudio', url];
  const { out } = await run('yt-dlp', args, { timeout: 120000 });
  const line = out.split('\n').find(Boolean);
  if (!line) throw new Error('no audio url');
  return line.trim();
}

// POST /extract { url }
app.post('/extract', async (req, res) => {
  try {
    const url = (req.body && req.body.url || '').trim();
    if (!url) return res.status(400).json({ error: 'missing url' });
    const audioUrl = await getBestAudioUrl(url);
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
    const src = req.query.src;
    if (!src) return res.status(400).send('missing src');
    res.setHeader('Content-Type', 'audio/mpeg');
    res.setHeader('Transfer-Encoding', 'chunked');

    const ffArgs = [
      '-loglevel','error','-nostdin',
      '-reconnect','1','-reconnect_streamed','1','-reconnect_on_network_error','1','-reconnect_delay_max','10',
      '-rw_timeout','60000000',
      '-i', src,
      '-vn','-c:a','libmp3lame','-ar','44100','-b:a','192k','-f','mp3','pipe:1'
    ];
    const ff = spawn('ffmpeg', ffArgs, { stdio: ['ignore','pipe','pipe'] });
    ff.stdout.pipe(res);
    ff.stderr.on('data', d => process.stderr.write(d));
    ff.on('close', code => {
      if (code !== 0 && !res.headersSent) res.status(500).end();
      res.end();
    });
  } catch {
    res.status(500).end();
  }
});

// GET /download?url=...
// Download-then-convert (temp file) and send as attachment
app.get('/download', async (req, res) => {
  try {
    const url = (req.query.url || '').trim();
    if (!url) return res.status(400).send('missing url');
    // fetch bestaudio URL
    const audioUrl = await getBestAudioUrl(url);
    // transcode via ffmpeg reading remote input (fast, no disk). For guaranteed speed, you can add pre-download.
    res.setHeader('Content-Type', 'audio/mpeg');
    res.setHeader('Content-Disposition', 'attachment; filename="audio.mp3"');
    const ff = spawn('ffmpeg', ['-loglevel','error','-nostdin','-reconnect','1','-reconnect_streamed','1','-reconnect_on_network_error','1','-reconnect_delay_max','10','-rw_timeout','60000000','-i', audioUrl,'-vn','-c:a','libmp3lame','-ar','44100','-b:a','192k','-f','mp3','pipe:1']);
    ff.stdout.pipe(res);
    ff.stderr.on('data', d => process.stderr.write(d));
    ff.on('close', code => { if (code !== 0 && !res.headersSent) res.status(500).end(); });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

const PORT = process.env.PORT || 3001;
app.listen(PORT, () => console.log(`Fast Node API running on http://localhost:${PORT}`));
