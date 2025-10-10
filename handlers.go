package main

import (
    "encoding/json"
    "fmt"
    "io"
    "net/http"
    "os"
    "path/filepath"
    "sync/atomic"
    "time"

    "github.com/google/uuid"
    "strings"
    "strconv"
    "sync"
    "golang.org/x/time/rate"
)

func handleExtract(w http.ResponseWriter, r *http.Request) {
    enableCORS(w, r)

    if r.Method == http.MethodOptions {
        w.WriteHeader(http.StatusOK)
        return
    }
    if atomic.LoadInt32(&intakePaused) == 1 {
        w.Header().Set("Retry-After", "5")
        http.Error(w, "Intake paused by admin", http.StatusServiceUnavailable)
        return
    }
    if r.Method != http.MethodPost {
        http.Error(w, "Invalid request method", http.StatusMethodNotAllowed)
        return
    }

    var req Request
    if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
        http.Error(w, "Invalid JSON", http.StatusBadRequest)
        return
    }
    if req.URL == "" || len(req.URL) > MaxURLLength {
        http.Error(w, "Missing YouTube URL", http.StatusBadRequest)
        return
    }

    if !isValidYouTubeURL(req.URL) {
        http.Error(w, "Invalid YouTube URL", http.StatusBadRequest)
        return
    }

    // Canonicalize URL (supports Shorts, embed, youtu.be, mobile)
    var videoID string
    if canon, ok := canonicalizeYouTubeURL(req.URL); ok {
        req.URL = canon
        if vid, ok2 := extractYouTubeVideoID(canon); ok2 {
            videoID = vid
        }
    }

    logInfof("extract_received job_id=~pending url=%s", req.URL)
    // Idempotency key check
    if req.IdempotencyKey != "" {
        if jid, err := getJobIDByIdempotency(req.IdempotencyKey); err == nil && jid != "" {
            if j, err2 := getJobFromRedis(jid); err2 == nil && j != nil {
                w.Header().Set("Content-Type", "application/json")
                json.NewEncoder(w).Encode(map[string]interface{}{
                    "job_id": j.ID,
                    "status": string(j.Status),
                    "download_url": j.DownloadURL,
                    "check_status_endpoint": fmt.Sprintf("http://localhost:8080/status/%s", j.ID),
                    "canonical_url": j.URL,
                })
                return
            }
        }
    }

    // Prefer Redis-based dedupe first
    if jobIDFromURL, err := getJobIDByURL(req.URL); err == nil && jobIDFromURL != "" {
        if jobByRedis, err2 := getJobFromRedis(jobIDFromURL); err2 == nil && jobByRedis != nil && jobByRedis.Status == StatusCompleted {
            w.Header().Set("Content-Type", "application/json")
            json.NewEncoder(w).Encode(map[string]string{
                "job_id": jobByRedis.ID,
                "status": string(jobByRedis.Status),
                "download_url": jobByRedis.DownloadURL,
                "check_status_endpoint": fmt.Sprintf("http://localhost:8080/status/%s", jobByRedis.ID),
            })
            return
        }
    }
    existingJob := findJobByURL(req.URL)
    if existingJob != nil && existingJob.Status == StatusCompleted {
        w.Header().Set("Content-Type", "application/json")
        json.NewEncoder(w).Encode(map[string]string{
            "job_id": existingJob.ID,
            "status": string(existingJob.Status),
            "download_url": existingJob.DownloadURL,
            "check_status_endpoint": fmt.Sprintf("http://localhost:8080/status/%s", existingJob.ID),
        })
        return
    }

    jobID := uuid.New().String()
    job := &ConversionJob{
        ID:         jobID,
        URL:        req.URL,
        VideoID:    videoID,
        Status:     StatusPending,
        CreatedAt:  time.Now(),
        MaxRetries: MaxJobRetries,
        Priority:   1,
        CallbackURL: req.CallbackURL,
    }

    jobStore.Lock()
    jobStore.jobs[jobID] = job
    jobStore.Unlock()

    saveJobToRedis(job)
    _ = saveURLMapping(req.URL, jobID)
    if req.IdempotencyKey != "" {
        _ = saveIdempotencyKey(req.IdempotencyKey, jobID)
    }
    atomic.AddInt64(&queuedJobs, 1)
    // derive queue length from channel length for accuracy
    qlen := len(jobQueue)
    if qlen < 0 { qlen = 0 }
    logInfof("queued job_id=%s queue_len=%d", jobID, qlen)

    resultCh := registerJobWaiter(jobID)

    select {
    case jobQueue <- job:
        w.Header().Set("Content-Type", "application/json")
        select {
        case doneJob := <-resultCh:
            if doneJob.Status == StatusCompleted {
                json.NewEncoder(w).Encode(map[string]string{
                    "job_id": jobID,
                    "status": string(doneJob.Status),
                    "download_url": doneJob.DownloadURL,
                    "check_status_endpoint": fmt.Sprintf("http://localhost:8080/status/%s", jobID),
                    "canonical_url": job.URL,
                })
            } else {
                // Do not surface immediate error; let background retries handle it.
                json.NewEncoder(w).Encode(map[string]interface{}{
                    "job_id": jobID,
                    "status": string(StatusProcessing),
                    "check_status_endpoint": fmt.Sprintf("http://localhost:8080/status/%s", jobID),
                    "canonical_url": job.URL,
                })
            }
        case <-time.After(FastPathWait):
            unregisterJobWaiter(jobID, resultCh)
            json.NewEncoder(w).Encode(map[string]string{
                "job_id": jobID,
                "status": string(job.Status),
                "check_status_endpoint": fmt.Sprintf("http://localhost:8080/status/%s", jobID),
                "canonical_url": job.URL,
            })
        }
    default:
        unregisterJobWaiter(jobID, resultCh)
        jobStore.Lock()
        delete(jobStore.jobs, jobID)
        jobStore.Unlock()
        atomic.AddInt64(&queuedJobs, -1)
        w.Header().Set("Retry-After", "1")
        http.Error(w, "Server busy, please try again later.", http.StatusServiceUnavailable)
    }
}

func handleStatus(w http.ResponseWriter, r *http.Request) {
    enableCORS(w, r)

    if r.Method == http.MethodOptions {
        w.WriteHeader(http.StatusOK)
        return
    }

    jobID := filepath.Base(r.URL.Path)
    if jobID == "" {
        http.Error(w, "Missing job ID", http.StatusBadRequest)
        return
    }

    job, err := getJobFromRedis(jobID)
    if err != nil || job == nil {
        jobStore.RLock()
        jobMem, exists := jobStore.jobs[jobID]
        jobStore.RUnlock()
        if !exists {
            http.Error(w, "Job not found", http.StatusNotFound)
            return
        }
        job = jobMem
    }

    response := struct {
        JobID       string    `json:"job_id"`
        Status      JobStatus `json:"status"`
        Progress    string    `json:"progress,omitempty"`
        DownloadURL string    `json:"download_url,omitempty"`
        Error       string    `json:"error,omitempty"`
        Metadata    *Metadata `json:"metadata,omitempty"`
        CreatedAt   time.Time `json:"created_at"`
        CompletedAt time.Time `json:"completed_at,omitempty"`
    }{
        JobID:       job.ID,
        Status:      job.Status,
        DownloadURL: job.DownloadURL,
        Error:       job.Error,
        Metadata:    job.Metadata,
        CreatedAt:   job.CreatedAt,
        CompletedAt: job.CompletedAt,
    }

    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(response)
}

func handleDownload(w http.ResponseWriter, r *http.Request) {
    enableCORS(w, r)

    if r.Method == http.MethodOptions {
        w.WriteHeader(http.StatusOK)
        return
    }

    filenameWithExt := filepath.Base(r.URL.Path)
    if !strings.HasSuffix(filenameWithExt, ".mp3") {
        http.Error(w, "Invalid filename", http.StatusBadRequest)
        return
    }
    jobID := filenameWithExt[:len(filenameWithExt)-len(".mp3")]

    job, err := getJobFromRedis(jobID)
    if err != nil || job == nil {
        jobStore.RLock()
        job, exists := jobStore.jobs[jobID]
        jobStore.RUnlock()
        if !exists || job.Status != StatusCompleted {
            http.Error(w, "File not found or conversion not completed", http.StatusNotFound)
            return
        }
    }

    if job.FilePath == "" {
        http.Error(w, "File path not available", http.StatusInternalServerError)
        return
    }

    // Mark download in progress to avoid deletion during streaming
    downloadTrackers.Lock()
    downloadTrackers.inProgress[job.ID]++
    downloadTrackers.Unlock()

    file, err := os.Open(job.FilePath)
    if err != nil {
        downloadTrackers.Lock()
        downloadTrackers.inProgress[job.ID]--
        if downloadTrackers.inProgress[job.ID] <= 0 { delete(downloadTrackers.inProgress, job.ID) }
        downloadTrackers.Unlock()
        http.Error(w, "Error opening file", http.StatusInternalServerError)
        return
    }
    defer func() {
        file.Close()
        downloadTrackers.Lock()
        downloadTrackers.inProgress[job.ID]--
        if downloadTrackers.inProgress[job.ID] <= 0 { delete(downloadTrackers.inProgress, job.ID) }
        downloadTrackers.Unlock()
    }()

    // Range support
    fi, _ := file.Stat()
    size := fi.Size()
    w.Header().Set("Accept-Ranges", "bytes")
    w.Header().Set("Content-Type", "audio/mpeg")
    w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"%s\"", filenameWithExt))
    w.Header().Set("Cache-Control", "public, max-age=3600")

    if rng := r.Header.Get("Range"); rng != "" {
        // Simple bytes=START-
        if strings.HasPrefix(rng, "bytes=") {
            parts := strings.TrimPrefix(rng, "bytes=")
            if strings.HasSuffix(parts, "-") {
                startStr := strings.TrimSuffix(parts, "-")
                if start, err := strconv.ParseInt(startStr, 10, 64); err == nil && start < size {
                    w.WriteHeader(http.StatusPartialContent)
                    w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, size-1, size))
                    file.Seek(start, 0)
                    io.Copy(w, file)
                    return
                }
            }
        }
    }

    // Full body
    w.Header().Set("Content-Length", fmt.Sprintf("%d", size))
    io.Copy(w, file)

    // Schedule deletion 10 minutes after first successful download
    if job.FirstDownloadedAt.IsZero() {
        job.FirstDownloadedAt = time.Now()
        saveJobToRedis(job)
        // Schedule deletion if not already scheduled
        downloadTrackers.Lock()
        already := downloadTrackers.scheduled[job.ID]
        if !already { downloadTrackers.scheduled[job.ID] = true }
        downloadTrackers.Unlock()
        if !already {
            go scheduleSafeDeletion(job)
        }
    }
}

// Simple docs pages
func handleDocs(w http.ResponseWriter, r *http.Request) {
    enableCORS(w, r)
    if r.Method != http.MethodGet { http.Error(w, "Method not allowed", http.StatusMethodNotAllowed); return }
    w.Header().Set("Content-Type", "text/html; charset=utf-8")
    io.WriteString(w, `<!doctype html><html><head><meta charset="utf-8"><title>YT MP3 API Docs</title><style>body{font-family:sans-serif;max-width:900px;margin:2rem auto;padding:0 1rem;}</style></head><body>
    <h1>YouTube to MP3 API - Documentation</h1>
    <p>High-level guide for backend integration.</p>
    <h2>Endpoints</h2>
    <ul>
      <li><code>POST /extract</code> - Start conversion. Body: { url, idempotency_key?, callback_url? }</li>
      <li><code>GET /status/{job_id}</code> - Check job status.</li>
      <li><code>GET /download/{job_id}.mp3</code> - Download MP3 (Range supported).</li>
      <li><code>GET /health</code>, <code>/metrics</code>, <code>/stats</code> - Monitoring.</li>
    </ul>
    <h2>Auth</h2>
    <p>If enabled, send <code>X-API-Key: &lt;your_key&gt;</code> header.</p>
    <h2>Notes</h2>
    <ul>
      <li>Provide valid YouTube URL (shorts and embed supported).</li>
      <li>Repeated requests for same video are deduped.</li>
      <li>Files are short-lived and may be deleted ~10 minutes after completion.</li>
    </ul>
    <p>Frontend-focused docs: <a href="/docs/frontend">/docs/frontend</a></p>
    </body></html>`)
}

// Admin Docs page with deployment, endpoints, and client examples
func handleAdminDocs(w http.ResponseWriter, r *http.Request) {
    if r.Method != http.MethodGet { http.Error(w, "Method not allowed", http.StatusMethodNotAllowed); return }
    w.Header().Set("Content-Type", "text/html; charset=utf-8")
    io.WriteString(w, `<!doctype html><html><head><meta charset="utf-8"><title>API Docs</title>
    <script src="https://cdn.tailwindcss.com"></script>
    </head><body class="bg-gray-50">
    <div class="max-w-5xl mx-auto p-6">
      <div class="flex items-center justify-between mb-6">
        <h1 class="text-2xl font-semibold">Full Documentation</h1>
        <nav class="space-x-4 text-blue-600">
          <a class="hover:underline" href="/admin">Home</a>
          <a class="hover:underline" href="/admin/playground">API Playground</a>
          <a class="hover:underline" href="/admin/liveops">Live Ops</a>
          <a class="hover:underline" href="/admin/jobs">Jobs</a>
          <a class="hover:underline" href="/admin/docs">Docs</a>
        </nav>
      </div>

      <div class="prose max-w-none">
        <h2>Deploy</h2>
        <p>Ubuntu 24.04+ recommended. Run installer:</p>
        <pre class="bg-gray-900 text-gray-100 p-3 rounded overflow-x-auto"><code>sudo bash scripts/install_ubuntu.sh
# Follow prompts for workers, queue, CORS, Redis, etc.
sudo systemctl status ytmp3-api
sudo systemctl restart ytmp3-api
# Logs
journalctl -u ytmp3-api -f</code></pre>

        <h2>Endpoints</h2>
        <ul>
          <li><code>POST /extract</code> { url, idempotency_key?, callback_url? }</li>
          <li><code>GET /status/{job_id}</code></li>
          <li><code>GET /download/{job_id}.mp3</code> (Range supported)</li>
          <li><code>DELETE /delete/{job_id}</code></li>
          <li><code>GET /health</code>, <code>/metrics</code>, <code>/metrics/prom</code>, <code>/stats</code></li>
        </ul>
        <p>If API key auth enabled, send header: <code>X-API-Key: YOUR_KEY</code>.</p>

        <h2>cURL</h2>
        <pre class="bg-gray-900 text-gray-100 p-3 rounded overflow-x-auto"><code># Create job
curl -s -X POST http://localhost:8080/extract \
  -H 'Content-Type: application/json' \
  -d '{"url":"https://www.youtube.com/watch?v=I7m7m4OMapE"}'

# Poll status
curl -s http://localhost:8080/status/JOB_ID

# Download
curl -L -o out.mp3 http://localhost:8080/download/JOB_ID.mp3

# Delete
curl -X DELETE http://localhost:8080/delete/JOB_ID</code></pre>

        <h2>React (fetch)</h2>
        <pre class="bg-gray-900 text-gray-100 p-3 rounded overflow-x-auto"><code>async function start(url){
  const r = await fetch('/extract',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({url})});
  const {job_id} = await r.json();
  let status;
  do {
    await new Promise(r=>setTimeout(r,2000));
    status = await fetch('/status/'+job_id).then(r=>r.json());
  } while(status.status !== 'completed' && status.status !== 'failed');
  if(status.download_url) window.location = status.download_url;
}</code></pre>

        <h2>Vanilla JS</h2>
        <pre class="bg-gray-900 text-gray-100 p-3 rounded overflow-x-auto"><code>fetch('/extract',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({url})})
 .then(r=>r.json()).then(({job_id})=>{
   const t=setInterval(async()=>{
     const s=await fetch('/status/'+job_id).then(r=>r.json());
     if(s.status==='completed'){ clearInterval(t); location=s.download_url; }
   },2000);
 });</code></pre>

        <h2>Node.js (axios)</h2>
        <pre class="bg-gray-900 text-gray-100 p-3 rounded overflow-x-auto"><code>const axios = require('axios');
async function run(){
  const {data:{job_id}} = await axios.post('http://localhost:8080/extract',{url:'YOUTUBE_URL'});
  while(true){
    const {data:s} = await axios.get('http://localhost:8080/status/'+job_id);
    if(s.status==='completed'){ console.log(s.download_url); break; }
    await new Promise(r=>setTimeout(r,2000));
  }
}
run();</code></pre>

        <h2>Python (requests)</h2>
        <pre class="bg-gray-900 text-gray-100 p-3 rounded overflow-x-auto"><code>import time, requests
r = requests.post('http://localhost:8080/extract', json={'url': 'YOUTUBE_URL'})
job_id = r.json()['job_id']
while True:
    s = requests.get(f'http://localhost:8080/status/{job_id}').json()
    if s['status'] in ('completed','failed'):
        print(s)
        break
    time.sleep(2)</code></pre>

        <h2>PHP (cURL)</h2>
        <pre class="bg-gray-900 text-gray-100 p-3 rounded overflow-x-auto"><code><?php
$ch = curl_init('http://localhost:8080/extract');
curl_setopt_array($ch,[CURLOPT_POST=>1,CURLOPT_HTTPHEADER=>['Content-Type: application/json'],CURLOPT_POSTFIELDS=>json_encode(['url'=>'YOUTUBE_URL']),CURLOPT_RETURNTRANSFER=>1]);
$resp = json_decode(curl_exec($ch), true);
$job = $resp['job_id'];
do {
  sleep(2);
  $s = json_decode(file_get_contents('http://localhost:8080/status/'.$job), true);
} while(!in_array($s['status'], ['completed','failed']));
if(isset($s['download_url'])) header('Location: '.$s['download_url']);
?></code></pre>

        <h2>Configuration notes</h2>
        <ul>
          <li>CORS via <code>ALLOWED_ORIGINS</code></li>
          <li>Auth via <code>REQUIRE_API_KEY</code>, send <code>X-API-Key</code></li>
          <li>Performance: <code>WORKER_POOL_SIZE</code>, <code>YTDLP_* </code>, <code>FFMPEG_* </code></li>
        </ul>
      </div>
    </div>
    </body></html>`)
}

func handleDocsFrontend(w http.ResponseWriter, r *http.Request) {
    enableCORS(w, r)
    if r.Method != http.MethodGet { http.Error(w, "Method not allowed", http.StatusMethodNotAllowed); return }
    w.Header().Set("Content-Type", "text/html; charset=utf-8")
    io.WriteString(w, `<!doctype html><html><head><meta charset="utf-8"><title>Frontend Integration</title><style>body{font-family:sans-serif;max-width:900px;margin:2rem auto;padding:0 1rem;}</style></head><body>
    <h1>Frontend Integration</h1>
    <p>Use fetch with CORS. Example:</p>
    <pre><code>fetch('/extract',{method:'POST',headers:{'Content-Type':'application/json','X-API-Key':'YOUR_KEY'},body:JSON.stringify({url})})
 .then(r=>r.json())
 .then(({job_id})=>pollStatus(job_id))</code></pre>
    <p>Poll status every few seconds. When status = completed, navigate to <code>/download/{job_id}.mp3</code>.</p>
    </body></html>`)
}

// Minimal admin dashboard (basic auth protected)
func handleAdmin(w http.ResponseWriter, r *http.Request) {
    if r.Method != http.MethodGet { http.Error(w, "Method not allowed", http.StatusMethodNotAllowed); return }
    w.Header().Set("Content-Type", "text/html; charset=utf-8")
    io.WriteString(w, `<!doctype html><html><head><meta charset="utf-8"><title>Admin</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <style>table{border-collapse:collapse}td,th{border:1px solid #e5e7eb;padding:6px}</style>
    <script>
    async function refresh(){
      const h = await fetch('/health',{headers:{'Authorization':localStorage.auth||''}}).then(r=>r.json()).catch(()=>({}));
      const m = await fetch('/metrics',{headers:{'Authorization':localStorage.auth||''}}).then(r=>r.json()).catch(()=>({}));
      document.getElementById('health').textContent = JSON.stringify(h,null,2);
      document.getElementById('metrics').textContent = JSON.stringify(m,null,2);
    }
    setInterval(refresh, 3000);
    window.onload=refresh;
    </script></head><body class="bg-gray-50">
    <div class="max-w-6xl mx-auto p-6">
      <div class="flex items-center justify-between mb-6">
        <h1 class="text-2xl font-semibold">Admin Dashboard</h1>
        <nav class="space-x-4 text-blue-600">
          <a class="hover:underline" href="/admin">Home</a>
          <a class="hover:underline" href="/admin/playground">API Playground</a>
          <a class="hover:underline" href="/admin/liveops">Live Ops</a>
          <a class="hover:underline" href="/admin/jobs">Jobs</a>
          <a class="hover:underline" href="/admin/docs">Docs</a>
        </nav>
      </div>
      <p class="mb-4 text-gray-600">Live server state, health, and metrics.</p>
      <div class="grid grid-cols-1 md:grid-cols-2 gap-6">
        <div class="bg-white rounded shadow p-4">
          <h2 class="font-medium mb-2">Health</h2>
          <pre id="health" class="text-sm bg-gray-100 p-3 rounded">loading...</pre>
        </div>
        <div class="bg-white rounded shadow p-4">
          <h2 class="font-medium mb-2">Metrics</h2>
          <pre id="metrics" class="text-sm bg-gray-100 p-3 rounded">loading...</pre>
        </div>
      </div>
    </div>
    </body></html>`)
}

// API Playground page
func handleAdminPlayground(w http.ResponseWriter, r *http.Request) {
    if r.Method != http.MethodGet { http.Error(w, "Method not allowed", http.StatusMethodNotAllowed); return }
    w.Header().Set("Content-Type", "text/html; charset=utf-8")
    io.WriteString(w, `<!doctype html><html><head><meta charset="utf-8"><title>API Playground</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <script>
    async function postExtract(){
      const url = document.getElementById('url').value.trim();
      if(!url) return;
      const resp = await fetch('/extract',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({url})}).then(r=>r.json());
      document.getElementById('extractResp').textContent = JSON.stringify(resp,null,2);
    }
    async function pollStatus(){
      const id = document.getElementById('jobid').value.trim();
      if(!id) return;
      const resp = await fetch('/status/'+id).then(r=>r.json());
      document.getElementById('statusResp').textContent = JSON.stringify(resp,null,2);
    }
    async function triggerDownload(){
      const id = document.getElementById('jobid').value.trim();
      if(!id) return; window.location='/download/'+id+'.mp3';
    }
    </script></head>
    <body class="bg-gray-50">
    <div class="max-w-5xl mx-auto p-6">
      <div class="flex items-center justify-between mb-6">
        <h1 class="text-2xl font-semibold">API Playground</h1>
        <nav class="space-x-4 text-blue-600">
          <a class="hover:underline" href="/admin">Home</a>
          <a class="hover:underline" href="/admin/playground">API Playground</a>
          <a class="hover:underline" href="/admin/liveops">Live Ops</a>
          <a class="hover:underline" href="/admin/jobs">Jobs</a>
          <a class="hover:underline" href="/admin/docs">Docs</a>
        </nav>
      </div>
      <div class="bg-white rounded shadow p-4 mb-6">
        <h2 class="font-medium mb-2">Extract</h2>
        <div class="flex gap-2 mb-3"><input id="url" class="flex-1 border p-2 rounded" placeholder="YouTube URL"><button onclick="postExtract()" class="px-3 py-2 bg-blue-600 text-white rounded">POST /extract</button></div>
        <pre id="extractResp" class="text-sm bg-gray-100 p-3 rounded">{}</pre>
      </div>
      <div class="bg-white rounded shadow p-4">
        <h2 class="font-medium mb-2">Status & Download</h2>
        <div class="flex gap-2 mb-3"><input id="jobid" class="flex-1 border p-2 rounded" placeholder="job_id"><button onclick="pollStatus()" class="px-3 py-2 bg-blue-600 text-white rounded">GET /status/{job_id}</button><button onclick="triggerDownload()" class="px-3 py-2 bg-green-600 text-white rounded">GET /download/{job_id}.mp3</button></div>
        <pre id="statusResp" class="text-sm bg-gray-100 p-3 rounded">{}</pre>
      </div>
    </div>
    </body></html>`)
}

// Live Ops page
func handleAdminLiveOps(w http.ResponseWriter, r *http.Request) {
    if r.Method != http.MethodGet { http.Error(w, "Method not allowed", http.StatusMethodNotAllowed); return }
    w.Header().Set("Content-Type", "text/html; charset=utf-8")
    io.WriteString(w, `<!doctype html><html><head><meta charset="utf-8"><title>Live Ops</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <script>
    async function pauseIntake(){ await fetch('/admin/api/pause', {method:'POST'}).then(()=>loadState()).catch(()=>{}); }
    async function resumeIntake(){ await fetch('/admin/api/resume', {method:'POST'}).then(()=>loadState()).catch(()=>{}); }
    async function clearQueue(){ await fetch('/admin/api/queue/clear',{method:'POST'}).then(()=>loadState()); }
    async function bumpWorkers(delta){ await fetch('/admin/api/workers?delta='+delta,{method:'POST'}).then(()=>loadState()); }
    async function setRate(){ const rps = document.getElementById('rps').value; const burst = document.getElementById('burst').value; await fetch('/admin/api/rate',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({rps:Number(rps),burst:Number(burst)})}); loadState(); }
    async function loadState(){ const s = await fetch('/admin/api/state').then(r=>r.json()).catch(()=>({})); document.getElementById('state').textContent=JSON.stringify(s,null,2); document.getElementById('rps').value = s.rate_limit||''; document.getElementById('burst').value = s.burst||''; }
    window.onload = loadState;
    </script></head>
    <body class="bg-gray-50">
    <div class="max-w-5xl mx-auto p-6">
      <div class="flex items-center justify-between mb-6">
        <h1 class="text-2xl font-semibold">Live Ops</h1>
        <nav class="space-x-4 text-blue-600">
          <a class="hover:underline" href="/admin">Home</a>
          <a class="hover:underline" href="/admin/playground">API Playground</a>
          <a class="hover:underline" href="/admin/liveops">Live Ops</a>
          <a class="hover:underline" href="/admin/jobs">Jobs</a>
          <a class="hover:underline" href="/admin/docs">Docs</a>
        </nav>
      </div>
      <div class="bg-white rounded shadow p-4">
        <h2 class="font-medium mb-2">Controls</h2>
        <div class="flex gap-3">
          <button onclick="pauseIntake()" class="px-3 py-2 bg-yellow-500 text-white rounded">Pause Intake</button>
          <button onclick="resumeIntake()" class="px-3 py-2 bg-green-600 text-white rounded">Resume Intake</button>
          <button onclick="clearQueue()" class="px-3 py-2 bg-red-600 text-white rounded">Clear Queue</button>
        </div>
        <div class="mt-4 grid grid-cols-1 md:grid-cols-2 gap-4">
          <div>
            <h3 class="font-medium mb-2">Worker pool</h3>
            <div class="flex gap-2">
              <button onclick="bumpWorkers(1)" class="px-3 py-2 bg-blue-600 text-white rounded">+1</button>
              <button onclick="bumpWorkers(-1)" class="px-3 py-2 bg-blue-600 text-white rounded">-1</button>
            </div>
          </div>
          <div>
            <h3 class="font-medium mb-2">Rate limit</h3>
            <div class="flex items-center gap-2">
              <input id="rps" class="border p-2 rounded w-24" placeholder="rps">
              <input id="burst" class="border p-2 rounded w-24" placeholder="burst">
              <button onclick="setRate()" class="px-3 py-2 bg-indigo-600 text-white rounded">Apply</button>
            </div>
          </div>
        </div>
        <h3 class="font-medium mt-4">State</h3>
        <pre id="state" class="text-sm bg-gray-100 p-3 rounded">{}</pre>
      </div>
    </div>
    </body></html>`)
}

// Jobs page
func handleAdminJobsPage(w http.ResponseWriter, r *http.Request) {
    if r.Method != http.MethodGet { http.Error(w, "Method not allowed", http.StatusMethodNotAllowed); return }
    w.Header().Set("Content-Type", "text/html; charset=utf-8")
    io.WriteString(w, `<!doctype html><html><head><meta charset="utf-8"><title>Jobs</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <script>
    async function loadJobs(){
      const list = await fetch('/admin/api/jobs').then(r=>r.json()).catch(()=>[]);
      const tbody = document.getElementById('jobs'); tbody.innerHTML='';
      list.forEach(j=>{
        const tr=document.createElement('tr');
        tr.innerHTML = `<td class='p-2 border'>${j.id}</td><td class='p-2 border'>${j.status}</td><td class='p-2 border'>${j.url||''}</td><td class='p-2 border'>${j.error||''}</td><td class='p-2 border'><button class='px-2 py-1 bg-blue-600 text-white rounded' onclick=retryJob('${j.id}')>Retry</button> <button class='px-2 py-1 bg-orange-600 text-white rounded' onclick=cancelJob('${j.id}')>Cancel</button> <button class='px-2 py-1 bg-red-600 text-white rounded' onclick=deleteJob('${j.id}')>Delete</button></td>`;
        tbody.appendChild(tr);
      });
    }
    async function retryJob(id){ await fetch('/admin/api/retry/'+id,{method:'POST'}).then(()=>loadJobs()); }
    async function cancelJob(id){ await fetch('/admin/api/cancel/'+id,{method:'POST'}).then(()=>loadJobs()); }
    async function deleteJob(id){ await fetch('/delete/'+id,{method:'DELETE'}).then(()=>loadJobs()); }
    window.onload=loadJobs;
    </script></head>
    <body class="bg-gray-50">
    <div class="max-w-6xl mx-auto p-6">
      <div class="flex items-center justify-between mb-6">
        <h1 class="text-2xl font-semibold">Jobs</h1>
        <nav class="space-x-4 text-blue-600">
          <a class="hover:underline" href="/admin">Home</a>
          <a class="hover:underline" href="/admin/playground">API Playground</a>
          <a class="hover:underline" href="/admin/liveops">Live Ops</a>
          <a class="hover:underline" href="/admin/jobs">Jobs</a>
          <a class="hover:underline" href="/admin/docs">Docs</a>
        </nav>
      </div>
      <div class="bg-white rounded shadow p-4">
        <table class="w-full text-sm">
          <thead><tr class="bg-gray-100"><th class="p-2 border">Job ID</th><th class="p-2 border">Status</th><th class="p-2 border">URL</th><th class="p-2 border">Error</th><th class="p-2 border">Actions</th></tr></thead>
          <tbody id="jobs"></tbody>
        </table>
      </div>
    </div>
    </body></html>`)
}

// Admin API: list jobs (basic)
func handleAdminAPIJobsList(w http.ResponseWriter, r *http.Request) {
    jobStore.RLock()
    out := make([]map[string]interface{}, 0, len(jobStore.jobs))
    for _, j := range jobStore.jobs {
        out = append(out, map[string]interface{}{"id": j.ID, "status": j.Status, "url": j.URL, "error": j.Error})
    }
    jobStore.RUnlock()
    // Augment with Redis jobs if not in memory (best-effort)
    // Skipped for brevity to avoid large scans in production
    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(out)
}

// Admin API: retry job
func handleAdminAPIRetry(w http.ResponseWriter, r *http.Request) {
    if r.Method != http.MethodPost { http.Error(w, "Method not allowed", http.StatusMethodNotAllowed); return }
    id := filepath.Base(r.URL.Path)
    jobStore.RLock()
    j, ok := jobStore.jobs[id]
    jobStore.RUnlock()
    if !ok { http.Error(w, "Job not found", http.StatusNotFound); return }
    j.Status = StatusPending; j.Error = ""; j.Retries = 0
    select { case jobQueue <- j: default: }
    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(map[string]string{"enqueued": id})
}

// Admin API: cancel job (best effort)
func handleAdminAPICancel(w http.ResponseWriter, r *http.Request) {
    if r.Method != http.MethodPost { http.Error(w, "Method not allowed", http.StatusMethodNotAllowed); return }
    id := filepath.Base(r.URL.Path)
    canceledJobs.Lock(); canceledJobs.m[id] = struct{}{}; canceledJobs.Unlock()
    // If job exists and is pending, mark as canceled immediately
    jobStore.Lock()
    if j, ok := jobStore.jobs[id]; ok {
        if j.Status == StatusPending {
            j.Status = StatusCanceled
            j.Error = "canceled by admin"
            saveJobToRedis(j)
        }
    }
    jobStore.Unlock()
    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(map[string]string{"canceled": id})
}

// Admin API: get state
func handleAdminAPIState(w http.ResponseWriter, r *http.Request) {
    state := map[string]interface{}{
        "intake_paused": atomic.LoadInt32(&intakePaused) == 1,
        "active_jobs": atomic.LoadInt64(&activeJobs),
        "queued_jobs": atomic.LoadInt64(&queuedJobs),
        "workers": WorkerPoolSize,
        "rate_limit": RequestsPerSecond,
        "burst": BurstSize,
    }
    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(state)
}

// Admin API: pause/resume intake
func handleAdminAPIPause(w http.ResponseWriter, r *http.Request) {
    if r.Method != http.MethodPost { http.Error(w, "Method not allowed", http.StatusMethodNotAllowed); return }
    atomic.StoreInt32(&intakePaused, 1)
    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(map[string]bool{"intake_paused": true})
}

func handleAdminAPIResume(w http.ResponseWriter, r *http.Request) {
    if r.Method != http.MethodPost { http.Error(w, "Method not allowed", http.StatusMethodNotAllowed); return }
    atomic.StoreInt32(&intakePaused, 0)
    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(map[string]bool{"intake_paused": false})
}

// Admin API: clear queue (best effort: drain pending jobs from channel and mark canceled)
func handleAdminAPIClearQueue(w http.ResponseWriter, r *http.Request) {
    if r.Method != http.MethodPost { http.Error(w, "Method not allowed", http.StatusMethodNotAllowed); return }
    drained := 0
    for {
        select {
        case j := <-jobQueue:
            if j != nil {
                j.Status = StatusCanceled
                j.Error = "cleared by admin"
                saveJobToRedis(j)
                drained++
            }
        default:
            goto DONE
        }
    }
DONE:
    atomic.StoreInt64(&queuedJobs, 0)
    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(map[string]int{"cleared": drained})
}

// Admin API: adjust worker pool size (delta +/-)
func handleAdminAPIWorkers(w http.ResponseWriter, r *http.Request) {
    if r.Method != http.MethodPost { http.Error(w, "Method not allowed", http.StatusMethodNotAllowed); return }
    deltaStr := r.URL.Query().Get("delta")
    d, _ := strconv.Atoi(deltaStr)
    if d == 0 { json.NewEncoder(w).Encode(map[string]int{"workers": WorkerPoolSize}); return }
    // We can only grow pool by starting more goroutines; shrinking only affects new jobs
    if d > 0 {
        for i := 0; i < d; i++ { go startWorker(WorkerPoolSize + i) }
        WorkerPoolSize += d
    } else {
        // reduce target size; workers check range on next start (best-effort)
        if WorkerPoolSize + d > 0 { WorkerPoolSize += d }
        if WorkerPoolSize < 1 { WorkerPoolSize = 1 }
    }
    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(map[string]int{"workers": WorkerPoolSize})
}

// Admin API: adjust admission rate limiter
func handleAdminAPIRate(w http.ResponseWriter, r *http.Request) {
    if r.Method != http.MethodPost { http.Error(w, "Method not allowed", http.StatusMethodNotAllowed); return }
    var req struct{ RPS int `json:"rps"`; Burst int `json:"burst"` }
    if err := json.NewDecoder(r.Body).Decode(&req); err != nil { http.Error(w, "Bad JSON", http.StatusBadRequest); return }
    if req.RPS <= 0 || req.Burst <= 0 { http.Error(w, "Invalid values", http.StatusBadRequest); return }
    RequestsPerSecond = req.RPS
    BurstSize = req.Burst
    rateLimiter = rate.NewLimiter(rate.Limit(RequestsPerSecond), BurstSize)
    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(map[string]int{"rps": RequestsPerSecond, "burst": BurstSize})
}
