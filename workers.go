package main

import (
    "fmt"
    "os"
    "path/filepath"
    "sync/atomic"
    "time"
)

func startWorker(workerID int) {
    logInfof("worker_startup worker_id=%d", workerID)
    for job := range jobQueue {
        processJob(job, workerID)
    }
}

func processJob(job *ConversionJob, workerID int) {
    atomic.AddInt64(&activeJobs, 1)
    atomic.AddInt64(&queuedJobs, -1)

    logInfof("worker_start job_id=%s worker_id=%d url=%s", job.ID, workerID, job.URL)

    updateJobStatus(job, StatusProcessing, "")
    job.StartedAt = time.Now()

    outputDir := "downloads"
    if err := os.MkdirAll(outputDir, os.ModePerm); err != nil {
        updateJobStatus(job, StatusFailed, fmt.Sprintf("Error creating downloads directory: %v", err))
        notifyJobCompletion(job)
        atomic.AddInt64(&activeJobs, -1)
        atomic.AddInt64(&failedJobs, 1)
        return
    }
    outputPath := filepath.Join(outputDir, job.ID+".mp3")

    logInfof("ytdlp_fetch_start job_id=%s", job.ID)
    t0 := time.Now()
    audioURL, meta, err := getAudioStreamFromYTDLP(job.URL)
    if err != nil {
        logErrorf("ytdlp_error job_id=%s err=%v", job.ID, err)
        handleJobFailure(job, err, "yt-dlp stream extraction failed")
        atomic.AddInt64(&activeJobs, -1)
        atomic.AddInt64(&failedJobs, 1)
        return
    }
    logInfof("ytdlp_fetch_done job_id=%s duration_ms=%d format_ext=%s abr=%d", job.ID, time.Since(t0).Milliseconds(), meta.Ext, meta.Abr)

    // Determine a sensible timeout for ffmpeg based on metadata duration
    ffTimeout := FFmpegMinTimeout
    if meta != nil && meta.Duration > 0 {
        // 2x duration + 3 minutes buffer, within [FFmpegMinTimeout, FFmpegMaxTimeout]
        calc := time.Duration(meta.Duration*2)*time.Second + 3*time.Minute
        if calc > ffTimeout { ffTimeout = calc }
    }
    logInfof("ffmpeg_start job_id=%s codec=libmp3lame ar=44100 bitrate=192k timeout=%s", job.ID, ffTimeout)
    t1 := time.Now()
    if err := convertStreamToMP3(audioURL, outputPath, ffTimeout); err != nil {
        logErrorf("ffmpeg_error job_id=%s err=%v", job.ID, err)
        handleJobFailure(job, err, "ffmpeg conversion failed")
        atomic.AddInt64(&activeJobs, -1)
        atomic.AddInt64(&failedJobs, 1)
        return
    }
    logInfof("ffmpeg_done job_id=%s duration_ms=%d output=%s", job.ID, time.Since(t1).Milliseconds(), outputPath)

    job.Status = StatusCompleted
    job.CompletedAt = time.Now()
    job.FilePath = outputPath
    job.DownloadURL = fmt.Sprintf("http://localhost:8080/download/%s.mp3", job.ID)
    job.Metadata = meta
    job.Error = ""

    saveJobToRedis(job)

    atomic.AddInt64(&activeJobs, -1)
    atomic.AddInt64(&completedJobs, 1)
    atomic.AddInt64(&totalProcessingTimeNs, job.CompletedAt.Sub(job.StartedAt).Nanoseconds())

    notifyJobCompletion(job)
    logInfof("job_completed job_id=%s total_ms=%d download_url=%s", job.ID, job.CompletedAt.Sub(job.CreatedAt).Milliseconds(), job.DownloadURL)
}
