const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const ping = require('ping');
const fs = require('fs');
const path = require('path');

const app = express();
const server = http.createServer(app);
const io = new Server(server);

const PORT = 4000;
const TARGET_INTERNET = '8.8.8.8'; // DNS Google untuk cek internet
const ARCHIVE_FILE = path.join(__dirname, 'all_incidents.json');
const SLOWTEST_FILE = path.join(__dirname, 'slow_history.json');
const SPEEDTEST_FILE = path.join(__dirname, 'speedtest_history.json');
const HISTORY_FILE = path.join(__dirname, 'latency_history.json');
const STATE_SNAPSHOT_FILE = path.join(__dirname, 'state_snapshot.json');

// --- Konfigurasi ---
const MAX_HISTORY_POINTS = 500;
const SLOW_LATENCY_THRESHOLD = 80; // ms - jika ping > 80ms dianggap LEMOT (lebih sensitif)
const SPEEDTEST_INTERVAL_MS = 60 * 1000; // setiap 1 menit
const SPEEDTEST_TIMEOUT_MS = 30000; // timeout 30 detik per speedtest
const FETCH_TIMEOUT_MS = 15000; // timeout 15 detik per fetch

// Endpoint speedtest Cloudflare — WORK dari Node.js fetch!
const SPEEDTEST_DOWNLOAD_URL = 'https://speed.cloudflare.com/__down?bytes=10000000';
const SPEEDTEST_UPLOAD_URL = 'https://speed.cloudflare.com/__up';

function ensureJsonArrayFile(filePath) {
    if (!fs.existsSync(filePath)) {
        fs.writeFileSync(filePath, JSON.stringify([]));
    }
}

function readJsonArray(filePath) {
    try {
        const data = JSON.parse(fs.readFileSync(filePath, 'utf-8'));
        return Array.isArray(data) ? data : [];
    } catch (err) {
        return [];
    }
}

function saveJsonArray(filePath, data) {
    fs.writeFileSync(filePath, JSON.stringify(data, null, 2));
}

function formatMbps(bytes, durationMs) {
    if (!durationMs || durationMs <= 0) {
        return 0;
    }
    return Number(((bytes * 8) / (durationMs / 1000) / (1024 * 1024)).toFixed(2));
}

function fetchWithTimeout(url, options = {}, timeoutMs = FETCH_TIMEOUT_MS) {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs);
    return fetch(url, { ...options, signal: controller.signal })
        .finally(() => clearTimeout(timer));
}

// Pastikan semua file JSON ada
ensureJsonArrayFile(ARCHIVE_FILE);
ensureJsonArrayFile(SLOWTEST_FILE);
ensureJsonArrayFile(SPEEDTEST_FILE);
ensureJsonArrayFile(HISTORY_FILE);
ensureJsonArrayFile(STATE_SNAPSHOT_FILE);

// ===== RESTORE STATE DARI SNAPSHOT =====
let snapshot = {};
try {
    const raw = fs.readFileSync(STATE_SNAPSHOT_FILE, 'utf-8');
    snapshot = JSON.parse(raw);
} catch (e) {
    snapshot = {};
}

let currentStatus = snapshot.currentStatus || 'ONLINE';
let downStartTime = snapshot.downStartTime || null;
let history = readJsonArray(HISTORY_FILE);
let incidentHistory = readJsonArray(ARCHIVE_FILE);
let slowHistory = readJsonArray(SLOWTEST_FILE);
let currentSlowStatus = snapshot.currentSlowStatus || 'NORMAL';
let slowStartTime = snapshot.slowStartTime || null;
let slowPeakLatency = snapshot.slowPeakLatency || 0;
let slowLatencySum = snapshot.slowLatencySum || 0;
let slowSampleCount = snapshot.slowSampleCount || 0;
let speedtestHistory = readJsonArray(SPEEDTEST_FILE);
let isSpeedtesting = false;

if (currentStatus === 'OFFLINE' && incidentHistory.length > 0 && incidentHistory[0].status_log === 'DOWN') {
    console.log('[RESTORE] Memulihkan state DOWN dari snapshot');
}
if (currentSlowStatus === 'SLOW' && slowHistory.length > 0 && slowHistory[0].status_log === 'SLOW') {
    console.log('[RESTORE] Memulihkan state SLOW dari snapshot');
}

app.use(express.static('public'));

// ============ SPEEDTEST FUNCTIONS ============

async function measureDownloadSpeed() {
    const startedAt = Date.now();
    const response = await fetchWithTimeout(SPEEDTEST_DOWNLOAD_URL, { cache: 'no-store' });

    if (!response.ok || !response.body) {
        throw new Error(`Download test gagal (${response.status})`);
    }

    let downloadedBytes = 0;
    const reader = response.body.getReader();

    while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        downloadedBytes += value.byteLength;
    }

    const durationMs = Date.now() - startedAt;

    console.log(`[SPEEDTEST] Download ${(downloadedBytes / 1024 / 1024).toFixed(1)}MB dalam ${(durationMs / 1000).toFixed(1)}s = ${formatMbps(downloadedBytes, durationMs)} Mbps`);

    return {
        bytes: downloadedBytes,
        durationMs,
        mbps: formatMbps(downloadedBytes, durationMs)
    };
}

async function measureUploadSpeed() {
    const uploadSizeBytes = 2 * 1024 * 1024;
    const uploadBody = Buffer.alloc(uploadSizeBytes, 0x61);
    const startedAt = Date.now();

    const response = await fetchWithTimeout(SPEEDTEST_UPLOAD_URL, {
        method: 'POST',
        headers: { 'Content-Type': 'application/octet-stream' },
        body: uploadBody
    });

    if (!response.ok) {
        throw new Error(`Upload test gagal (${response.status})`);
    }

    const durationMs = Date.now() - startedAt;

    console.log(`[SPEEDTEST] Upload ${(uploadSizeBytes / 1024 / 1024).toFixed(1)}MB dalam ${(durationMs / 1000).toFixed(1)}s = ${formatMbps(uploadSizeBytes, durationMs)} Mbps`);

    return {
        bytes: uploadSizeBytes,
        durationMs,
        mbps: formatMbps(uploadSizeBytes, durationMs)
    };
}

// ============ STATE MANAGEMENT ============

function saveStateSnapshot() {
    const snap = {
        currentStatus,
        downStartTime,
        currentSlowStatus,
        slowStartTime,
        slowPeakLatency,
        slowLatencySum,
        slowSampleCount,
        savedAt: new Date().toISOString()
    };
    try {
        fs.writeFileSync(STATE_SNAPSHOT_FILE, JSON.stringify(snap, null, 2));
    } catch (e) {
        console.error('Gagal menyimpan snapshot:', e.message);
    }
}

function emitState() {
    io.emit('stream-update', {
        status: currentStatus,
        latency: history.length > 0 ? history[history.length - 1].latency : 0,
        history: history,
        incidents: incidentHistory,
        slowEvents: slowHistory,
        speedtests: speedtestHistory,
        totalDown: incidentHistory.length,
        totalSlow: slowHistory.length,
        totalSpeedtests: speedtestHistory.length
    });
}

// ============ PING CHECK ============

async function checkConnection() {
    const now = new Date();
    const timestamp = now.toLocaleTimeString('id-ID');
    const dateString = now.toLocaleDateString('id-ID', { day: 'numeric', month: 'short', year: 'numeric' });

    try {
        const res = await ping.promise.probe(TARGET_INTERNET, { timeout: 1 });
        let latency = res.alive ? Math.round(res.time) : 0;
        const isSlow = res.alive && latency > SLOW_LATENCY_THRESHOLD;

        // --- DETECTION LEMOT ---
        if (isSlow) {
            if (currentSlowStatus === 'NORMAL') {
                // MULAI LEMOT — catat segera!
                currentSlowStatus = 'SLOW';
                slowStartTime = Date.now();
                slowPeakLatency = latency;
                slowLatencySum = latency;
                slowSampleCount = 1;

                slowHistory.unshift({
                    date: dateString,
                    slow_time: timestamp,
                    normal_time: '-',
                    duration: 'Sedang lemot...',
                    peak_latency_ms: latency,
                    avg_latency_ms: latency,
                    status_log: 'SLOW'
                });
                saveJsonArray(SLOWTEST_FILE, slowHistory);
                saveStateSnapshot();
                console.log(`[LEMOT] DETECTED! Latency: ${latency}ms pada ${timestamp}`);
            } else {
                // Masih lemot — update peak & average
                slowLatencySum += latency;
                slowSampleCount += 1;
                if (latency > slowPeakLatency) {
                    slowPeakLatency = latency;
                }
                if (slowHistory.length > 0 && slowHistory[0].status_log === 'SLOW') {
                    slowHistory[0].peak_latency_ms = slowPeakLatency;
                    slowHistory[0].avg_latency_ms = Math.round(slowLatencySum / slowSampleCount);
                    saveJsonArray(SLOWTEST_FILE, slowHistory);
                }
            }
        } else if (currentSlowStatus === 'SLOW') {
            // SEMBUH dari lemot
            currentSlowStatus = 'NORMAL';
            const slowDurationSeconds = Math.round((Date.now() - slowStartTime) / 1000);

            let durationText;
            if (slowDurationSeconds < 60) {
                durationText = `${slowDurationSeconds} detik`;
            } else {
                const m = Math.floor(slowDurationSeconds / 60);
                const s = slowDurationSeconds % 60;
                durationText = `${m} menit ${s} detik`;
            }

            if (slowHistory.length > 0 && slowHistory[0].normal_time === '-') {
                slowHistory[0].normal_time = timestamp;
                slowHistory[0].duration = durationText;
                slowHistory[0].status_log = 'RESOLVED';
                slowHistory[0].peak_latency_ms = slowPeakLatency;
                slowHistory[0].avg_latency_ms = Math.round(slowLatencySum / slowSampleCount);
            }
            saveJsonArray(SLOWTEST_FILE, slowHistory);
            saveStateSnapshot();
            console.log(`[LEMOT] RESOLVED — ${durationText}, peak: ${slowPeakLatency}ms`);
            slowStartTime = null;
            slowPeakLatency = 0;
            slowLatencySum = 0;
            slowSampleCount = 0;
        }

        // --- DETECTION DOWN ---
        if (!res.alive && currentStatus === 'ONLINE') {
            currentStatus = 'OFFLINE';
            downStartTime = Date.now();

            incidentHistory.unshift({
                date: dateString,
                down_time: timestamp,
                up_time: '-',
                duration: 'Mengalami Down...',
                status_log: 'DOWN'
            });
            saveJsonArray(ARCHIVE_FILE, incidentHistory);
            saveStateSnapshot();
            console.log(`[DOWN] DETECTED! Pada ${timestamp}`);
        } else if (res.alive && currentStatus === 'OFFLINE') {
            currentStatus = 'ONLINE';
            let downtimeSeconds = Math.round((Date.now() - downStartTime) / 1000);

            let durationText;
            if (downtimeSeconds < 60) {
                durationText = `${downtimeSeconds} detik`;
            } else {
                const m = Math.floor(downtimeSeconds / 60);
                const s = downtimeSeconds % 60;
                durationText = `${m} menit ${s} detik`;
            }

            if (incidentHistory.length > 0 && incidentHistory[0].up_time === '-') {
                incidentHistory[0].up_time = timestamp;
                incidentHistory[0].duration = durationText;
                incidentHistory[0].status_log = 'RESOLVED';
            }
            saveJsonArray(ARCHIVE_FILE, incidentHistory);
            saveStateSnapshot();
            console.log(`[DOWN] RESOLVED — ${durationText}`);
            downStartTime = null;
        }

        // Jika DOWN — pastikan SLOW juga selesai
        if (!res.alive && currentSlowStatus === 'SLOW') {
            currentSlowStatus = 'NORMAL';
            const slowDurationSeconds = Math.round((Date.now() - slowStartTime) / 1000);

            let durationText;
            if (slowDurationSeconds < 60) {
                durationText = `${slowDurationSeconds} detik`;
            } else {
                const m = Math.floor(slowDurationSeconds / 60);
                const s = slowDurationSeconds % 60;
                durationText = `${m} menit ${s} detik`;
            }

            if (slowHistory.length > 0 && slowHistory[0].normal_time === '-') {
                slowHistory[0].normal_time = timestamp;
                slowHistory[0].duration = durationText;
                slowHistory[0].status_log = 'TERPUTUS';
                slowHistory[0].peak_latency_ms = slowPeakLatency;
                slowHistory[0].avg_latency_ms = Math.round(slowLatencySum / slowSampleCount);
            }
            saveJsonArray(SLOWTEST_FILE, slowHistory);
            saveStateSnapshot();
            slowStartTime = null;
            slowPeakLatency = 0;
            slowLatencySum = 0;
            slowSampleCount = 0;
        }

        // --- SIMPAN HISTORY GRAFIK ---
        history.push({
            time: timestamp,
            latency: latency,
            status: res.alive ? (latency > SLOW_LATENCY_THRESHOLD ? 'LEMOT' : 'ONLINE') : 'OFFLINE'
        });
        if (history.length > MAX_HISTORY_POINTS) {
            history.shift();
        }
        saveJsonArray(HISTORY_FILE, history);

        // Kirim ke browser via WebSocket
        emitState();

    } catch (err) {
        console.error("Ping Error:", err);
    }
}

// ============ SPEEDTEST RUNNER ============

async function runSpeedtest() {
    if (isSpeedtesting) {
        return;
    }

    isSpeedtesting = true;

    const now = new Date();
    const dateString = now.toLocaleDateString('id-ID', { day: 'numeric', month: 'short', year: 'numeric' });
    const timestamp = now.toLocaleTimeString('id-ID');

    console.log(`[SPEEDTEST] Memulai speedtest pada ${timestamp}...`);

    try {
        const [download, upload] = await Promise.all([
            measureDownloadSpeed(),
            measureUploadSpeed()
        ]);

        speedtestHistory.unshift({
            date: dateString,
            time: timestamp,
            download_mbps: download.mbps,
            upload_mbps: upload.mbps,
            download_bytes: download.bytes,
            upload_bytes: upload.bytes,
            status: 'OK'
        });

        console.log(`[SPEEDTEST] SELESAI — Download: ${download.mbps} Mbps, Upload: ${upload.mbps} Mbps`);
    } catch (err) {
        speedtestHistory.unshift({
            date: dateString,
            time: timestamp,
            download_mbps: '-',
            upload_mbps: '-',
            download_bytes: 0,
            upload_bytes: 0,
            status: 'GAGAL',
            error: err.message
        });
        console.error('[SPEEDTEST] GAGAL:', err.message);
    } finally {
        saveJsonArray(SPEEDTEST_FILE, speedtestHistory);
        emitState();
        isSpeedtesting = false;
    }
}

// ============ STARTUP ============

// Ping setiap 1 detik
setInterval(checkConnection, 1000);

// Speedtest setiap 1 menit
setInterval(runSpeedtest, SPEEDTEST_INTERVAL_MS);
// Speedtest pertama setelah 3 detik
setTimeout(runSpeedtest, 3000);

server.listen(PORT, () => {
    console.log('='.repeat(50));
    console.log(`🚀 Live Monitor berjalan di http://localhost:${PORT}`);
    console.log('='.repeat(50));
    console.log(`📊 History latency: ${history.length} titik (max ${MAX_HISTORY_POINTS})`);
    console.log(`📋 Insiden DOWN: ${incidentHistory.length}`);
    console.log(`🐢 Insiden LEMOT: ${slowHistory.length} (threshold: ${SLOW_LATENCY_THRESHOLD}ms)`);
    console.log(`🚀 Riwayat speedtest: ${speedtestHistory.length}`);
    console.log(`💾 Semua data tersimpan permanen di file JSON`);
    console.log('='.repeat(50));
});
