/* ═══════════════════════════════════════════════════════════════════
   VoltRoute – Client-Side JavaScript (Tier 1 Logic)
   ═══════════════════════════════════════════════════════════════════
   Implements:
     • REST API calls to Tier-2 Gateway
     • SSE (EventSource) for real-time power telemetry (Unit 2)
     • SSE (EventSource) for Pub/Sub event stream (Unit 2)
     • Dynamic UI updates for Lamport Queue & Mutex visualization
   ═══════════════════════════════════════════════════════════════════ */

// ── State ──────────────────────────────────────────────────────────
let eventLog = [];

// ══════════════════════════════════════════════════════════════════════
//  Init – Load stations, start SSE streams
// ══════════════════════════════════════════════════════════════════════
document.addEventListener("DOMContentLoaded", () => {
    loadStations();
    startPowerStream();
    startEventStream();

    // Battery slider live update
    const slider = document.getElementById("battery-pct");
    const display = document.getElementById("battery-value");
    slider.addEventListener("input", () => {
        display.textContent = slider.value + "%";
        // Color shift based on battery level
        const pct = parseInt(slider.value);
        if (pct < 20) display.style.color = "#ef4444";
        else if (pct < 50) display.style.color = "#f59e0b";
        else display.style.color = "#00e68a";
    });
});


// ══════════════════════════════════════════════════════════════════════
//  REST API: Load Stations
// ══════════════════════════════════════════════════════════════════════
async function loadStations() {
    try {
        const res = await fetch("/api/stations");
        const data = await res.json();
        if (data.stations) {
            renderStations(data.stations);
            populateStationSelect(data.stations);
            renderMutexVisual(data.stations);
            document.getElementById("connection-status").textContent = "Connected";
        }
    } catch (err) {
        document.getElementById("connection-status").textContent = "Disconnected";
        console.error("Failed to load stations:", err);
    }
}


function populateStationSelect(stations) {
    const select = document.getElementById("station-select");
    select.innerHTML = "";
    stations.forEach(s => {
        const opt = document.createElement("option");
        opt.value = s.id;
        opt.textContent = `${s.name} (${s.used_slots}/${s.total_slots} used)`;
        select.appendChild(opt);
    });
}


function renderStations(stations) {
    const grid = document.getElementById("stations-grid");
    grid.innerHTML = "";
    stations.forEach(s => {
        const pct = (s.used_slots / s.total_slots) * 100;
        let barColor;
        if (pct >= 100) barColor = "#ef4444";
        else if (pct >= 60) barColor = "#f59e0b";
        else barColor = "#00e68a";

        const card = document.createElement("div");
        card.className = `station-card ${s.transformer_locked ? "locked" : ""}`;
        card.innerHTML = `
            <div class="station-name">${s.name}</div>
            <div class="station-location">📍 ${s.location}</div>
            <div class="station-stats">
                <div class="stat">
                    <div class="stat-value">${s.used_slots}/${s.total_slots}</div>
                    <div class="stat-label">Slots</div>
                </div>
                <div class="stat">
                    <div class="stat-value" style="color: var(--accent-cyan)">${s.power_kw}</div>
                    <div class="stat-label">kW</div>
                </div>
            </div>
            <div class="slot-bar">
                <div class="slot-bar-fill" style="width: ${pct}%; background: ${barColor}"></div>
            </div>
            <span class="mutex-badge ${s.transformer_locked ? 'mutex-locked' : 'mutex-open'}">
                ${s.transformer_locked ? '🔒 MUTEX LOCKED' : '🔓 MUTEX OPEN'}
            </span>
        `;
        grid.appendChild(card);
    });
}


// ══════════════════════════════════════════════════════════════════════
//  REST API: Book Slot (calls gRPC via Gateway)
// ══════════════════════════════════════════════════════════════════════
async function bookSlot() {
    const vehicleId = document.getElementById("vehicle-id").value.trim();
    const stationId = document.getElementById("station-select").value;
    const batteryPct = parseInt(document.getElementById("battery-pct").value);
    const resultBox = document.getElementById("booking-result");

    if (!vehicleId) {
        showResult(resultBox, "error", "⚠️ Please enter a Vehicle ID");
        return;
    }

    try {
        const res = await fetch("/api/book", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
                vehicle_id: vehicleId,
                station_id: stationId,
                battery_pct: batteryPct,
            }),
        });
        const data = await res.json();
        if (data.success) {
            showResult(resultBox, "success",
                `✅ ${data.message}<br>` +
                `<small>Lamport TS: <strong>${data.lamport_ts}</strong> | Queue Position: <strong>#${data.queue_pos}</strong></small>`
            );
        } else {
            showResult(resultBox, "error", `❌ ${data.message || data.error}`);
        }
        loadStations();
        loadQueue();
    } catch (err) {
        showResult(resultBox, "error", `❌ Server unreachable: ${err.message}`);
    }
}


// ══════════════════════════════════════════════════════════════════════
//  REST API: Release Slot
// ══════════════════════════════════════════════════════════════════════
async function releaseSlot() {
    const vehicleId = document.getElementById("vehicle-id").value.trim();
    const stationId = document.getElementById("station-select").value;
    const resultBox = document.getElementById("booking-result");

    if (!vehicleId) {
        showResult(resultBox, "error", "⚠️ Please enter a Vehicle ID to release");
        return;
    }

    try {
        const res = await fetch("/api/release", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
                vehicle_id: vehicleId,
                station_id: stationId,
            }),
        });
        const data = await res.json();
        if (data.success) {
            showResult(resultBox, "success", `🔓 ${data.message}`);
        } else {
            showResult(resultBox, "error", `❌ ${data.message || data.error}`);
        }
        loadStations();
        loadQueue();
    } catch (err) {
        showResult(resultBox, "error", `❌ Server unreachable: ${err.message}`);
    }
}


function showResult(box, type, html) {
    box.className = `result-box ${type}`;
    box.innerHTML = html;
}


// ══════════════════════════════════════════════════════════════════════
//  REST API: Load Lamport Queue
// ══════════════════════════════════════════════════════════════════════
async function loadQueue() {
    try {
        const res = await fetch("/api/queue");
        const data = await res.json();
        renderQueue(data.queue || []);
    } catch (err) {
        console.error("Failed to load queue:", err);
    }
}


function renderQueue(queue) {
    const list = document.getElementById("queue-list");
    if (queue.length === 0) {
        list.innerHTML = '<div class="empty-state">No vehicles in queue</div>';
        return;
    }
    list.innerHTML = "";
    queue.forEach(e => {
        const entry = document.createElement("div");
        entry.className = "queue-entry";
        entry.innerHTML = `
            <span>${e.vehicle_id}</span>
            <span>${e.station_id}</span>
            <span>${e.battery_pct}%</span>
            <span class="lamport-ts">${e.lamport_ts}</span>
            <span class="queue-status ${e.status === 'CHARGING' ? 'status-charging' : 'status-waiting'}">
                ${e.status}
            </span>
        `;
        list.appendChild(entry);
    });
}


// ══════════════════════════════════════════════════════════════════════
//  Unit 2 – SSE: Power Telemetry Stream
// ══════════════════════════════════════════════════════════════════════
function startPowerStream() {
    const evtSource = new EventSource("/stream/power");
    const container = document.getElementById("power-bars");

    evtSource.onmessage = (event) => {
        try {
            const stations = JSON.parse(event.data);
            if (stations.error) return;

            container.innerHTML = "";
            stations.forEach(s => {
                const maxPower = 200; // max scale
                const pct = Math.min((s.power_kw / maxPower) * 100, 100);

                const item = document.createElement("div");
                item.className = "power-item";
                item.innerHTML = `
                    <div class="power-label">${s.name}</div>
                    <div class="power-bar-track">
                        <div class="power-bar-fill" style="width: ${pct}%"></div>
                    </div>
                    <div class="power-value">${s.power_kw} kW</div>
                `;
                container.appendChild(item);
            });
        } catch (e) {
            console.error("SSE parse error:", e);
        }
    };

    evtSource.onerror = () => {
        console.warn("SSE power stream error – will auto-reconnect");
    };
}


// ══════════════════════════════════════════════════════════════════════
//  Unit 2 – SSE: Pub/Sub Event Stream
// ══════════════════════════════════════════════════════════════════════
function startEventStream() {
    const evtSource = new EventSource("/stream/events");
    const log = document.getElementById("event-log");

    evtSource.onmessage = (event) => {
        try {
            const data = JSON.parse(event.data);
            if (data.error) return;

            // Update queue and stations from event stream
            if (data.queue) renderQueue(data.queue);
            if (data.stations) {
                renderStations(data.stations.map(s => ({
                    ...s,
                    power_kw: 0,
                    location: "",
                })));
                renderMutexVisual(data.stations);
            }

            // Add event to log
            const time = new Date(data.timestamp * 1000).toLocaleTimeString();
            const queueCount = data.queue ? data.queue.length : 0;
            const chargingCount = data.queue ? data.queue.filter(e => e.status === "CHARGING").length : 0;

            const item = document.createElement("div");
            item.className = "event-item booking";
            item.innerHTML = `
                <span class="event-time">${time}</span>
                <span class="event-type" style="background: rgba(59,130,246,0.2); color: #3b82f6">SYNC</span>
                Queue: ${queueCount} vehicles | Charging: ${chargingCount} active
            `;
            log.prepend(item);

            // Keep only last 20 events
            while (log.children.length > 20) {
                log.removeChild(log.lastChild);
            }
        } catch (e) {
            console.error("SSE event parse error:", e);
        }
    };
}


// ══════════════════════════════════════════════════════════════════════
//  Mutex Visualizer
// ══════════════════════════════════════════════════════════════════════
function renderMutexVisual(stations) {
    const container = document.getElementById("mutex-visual");
    container.innerHTML = "";

    stations.forEach(s => {
        const total = s.total_slots;
        const used = s.used_slots;
        const slotsHtml = Array.from({ length: total }, (_, i) => {
            const occupied = i < used;
            return `<div class="mutex-slot ${occupied ? 'occupied' : 'free'}">${occupied ? '🔴' : '🟢'}</div>`;
        }).join("");

        const stationEl = document.createElement("div");
        stationEl.className = "mutex-station";
        stationEl.innerHTML = `
            <div class="mutex-station-name">${s.name}</div>
            <div class="mutex-slots">${slotsHtml}</div>
            <div class="mutex-info">
                Capacity: ${used}/${total} |
                Mutex: <strong style="color: ${s.transformer_locked ? '#ef4444' : '#00e68a'}">
                    ${s.transformer_locked ? 'LOCKED (Critical Section Full)' : 'OPEN (Accepting Vehicles)'}
                </strong>
            </div>
        `;
        container.appendChild(stationEl);
    });
}
