"""
VoltRoute – Flask Web Gateway (Tier 2)
========================================
Implements:
  • Unit 1 – Acts as Tier-2 middleware (between Client UI and gRPC Server)
  • Unit 2 – gRPC client stubs to call Tier-3
  • Unit 2 – SSE (Server-Sent Events) for real-time power telemetry stream
  • Unit 2 – Pub/Sub event broadcasting to connected clients
"""

import grpc
import json
import time
import threading
import logging
from flask import Flask, render_template, request, jsonify, Response

import charging_pb2
import charging_pb2_grpc

logging.basicConfig(level=logging.INFO, format="%(asctime)s [GATEWAY] %(message)s")

app = Flask(__name__, static_folder="static", template_folder="templates")

# ── gRPC Channel to Tier-3 Server ───────────────────────────────────
GRPC_HOST = "volt-server"  # Docker/K8s service name
GRPC_PORT = "50051"

def get_grpc_stub():
    """Create a gRPC stub to communicate with Tier-3 server."""
    import os
    host = os.environ.get("GRPC_HOST", GRPC_HOST)
    port = os.environ.get("GRPC_PORT", GRPC_PORT)
    channel = grpc.insecure_channel(f"{host}:{port}")
    return charging_pb2_grpc.ChargingGridStub(channel)


# ══════════════════════════════════════════════════════════════════════
#  REST API Endpoints (Tier-2 → calls Tier-3 via gRPC)
# ══════════════════════════════════════════════════════════════════════

@app.route("/")
def index():
    """Serve the main UI."""
    return render_template("index.html")


@app.route("/api/stations", methods=["GET"])
def get_stations():
    """Get all charging station statuses via gRPC."""
    try:
        stub = get_grpc_stub()
        response = stub.GetStations(charging_pb2.Empty())
        stations = []
        for s in response.stations:
            stations.append({
                "id": s.id,
                "name": s.name,
                "location": s.location,
                "total_slots": s.total_slots,
                "used_slots": s.used_slots,
                "power_kw": round(s.power_kw, 1),
                "transformer_locked": s.transformer_locked,
            })
        return jsonify({"stations": stations})
    except grpc.RpcError as e:
        logging.error(f"gRPC error: {e}")
        return jsonify({"error": "Server unavailable"}), 503


@app.route("/api/book", methods=["POST"])
def book_slot():
    """Book a charging slot via gRPC RPC call."""
    data = request.json
    vehicle_id = data.get("vehicle_id", "")
    station_id = data.get("station_id", "")
    battery_pct = data.get("battery_pct", 50)

    if not vehicle_id or not station_id:
        return jsonify({"error": "vehicle_id and station_id required"}), 400

    try:
        stub = get_grpc_stub()
        response = stub.BookSlot(charging_pb2.BookRequest(
            vehicle_id=vehicle_id,
            station_id=station_id,
            battery_pct=battery_pct,
        ))
        return jsonify({
            "success": response.success,
            "message": response.message,
            "lamport_ts": response.lamport_ts,
            "queue_pos": response.queue_pos,
        })
    except grpc.RpcError as e:
        logging.error(f"gRPC error: {e}")
        return jsonify({"error": "Booking failed – server unavailable"}), 503


@app.route("/api/release", methods=["POST"])
def release_slot():
    """Release a charging slot via gRPC RPC call."""
    data = request.json
    vehicle_id = data.get("vehicle_id", "")
    station_id = data.get("station_id", "")

    if not vehicle_id or not station_id:
        return jsonify({"error": "vehicle_id and station_id required"}), 400

    try:
        stub = get_grpc_stub()
        response = stub.ReleaseSlot(charging_pb2.ReleaseRequest(
            vehicle_id=vehicle_id,
            station_id=station_id,
        ))
        return jsonify({
            "success": response.success,
            "message": response.message,
        })
    except grpc.RpcError as e:
        logging.error(f"gRPC error: {e}")
        return jsonify({"error": "Release failed – server unavailable"}), 503


@app.route("/api/queue", methods=["GET"])
def get_queue():
    """Get the Lamport-ordered charging queue via gRPC."""
    try:
        stub = get_grpc_stub()
        response = stub.GetQueue(charging_pb2.Empty())
        entries = []
        for e in response.entries:
            entries.append({
                "vehicle_id": e.vehicle_id,
                "station_id": e.station_id,
                "battery_pct": e.battery_pct,
                "lamport_ts": e.lamport_ts,
                "status": e.status,
            })
        return jsonify({"queue": entries})
    except grpc.RpcError as e:
        logging.error(f"gRPC error: {e}")
        return jsonify({"error": "Queue unavailable"}), 503


# ══════════════════════════════════════════════════════════════════════
#  Unit 2 – SSE (Server-Sent Events) for Real-Time Power Telemetry
# ══════════════════════════════════════════════════════════════════════

@app.route("/stream/power")
def stream_power():
    """
    SSE endpoint: streams real-time power telemetry from all stations.
    The client opens an EventSource connection and receives updates
    every 2 seconds with station power readings.
    """
    def generate():
        while True:
            try:
                stub = get_grpc_stub()
                response = stub.GetStations(charging_pb2.Empty())
                data = []
                for s in response.stations:
                    data.append({
                        "id": s.id,
                        "name": s.name,
                        "power_kw": round(s.power_kw, 1),
                        "used_slots": s.used_slots,
                        "total_slots": s.total_slots,
                        "transformer_locked": s.transformer_locked,
                    })
                yield f"data: {json.dumps(data)}\n\n"
            except Exception as e:
                yield f"data: {json.dumps({'error': str(e)})}\n\n"
            time.sleep(2)

    return Response(generate(), mimetype="text/event-stream")


# ══════════════════════════════════════════════════════════════════════
#  Unit 2 – Pub/Sub Event Stream (alerts, mutex grants, etc.)
# ══════════════════════════════════════════════════════════════════════

@app.route("/stream/events")
def stream_events():
    """
    SSE endpoint: Pub/Sub event stream.
    Clients subscribe to receive booking alerts, mutex grants,
    and release notifications in real-time.
    """
    def generate():
        last_idx = 0
        while True:
            try:
                stub = get_grpc_stub()
                # Poll queue for changes (acts as pub/sub proxy)
                queue_resp = stub.GetQueue(charging_pb2.Empty())
                station_resp = stub.GetStations(charging_pb2.Empty())

                event_data = {
                    "queue": [{
                        "vehicle_id": e.vehicle_id,
                        "station_id": e.station_id,
                        "battery_pct": e.battery_pct,
                        "lamport_ts": e.lamport_ts,
                        "status": e.status,
                    } for e in queue_resp.entries],
                    "stations": [{
                        "id": s.id,
                        "name": s.name,
                        "used_slots": s.used_slots,
                        "total_slots": s.total_slots,
                        "transformer_locked": s.transformer_locked,
                    } for s in station_resp.stations],
                    "timestamp": time.time(),
                }
                yield f"data: {json.dumps(event_data)}\n\n"
            except Exception as e:
                yield f"data: {json.dumps({'error': str(e)})}\n\n"
            time.sleep(3)

    return Response(generate(), mimetype="text/event-stream")


# ══════════════════════════════════════════════════════════════════════
#  Entry Point
# ══════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    logging.info("🌐 VoltRoute Gateway starting on port 5000")
    app.run(host="0.0.0.0", port=5000, debug=False, threaded=True)
