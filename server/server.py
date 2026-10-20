"""
VoltRoute – gRPC Charging Grid Server (Tier 3)
================================================
Implements:
  • Unit 1 – Acts as Tier-3 back-end (data + logic)
  • Unit 2 – gRPC RPC service methods
  • Unit 3 – Lamport Logical Clock for queue ordering
  • Unit 3 – Mutual Exclusion (mutex) for transformer capacity
"""

import grpc
import time
import threading
import json
import logging
from concurrent import futures

import charging_pb2
import charging_pb2_grpc

logging.basicConfig(level=logging.INFO, format="%(asctime)s [SERVER] %(message)s")

# ══════════════════════════════════════════════════════════════════════
#  UNIT 3 – Lamport Logical Clock
# ══════════════════════════════════════════════════════════════════════
class LamportClock:
    """
    Lamport Logical Clock implementation.
    Rules:
      1. Before every local event → clock += 1
      2. On send(msg) → clock += 1, attach clock value
      3. On receive(msg, ts) → clock = max(clock, ts) + 1
    """
    def __init__(self):
        self._value = 0
        self._lock = threading.Lock()

    def tick(self):
        """Local event – increment clock."""
        with self._lock:
            self._value += 1
            return self._value

    def update(self, received_ts):
        """Receive event – merge with received timestamp."""
        with self._lock:
            self._value = max(self._value, received_ts) + 1
            return self._value

    @property
    def value(self):
        with self._lock:
            return self._value


# ══════════════════════════════════════════════════════════════════════
#  UNIT 3 – Mutual Exclusion (Transformer Capacity Lock)
# ══════════════════════════════════════════════════════════════════════
class TransformerMutex:
    """
    Mutual Exclusion for grid transformer capacity.
    Each station's transformer can supply power to a limited number of
    vehicles simultaneously. The mutex ensures that the capacity is
    never exceeded (critical section protection).
    
    Algorithm: Centralized Mutex (Coordinator-based)
    - The server acts as the central coordinator
    - Vehicles REQUEST access to charge
    - Coordinator GRANTs if capacity available, else QUEUEs
    - On RELEASE, coordinator grants to next in wait-queue
    """
    def __init__(self, max_capacity):
        self.max_capacity = max_capacity
        self.current_count = 0
        self._lock = threading.Lock()
        self._wait_queue = []  # vehicles waiting for access
        self.is_locked = False  # True when at full capacity

    def acquire(self, vehicle_id):
        """Try to acquire transformer access (enter critical section)."""
        with self._lock:
            if self.current_count < self.max_capacity:
                self.current_count += 1
                self.is_locked = (self.current_count >= self.max_capacity)
                logging.info(f"MUTEX GRANT: {vehicle_id} → capacity {self.current_count}/{self.max_capacity}")
                return True  # GRANTED
            else:
                self._wait_queue.append(vehicle_id)
                self.is_locked = True
                logging.info(f"MUTEX QUEUE: {vehicle_id} → wait queue pos {len(self._wait_queue)}")
                return False  # QUEUED

    def release(self, vehicle_id):
        """Release transformer access (exit critical section)."""
        with self._lock:
            if self.current_count > 0:
                self.current_count -= 1
            self.is_locked = (self.current_count >= self.max_capacity)
            # Grant to next waiting vehicle if any
            next_vehicle = None
            if self._wait_queue and self.current_count < self.max_capacity:
                next_vehicle = self._wait_queue.pop(0)
                self.current_count += 1
                self.is_locked = (self.current_count >= self.max_capacity)
            logging.info(f"MUTEX RELEASE: {vehicle_id} → capacity {self.current_count}/{self.max_capacity}")
            return next_vehicle


# ══════════════════════════════════════════════════════════════════════
#  Station & Queue Data
# ══════════════════════════════════════════════════════════════════════
STATIONS = {
    "ST-001": {
        "name": "MG Road Supercharger",
        "location": "MG Road, Pune",
        "total_slots": 4,
        "used_slots": 0,
        "power_kw": 150.0,
        "mutex": TransformerMutex(max_capacity=4),
    },
    "ST-002": {
        "name": "Hinjewadi Tech Hub",
        "location": "Hinjewadi Phase 1, Pune",
        "total_slots": 3,
        "used_slots": 0,
        "power_kw": 120.0,
        "mutex": TransformerMutex(max_capacity=3),
    },
    "ST-003": {
        "name": "Kothrud Green Station",
        "location": "Kothrud, Pune",
        "total_slots": 2,
        "used_slots": 0,
        "power_kw": 80.0,
        "mutex": TransformerMutex(max_capacity=2),
    },
}

# Charging queue – ordered by Lamport timestamps
charging_queue = []  # list of dicts
queue_lock = threading.Lock()
lamport_clock = LamportClock()

# Pub/Sub subscribers (Unit 2) – list of callback info
pubsub_events = []  # shared event log for SSE
pubsub_lock = threading.Lock()


def publish_event(event_type, data):
    """Unit 2 – Publish event to Pub/Sub channel."""
    event = {
        "type": event_type,
        "data": data,
        "timestamp": time.time(),
        "lamport_ts": lamport_clock.value,
    }
    with pubsub_lock:
        pubsub_events.append(event)
        # Keep only last 100 events
        if len(pubsub_events) > 100:
            pubsub_events.pop(0)
    logging.info(f"PUB/SUB → {event_type}: {data.get('message', '')}")


# ══════════════════════════════════════════════════════════════════════
#  gRPC Service Implementation (Unit 2 – RPC)
# ══════════════════════════════════════════════════════════════════════
class ChargingGridServicer(charging_pb2_grpc.ChargingGridServicer):

    def BookSlot(self, request, context):
        """
        RPC: Book a charging slot.
        Uses Lamport Clock for ordering and Mutex for capacity control.
        """
        vehicle_id = request.vehicle_id
        station_id = request.station_id
        battery_pct = request.battery_pct

        logging.info(f"BookSlot request: vehicle={vehicle_id}, station={station_id}, battery={battery_pct}%")

        # Validate station
        if station_id not in STATIONS:
            return charging_pb2.BookResponse(
                success=False,
                message=f"Station {station_id} not found",
                lamport_ts=0,
                queue_pos=-1,
            )

        station = STATIONS[station_id]

        # Unit 3 – Lamport Clock: tick for this event
        ts = lamport_clock.tick()

        # Unit 3 – Mutual Exclusion: try to acquire transformer
        granted = station["mutex"].acquire(vehicle_id)

        if granted:
            station["used_slots"] += 1
            status = "CHARGING"
            message = f"Charging started at {station['name']} (Lamport TS: {ts})"
        else:
            status = "WAITING"
            message = f"Queued at {station['name']} – transformer at capacity (Lamport TS: {ts})"

        # Add to Lamport-ordered queue
        entry = {
            "vehicle_id": vehicle_id,
            "station_id": station_id,
            "battery_pct": battery_pct,
            "lamport_ts": ts,
            "status": status,
        }
        with queue_lock:
            charging_queue.append(entry)
            # Sort by Lamport timestamp (total ordering)
            charging_queue.sort(key=lambda x: x["lamport_ts"])
            queue_pos = next(i for i, e in enumerate(charging_queue) if e["vehicle_id"] == vehicle_id) + 1

        # Unit 2 – Pub/Sub: publish booking event
        publish_event("BOOKING", {
            "vehicle_id": vehicle_id,
            "station": station["name"],
            "status": status,
            "lamport_ts": ts,
            "message": message,
        })

        return charging_pb2.BookResponse(
            success=True,
            message=message,
            lamport_ts=ts,
            queue_pos=queue_pos,
        )

    def ReleaseSlot(self, request, context):
        """RPC: Release a charging slot. Frees the mutex."""
        vehicle_id = request.vehicle_id
        station_id = request.station_id

        logging.info(f"ReleaseSlot request: vehicle={vehicle_id}, station={station_id}")

        if station_id not in STATIONS:
            return charging_pb2.ReleaseResponse(
                success=False,
                message=f"Station {station_id} not found",
            )

        station = STATIONS[station_id]

        # Unit 3 – Mutual Exclusion: release transformer
        next_vehicle = station["mutex"].release(vehicle_id)
        if station["used_slots"] > 0:
            station["used_slots"] -= 1

        # Remove from queue
        with queue_lock:
            for i, entry in enumerate(charging_queue):
                if entry["vehicle_id"] == vehicle_id and entry["station_id"] == station_id:
                    charging_queue.pop(i)
                    break

        # If a waiting vehicle got promoted, update its status
        if next_vehicle:
            station["used_slots"] += 1
            with queue_lock:
                for entry in charging_queue:
                    if entry["vehicle_id"] == next_vehicle and entry["station_id"] == station_id:
                        entry["status"] = "CHARGING"
                        break

            publish_event("MUTEX_GRANT", {
                "vehicle_id": next_vehicle,
                "station": station["name"],
                "message": f"Mutex GRANTED to {next_vehicle} after {vehicle_id} released",
            })

        lamport_clock.tick()

        publish_event("RELEASE", {
            "vehicle_id": vehicle_id,
            "station": station["name"],
            "message": f"{vehicle_id} finished charging at {station['name']}",
        })

        return charging_pb2.ReleaseResponse(
            success=True,
            message=f"Slot released at {station['name']}",
        )

    def GetStations(self, request, context):
        """RPC: Get all station statuses."""
        lamport_clock.tick()
        stations = []
        for sid, s in STATIONS.items():
            stations.append(charging_pb2.Station(
                id=sid,
                name=s["name"],
                location=s["location"],
                total_slots=s["total_slots"],
                used_slots=s["used_slots"],
                power_kw=s["power_kw"],
                transformer_locked=s["mutex"].is_locked,
            ))
        return charging_pb2.StationList(stations=stations)

    def GetQueue(self, request, context):
        """RPC: Get Lamport-ordered charging queue."""
        lamport_clock.tick()
        entries = []
        with queue_lock:
            for e in charging_queue:
                entries.append(charging_pb2.QueueEntry(
                    vehicle_id=e["vehicle_id"],
                    station_id=e["station_id"],
                    battery_pct=e["battery_pct"],
                    lamport_ts=e["lamport_ts"],
                    status=e["status"],
                ))
        return charging_pb2.QueueList(entries=entries)


# ══════════════════════════════════════════════════════════════════════
#  Power Telemetry Simulator (for SSE stream – Unit 2)
# ══════════════════════════════════════════════════════════════════════
def simulate_power():
    """Simulate fluctuating power readings for stations."""
    import random
    while True:
        for sid, station in STATIONS.items():
            base = station["power_kw"]
            # Fluctuate ±15%
            fluctuation = base * random.uniform(-0.15, 0.15)
            station["power_kw"] = round(max(10, base + fluctuation), 1)
        time.sleep(2)


# ══════════════════════════════════════════════════════════════════════
#  Server Entry Point
# ══════════════════════════════════════════════════════════════════════
def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    charging_pb2_grpc.add_ChargingGridServicer_to_server(
        ChargingGridServicer(), server
    )
    server.add_insecure_port("[::]:50051")
    logging.info("⚡ VoltRoute gRPC Server started on port 50051")

    # Start power telemetry simulator in background
    power_thread = threading.Thread(target=simulate_power, daemon=True)
    power_thread.start()
    logging.info("📊 Power telemetry simulator started")

    server.start()
    server.wait_for_termination()


if __name__ == "__main__":
    serve()
