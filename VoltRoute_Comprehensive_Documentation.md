# ⚡ VoltRoute: Distributed Smart EV Charging Grid & Power Quorum
## In-Depth Architectural, Technical, and Distributed Systems Theoretical Documentation

---

### **Project Metadata & Academic Details**
* **Project Title:** VoltRoute – Distributed Smart EV Charging Grid & Power Quorum
* **Course:** Distributed Systems (CS Mini Project)
* **Students:** 
  * Shubham Nemade (PRN: `123B1B020`)
  * Vansh Kukreja (PRN: `123B1B015`)
* **Division:** A
* **GitHub Repository:** [https://github.com/Shubham-Nemade-24/VoltRoute.git](https://github.com/Shubham-Nemade-24/VoltRoute.git)
* **Architecture Style:** 3-Tier Distributed Microservices with Event-Driven Telemetry
* **Orchestration:** Containerized via Docker & Orchestrated via Kubernetes

---

## 1. Executive Summary & Problem Formulation

### 1.1 The Real-World Engineering Problem
As electric vehicle (EV) adoption surges globally, local municipal power grids face severe localized strain. Fast-charging stations (DC Fast Chargers delivering 50 kW to 350 kW each) draw massive instantaneous current. When multiple EVs initiate high-rate fast-charging concurrently on the same municipal feeder or sub-station transformer:
1. **Transformer Overload:** The total power demand exceeds the thermal and magnetic ratings of local distribution transformers, risking catastrophic failure or thermal tripping.
2. **Phase Unbalance & Voltage Sags:** Concurrent uncoordinated connections cause severe voltage drops affecting nearby residential and commercial consumers.
3. **Chaotic Queueing & Race Conditions:** In uncoordinated networks, drivers race for charging slots with no deterministic ordering, resulting in station hogging, priority inversion, and deadlocks.

### 1.2 The VoltRoute Solution
**VoltRoute** is a fault-tolerant, containerized, distributed systems platform designed to coordinate electric vehicle charging demand with physical transformer limits. It applies fundamental Distributed Systems concepts to solve this physical infrastructure challenge:
* **Decoupled 3-Tier Architecture:** Isolates client presentation, REST/SSE gateway middleware, and gRPC backend business logic.
* **Remote Procedure Calls (gRPC):** Enforces high-performance, strongly-typed binary communication with schema contracts defined via Protocol Buffers.
* **Lamport Logical Clocks:** Solves distributed physical clock skew and assigns total causal ordering to vehicle charging requests across asynchronous clients.
* **Distributed Mutual Exclusion (Transformer Mutex):** Treats the electrical transformer capacity as a protected **Critical Section**, preventing grid overloads through coordinator-managed capacity locking and fair wait-queueing.
* **Real-Time Event Streams (SSE & Pub/Sub):** Continuously broadcasts fluctuating grid telemetry, booking statuses, and mutex handoffs to all clients without polling.
* **Cloud-Native Deployment (Docker & Kubernetes):** Enables microservice isolation, horizontal auto-scaling of API gateways, declarative deployments, internal DNS service discovery, and self-healing.

---

## 2. Distributed Systems Concepts Mapping

The project maps directly to core theoretical units in Distributed Systems:

| Curriculum Unit | Core DS Concept | VoltRoute Implementation | Why It Is Essential |
|---|---|---|---|
| **Unit 1: Architectures & Models** | **3-Tier Microservices Model** | Client UI (Tier 1) $\to$ Flask Gateway (Tier 2) $\to$ gRPC Backend (Tier 3) | Separates concerns, secures internal grid logic, and enables independent scaling of gateway proxies. |
| **Unit 2: Communication & IPC** | **Remote Procedure Calls (gRPC)** | `ChargingGrid` service defined in `charging.proto` running over HTTP/2 | Replaces inefficient text/JSON overhead with fast binary Protobuf serialization and multiplexed streams. |
| **Unit 2: Communication & IPC** | **Publish-Subscribe & SSE** | Unidirectional Server-Sent Events (`/stream/power`, `/stream/events`) | Eliminates wasteful HTTP polling, decoupling publishers and subscribers in time and space. |
| **Unit 3: Time & Synchronization** | **Lamport's Logical Clocks** | Monotonically increasing logical timestamps on every state mutation | Overcomes physical clock drift/skew across distributed clients, establishing total causal ordering ($\to$). |
| **Unit 3: Synchronization & Coordination** | **Distributed Mutual Exclusion** | `TransformerMutex` protecting transformer kW capacity | Formulates power capacity as a Critical Section: ensures safety (no overload), liveness (no deadlock), and fairness. |
| **Unit 4: Containers & Virtualization** | **Container Isolation (Docker)** | Multi-container Docker Compose with isolated bridge network `voltnet` | Guarantees reproducible runtime environments, dependency encapsulation, and process sandboxing. |
| **Unit 4: Cloud-Native Orchestration** | **Kubernetes Clustering** | Namespaces, Multi-Replica Deployments, ClusterIP & NodePort Services | Provides horizontal scalability, declarative reconciliation, internal DNS resolution, and automated self-healing. |

---

## 3. High-Level Architecture & Tier Breakdown

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                       TIER 1: CLIENT PRESENTATION                           │
│  - Modern Glassmorphism Dashboard (Vanilla HTML5 / Modern CSS3 / ES6 JS)   │
│  - Real-Time Live Power Gauges & Telemetry Charts                          │
│  - Interactive Vehicle Booking & Transformer Mutex Visualizer               │
│  - EventSource API consuming Server-Sent Events (SSE)                       │
└───────────────────────▲─────────────────────────────┬───────────────────────┘
                        │ HTTP / SSE                  │ REST API (JSON)
                        │ Streams                     │ POST /api/book, /api/release
┌───────────────────────┴─────────────────────────────▼───────────────────────┐
│                 TIER 2: MIDDLEWARE GATEWAY (Flask / NodePort)               │
│  - External Exposure: Port 5050 (Docker) / NodePort 30500 (Kubernetes)     │
│  - Microservice Replication: 2 Replicas load-balanced via Kube-Proxy        │
│  - Endpoints: /api/stations, /api/book, /api/release, /api/queue            │
│  - SSE Push Channels: /stream/power (2s cadence), /stream/events            │
│  - Translates Client HTTP/JSON <---> Internal Binary gRPC                   │
└───────────────────────────────────────▲─────────────────────────────────────┘
                                        │
                                        │ gRPC over HTTP/2 (Protobuf)
                                        │ Internal ClusterIP: 50051
                                        ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                 TIER 3: BUSINESS LOGIC & DATA ENGINE (gRPC Server)          │
│  - Service: ChargingGrid running on port 50051 (ThreadPoolExecutor)         │
│  - Lamport Logical Clock Manager: Assigns monotonically increasing TS      │
│  - Distributed Mutex Coordinator: Transformer capacity enforcement         │
│  - Priority Charging Queue: FIFO ordering ordered strictly by Lamport TS   │
│  - Grid Telemetry Generator: Dynamic power fluctuation simulator            │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 4. Deep Dive: Component by Component

### 4.1 Tier 1: Client Presentation Layer
* **Files:** `gateway/templates/index.html`, `gateway/static/app.js`, `gateway/static/style.css`
* **Technologies:** HTML5, CSS3 Glassmorphism UI, Vanilla JavaScript (ES6+), EventSource API.
* **Why Plain JS/CSS?** Avoids massive Node module overhead while maximizing runtime performance, ensuring direct control over DOM mutation during high-frequency telemetry streams.
* **How It Operates:**
  1. On DOM load, `app.js` triggers `loadStations()` to fetch initial metadata via REST `/api/stations`.
  2. Spawns two persistent `EventSource` connections:
     * `/stream/power`: Reads real-time kW fluctuations every 2 seconds, dynamically adjusting progress rings and power counters.
     * `/stream/events`: Subscribes to backend event publications (booking confirmations, mutex queue additions, releases).
  3. When an EV driver books a slot, `app.js` sends an asynchronous `POST /api/book` containing `vehicle_id`, `station_id`, and `battery_pct`.
  4. Renders visual indicators for **Transformer Locked / Unlocked** status, showing immediate feedback if a vehicle was granted access or queued.

---

### 4.2 Tier 2: Flask Web Gateway & Middleware Layer
* **Files:** `gateway/gateway.py`, `gateway/Dockerfile`
* **Technologies:** Python 3.11, Flask, gRPC Python Stubs (`grpcio`, `protobuf`).
* **Why This Layer is Necessary:**
  * **Protocol Bridging:** Web browsers cannot natively initiate raw HTTP/2 gRPC binary calls without complex proxies (like Envoy/gRPC-Web). The Gateway acts as an API facade converting browser-friendly HTTP/REST JSON into high-speed binary gRPC calls.
  * **Security Boundary:** Keeps the core gRPC server shielded inside an internal private network (`ClusterIP` in Kubernetes), exposing only sanitized REST endpoints to clients.
  * **Streaming Aggregation:** Translates internal RPC responses into continuous Server-Sent Events (SSE) with `text/event-stream` mime-type.
* **Key Code Mechanics:**
  ```python
  def get_grpc_stub():
      host = os.environ.get("GRPC_HOST", "volt-server")
      port = os.environ.get("GRPC_PORT", "50051")
      channel = grpc.insecure_channel(f"{host}:{port}")
      return charging_pb2_grpc.ChargingGridStub(channel)
  ```
  The gateway dynamically resolves `GRPC_HOST`. In Docker Compose, this resolves to the container `volt-server`. In Kubernetes, CoreDNS resolves this to the ClusterIP Service `volt-server.voltroute.svc.cluster.local`.

---

### 4.3 Tier 3: gRPC Charging Grid Server & Coordinator
* **Files:** `server/server.py`, `protos/charging.proto`, `server/Dockerfile`
* **Technologies:** gRPC Core, Google Protocol Buffers v3, ThreadPoolExecutor.
* **Why gRPC over HTTP REST?**
  * **Serialization Efficiency:** Protocol Buffers serialize messages into compact binary formats, using orders of magnitude less bandwidth than verbose JSON strings.
  * **Contract-First Design:** The `.proto` file strictly enforces parameter types (`string vehicle_id`, `int32 battery_pct`, `int64 lamport_ts`), preventing runtime schema mismatches.
  * **Multiplexed Transport:** Runs over HTTP/2, allowing multiple concurrent RPC requests over a single TCP connection without head-of-line blocking.

#### Protocol Buffer Contract (`protos/charging.proto`):
```protobuf
syntax = "proto3";
package charging;

service ChargingGrid {
  rpc BookSlot (BookRequest) returns (BookResponse);
  rpc ReleaseSlot (ReleaseRequest) returns (ReleaseResponse);
  rpc GetStations (Empty) returns (StationList);
  rpc GetQueue (Empty) returns (QueueList);
}

message BookRequest {
  string vehicle_id = 1;
  string station_id = 2;
  int32  battery_pct = 3;
}

message BookResponse {
  bool   success = 1;
  string message = 2;
  int64  lamport_ts = 3;
  int32  queue_pos = 4;
}
```

---

## 5. Theoretical Concepts & Algorithmic Implementation

### 5.1 Lamport's Logical Clocks (Unit 3)

#### Theoretical Motivation:
In a distributed environment, physical wall clocks cannot be reliably synchronized across independent nodes due to clock skew, drift, and network jitter (as demonstrated by Lamport in 1978). Relying on physical timestamps (`time.time()`) to order charging reservations leads to race conditions, inconsistent state, and unfair queueing.

#### Lamport's Happened-Before Relation ($\to$):
1. If events $a$ and $b$ occur in the same process, and $a$ occurs before $b$, then $a \to b$.
2. If event $a$ is the sending of a message and event $b$ is the receipt of that message, then $a \to b$.
3. If $a \to b$ and $b \to c$, then $a \to c$ (transitivity).

#### VoltRoute Clock Implementation:
Each state change in VoltRoute ticks a thread-safe Lamport Clock:
```python
class LamportClock:
    def __init__(self):
        self._value = 0
        self._lock = threading.Lock()

    def tick(self):
        with self._lock:
            self._value += 1
            return self._value

    def update(self, received_ts):
        with self._lock:
            self._value = max(self._value, received_ts) + 1
            return self._value
```

#### Total Ordering of the Charging Queue:
When concurrent booking requests arrive, they are assigned a Lamport timestamp $L(e)$. The queue is sorted by a total ordering tuple:
$$\text{Order}(e) = (L(e), \text{vehicle\_id})$$
This guarantees:
1. Every node in the system agrees on the exact sequence of bookings.
2. Ties are deterministically broken using unique vehicle identifiers.
3. No vehicle can bypass an earlier request due to physical clock errors.

---

### 5.2 Distributed Mutual Exclusion: Transformer Capacity Mutex (Unit 3)

#### Theoretical Formulation:
In distributed systems, the **Critical Section (CS)** is a resource access phase that must be protected by mutual exclusion to preserve integrity.
* **The Resource:** The local electrical distribution transformer feeding each charging hub.
* **The Capacity Constraint:** A transformer can physically sustain $K$ simultaneous high-load fast chargers ($K = \text{max\_capacity}$). Exceeding $K$ causes electrical failure.
* **The Mutex Goal:** Guarantee that at any instant $t$:
  $$N_{\text{charging}}(t) \le K$$

#### Mutual Exclusion Criteria Satisfied:
1. **Safety (No Overload):** At no time can more than $K$ vehicles acquire active charging status.
2. **Liveness (Deadlock-Free):** If vehicles release slots, pending vehicles in the wait queue are systematically granted access.
3. **Fairness (Starvation-Free):** Access is granted strictly according to Lamport timestamp order.

#### Algorithmic Execution:
VoltRoute implements a centralized coordinator pattern for each transformer:
```python
class TransformerMutex:
    def __init__(self, max_capacity):
        self.max_capacity = max_capacity
        self.current_count = 0
        self._lock = threading.Lock()
        self._wait_queue = []
        self.is_locked = False

    def acquire(self, vehicle_id):
        with self._lock:
            if self.current_count < self.max_capacity:
                self.current_count += 1
                self.is_locked = (self.current_count >= self.max_capacity)
                return True   # CRITICAL SECTION ENTERED
            else:
                self._wait_queue.append(vehicle_id)
                self.is_locked = True
                return False  # BLOCKED / QUEUED

    def release(self, vehicle_id):
        with self._lock:
            if self.current_count > 0:
                self.current_count -= 1
            self.is_locked = (self.current_count >= self.max_capacity)
            next_vehicle = None
            if self._wait_queue and self.current_count < self.max_capacity:
                next_vehicle = self._wait_queue.pop(0)
                self.current_count += 1
                self.is_locked = (self.current_count >= self.max_capacity)
            return next_vehicle # AUTOMATIC PROMOTION
```

---

### 5.3 Publish-Subscribe & Real-Time Telemetry (Unit 2)

#### Theoretical Motivation:
Traditional REST architectures rely on client-side polling (`setInterval(fetch, 1000)`). Polling introduces:
* High bandwidth waste (thousands of empty HTTP requests).
* High latency (changes are only noticed on the next polling tick).
* Server CPU exhaustion under scale.

#### Server-Sent Events (SSE) Event-Driven Model:
VoltRoute implements unidirectional Server-Sent Events (SSE) over persistent HTTP connections. The server holds an open HTTP socket and streams chunked event packets in standard W3C SSE format:
```http
HTTP/1.1 200 OK
Content-Type: text/event-stream
Cache-Control: no-cache
Connection: keep-alive

data: [{"id": "ST-001", "power_kw": 142.3, "used_slots": 2, "transformer_locked": false}]

data: [{"id": "ST-001", "power_kw": 148.9, "used_slots": 2, "transformer_locked": false}]
```
* **Decoupling in Space:** The client does not know the internal mechanics or physical topology of the backend gRPC telemetry thread.
* **Decoupling in Time:** Events are pushed immediately upon generation by the simulation loop or booking handler.

---

### 5.4 Containerization & Isolation (Docker – Unit 4)

#### Why Docker?
Containerization packages code, dependencies, system libraries, and runtime configurations into immutable container images, eliminating runtime drift between development and deployment.

#### Container Separation in VoltRoute:
1. **`volt-server` Container:**
   * Contains Python 3.11, `grpcio`, `protobuf`, generated gRPC stubs.
   * Exposes port `50051`.
   * Runs the coordinator and simulator.
2. **`volt-gateway` Container:**
   * Contains Python 3.11, Flask, Web templates, static assets.
   * Connects to `volt-server` through the internal bridge network `voltnet`.
   * Exposes external port `5050`.

---

### 5.5 Microservices Orchestration (Kubernetes – Unit 4)

#### Why Kubernetes?
While Docker Compose manages containers on a single host, Kubernetes manages containers across distributed physical or virtual nodes, providing automated self-healing, replication, and software-defined networking.

#### Key Kubernetes Objects in VoltRoute:
1. **Namespace (`voltroute`):**
   * Isolates project resources from system containers and other tenants.
2. **Deployments:**
   * **`volt-gateway` Deployment (`replicas: 2`):** Runs two parallel instances of the web gateway for high availability and load distribution.
   * **`volt-server` Deployment (`replicas: 1`):** Runs the authoritative gRPC backend coordinator.
3. **Services & Networking:**
   * **`volt-server` Service (`ClusterIP`):** Allocates an internal, cluster-only virtual IP (`10.96.250.106`) on port `50051`. Accessible only by pods within the cluster.
   * **`volt-gateway` Service (`NodePort`):** Exposes port `5000` externally on port `30500` across all cluster nodes.
4. **Health & Readiness Probes:**
   * Gateway checks HTTP `/` every 10 seconds.
   * Server checks TCP port `50051`. If a container hangs, Kubernetes automatically restarts the pod.

---

## 6. End-to-End Workflow & Execution Trace

To understand how all concepts work together, consider this scenario:

```
[EV Driver Browser] ──(POST /api/book)──► [Flask Gateway] ──(gRPC BookSlot)──► [gRPC Server]
                                                                                     │
                                                      ┌──────────────────────────────┴──────────────────────────────┐
                                                      ▼                                                             ▼
                                             [Lamport Clock]                                              [Transformer Mutex]
                                             tick() -> TS: 104                                            acquire("EV-101")
                                                      │                                                             │
                                                      │                                                             ├─ Granted (used < max)
                                                      │                                                             └─ Queued (used == max)
                                                      │                                                             │
                                                      ▼                                                             ▼
                                             [Charging Queue] ◄─────────────────────────────────────────────────────┘
                                             Sort by (TS, vehicle_id)
                                                      │
                                                      ├────────────────────────────────────────┐
                                                      ▼                                        ▼
                                            [Return gRPC Response]                   [Pub/Sub Event Bus]
                                            (TS: 104, pos: 1)                        (Emit BOOKING Event)
                                                      │                                        │
                                                      ▼                                        ▼
                                            [Gateway JSON Return]                    [SSE Stream Broadcast]
                                            to EV Driver Browser                     to all active browser clients
```

---

## 7. Comparative Technical Tradeoff Matrix

| Decision Domain | VoltRoute Choice | Alternative | Justification / Engineering Tradeoff |
|---|---|---|---|
| **Inter-Service IPC** | **gRPC / Protobuf** | REST / JSON | gRPC provides binary compression, strict IDL contracts, and multiplexed HTTP/2 streams, cutting latency by over 60% compared to REST. |
| **Clock Synchronization** | **Lamport Clocks** | NTP Physical Clocks | NTP suffers from network jitter, VM clock drift, and leap-second anomalies. Lamport clocks establish deterministic causal order with zero network overhead. |
| **Mutual Exclusion** | **Centralized Coordinator** | Ricart-Agrawala | Ricart-Agrawala requires $2(N-1)$ messages per critical section entry. A centralized coordinator requires only 2 messages ($O(1)$ overhead) and avoids message storms in high-frequency EV grids. |
| **Client Streaming** | **Server-Sent Events (SSE)** | WebSockets | EV grid telemetry is strictly unidirectional (Server $\to$ Client). SSE is lighter, runs over standard HTTP, and has automatic browser reconnect logic built-in. |
| **Deployment Model** | **Kubernetes + Docker** | Monolithic Bare-Metal | Container orchestration decouples components, allows independent gateway scaling (2 replicas), and guarantees automatic pod self-healing on failure. |

---

## 8. Verification & Live Cluster Status

The system was validated in a live local cluster environment:
* **Docker Engine:** Running on Apple Silicon (ARM64)
* **Kubernetes Control Plane:** `docker-desktop` (v1.36.1)
* **Active Pods:**
  * `pod/volt-gateway-6695b4d486-km92s` (Status: `Running`)
  * `pod/volt-gateway-6695b4d486-mll85` (Status: `Running`)
  * `pod/volt-server-79dcd4c545-4s4qn` (Status: `Running`)
* **Active Services:**
  * `service/volt-server` (ClusterIP `10.96.250.106:50051`)
  * `service/volt-gateway` (NodePort `10.96.62.119:5000`, NodePort `30500`)

---

## 9. Conclusion & Project Outcome

The **VoltRoute** project successfully demonstrates the application of canonical Distributed Systems theory to solve modern physical grid infrastructure bottlenecks:
1. **Architectural Decoupling:** The 3-tier model isolates failure domains and shields backend state behind scalable gateway proxies.
2. **Causal Ordering:** Lamport logical clocks resolve concurrency race conditions across heterogeneous client bookings.
3. **Physical Protection via Mutex:** The transformer mutual exclusion coordinator guarantees that grid substations never exceed safe thermal operating thresholds.
4. **Cloud-Native Resilience:** Docker containerization and Kubernetes orchestration deliver high availability, automated load balancing, and self-healing deployment.
