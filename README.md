# ⚡ VoltRoute – Distributed Smart EV Charging Grid

A distributed systems mini project implementing a Smart EV Charging Station Network using microservice architecture.

## 📋 Distributed Systems Concepts Implemented

| Unit | Concept | Implementation |
|------|---------|---------------|
| **Unit 1** | 3-Tier Architecture | Client UI → Flask Gateway → gRPC Server |
| **Unit 2** | RPC (gRPC) | Protocol Buffers + gRPC service for station booking |
| **Unit 2** | SSE (Server-Sent Events) | Real-time power telemetry stream |
| **Unit 2** | Pub/Sub | Event broadcasting for booking alerts & mutex grants |
| **Unit 3** | Lamport Logical Clock | Total ordering of vehicle charging queue |
| **Unit 3** | Mutual Exclusion | Centralized mutex for transformer capacity control |
| **Unit 4** | Docker | Containerized microservices with Docker Compose |
| **Unit 4** | Kubernetes | K8s Deployments, Services, Pods, Namespaces |

## 🏗️ Architecture

```
┌──────────────┐     REST/SSE      ┌──────────────┐      gRPC       ┌──────────────┐
│   Tier 1     │ ───────────────►  │   Tier 2     │ ─────────────►  │   Tier 3     │
│  EV Driver   │                   │ Flask Gateway│                  │ Charging Grid│
│  Browser UI  │ ◄─── SSE Stream   │  (Port 5000) │ ◄── Protobuf    │  (Port 50051)│
└──────────────┘                   └──────────────┘                  └──────────────┘
```

## 🚀 How to Run

### Docker Compose
```bash
cd voltroute
docker compose up --build
```
Open `http://localhost:5000` in your browser.

### Kubernetes (Minikube)
```bash
minikube start
eval $(minikube docker-env)
docker build -f server/Dockerfile -t voltroute/server:latest .
docker build -f gateway/Dockerfile -t voltroute/gateway:latest .
kubectl apply -f k8s/namespace.yaml
kubectl apply -f k8s/server-deployment.yaml
kubectl apply -f k8s/gateway-deployment.yaml
minikube service volt-gateway -n voltroute
```

## 🛠️ Technologies Used

- **Python 3.11** – Backend language
- **Flask** – Web framework (Tier 2 Gateway)
- **gRPC + Protobuf** – Remote Procedure Call framework
- **Docker** – Containerization
- **Kubernetes** – Container orchestration
- **HTML/CSS/JS** – Frontend UI
- **SSE (EventSource API)** – Real-time streaming
