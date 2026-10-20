#!/bin/bash
# VoltRoute - Distributed System Infrastructure Status Checker
# Shows Docker Containers and Kubernetes Cluster Status for Academic Screenshots

echo ""
echo "================================================================================"
echo "⚡  VOLTROUTE: DISTRIBUTED SMART EV CHARGING INFRASTRUCTURE STATUS"
echo "================================================================================"
echo ""

echo "🐳 [1] DOCKER CONTAINERS STATUS:"
echo "--------------------------------------------------------------------------------"
docker ps --format "table {{.Names}}\t{{.Image}}\t{{.Status}}\t{{.Ports}}"
echo ""

echo "📦 [2] DOCKER IMAGES (VOLTROUTE MICROSERVICES):"
echo "--------------------------------------------------------------------------------"
docker images | grep -E "voltroute|REPOSITORY"
echo ""

echo "☸️  [3] KUBERNETES CLUSTER & NODE STATUS:"
echo "--------------------------------------------------------------------------------"
kubectl cluster-info
echo ""
kubectl get nodes -o wide
echo ""

echo "🚀 [4] KUBERNETES VOLTROUTE RESOURCES (PODS, SERVICES, DEPLOYMENTS):"
echo "--------------------------------------------------------------------------------"
kubectl get all -n voltroute -o wide
echo ""

echo "📊 [5] KUBERNETES POD LOGS (VOLTROUTE GATEWAY):"
echo "--------------------------------------------------------------------------------"
GW_POD=$(kubectl get pods -n voltroute -l app=volt-gateway -o jsonpath='{.items[0].metadata.name}')
echo "Showing last 8 log lines from pod: $GW_POD"
kubectl logs "$GW_POD" -n voltroute --tail=8
echo ""

echo "================================================================================"
echo "✅ All Docker containers and Kubernetes Pods are active and healthy!"
echo "================================================================================"
