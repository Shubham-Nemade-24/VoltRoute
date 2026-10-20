FROM python:3.11-slim

WORKDIR /app

# Install protobuf compiler
RUN apt-get update && apt-get install -y --no-install-recommends \
    protobuf-compiler && rm -rf /var/lib/apt/lists/*

# Copy requirements and install
COPY gateway/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy proto file and generate Python stubs
COPY protos/charging.proto ./protos/
RUN python -m grpc_tools.protoc -I./protos --python_out=. --grpc_python_out=. protos/charging.proto

# Copy gateway code and static files
COPY gateway/gateway.py .
COPY gateway/templates/ ./templates/
COPY gateway/static/ ./static/

EXPOSE 5000

CMD ["python", "gateway.py"]
