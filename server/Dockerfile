FROM python:3.11-slim

WORKDIR /app

# Install protobuf compiler
RUN apt-get update && apt-get install -y --no-install-recommends \
    protobuf-compiler && rm -rf /var/lib/apt/lists/*

# Copy requirements and install
COPY server/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy proto file and generate Python stubs
COPY protos/charging.proto ./protos/
RUN python -m grpc_tools.protoc -I./protos --python_out=. --grpc_python_out=. protos/charging.proto

# Copy server code
COPY server/server.py .

EXPOSE 50051

CMD ["python", "server.py"]
