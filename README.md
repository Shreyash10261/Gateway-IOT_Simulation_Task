# IoT Edge Gateway

The **Edge Gateway** is a high-performance, enterprise-grade routing microservice written in Go. It acts as the critical bridge between cloud infrastructure (Azure IoT Hub) and local physical hardware devices (or simulators) residing on an isolated local area network (LAN).

## 🚀 Key Features

* **Clean Architecture**: Strictly separated layers (Core Domain, Use Cases, Ports/Adapters) for maximum testability and maintainability.
* **Northbound Cloud Integration**: Securely connects to Azure IoT Hub using X.509 Certificate authentication and native MQTT for high-speed Cloud-to-Device (C2D) messaging.
* **Southbound Protocol Routing**: Dynamically translates JSON cloud payloads into raw TCP bytes for specific hardware protocols (e.g., PJLink for projectors).
* **High Concurrency**: Utilizes a Go worker pool pattern to handle thousands of simultaneous device commands without blocking the main event loop.
* **Observability**: Built-in Prometheus metrics server (`:9090`) tracking command execution latency, success/error rates, and worker pool saturation.
* **Network Isolation (MacVLAN)**: Natively interfaces with Docker MacVLANs to route traffic directly to local device IP addresses while maintaining internet access for Azure via the default bridge.

## 📂 Project Structure

```text
edge-gateway/
├── cmd/
│   └── gateway/
│       └── main.go                 # Application entrypoint & dependency injection
├── internal/
│   ├── core/
│   │   ├── domain/                 # Core business models (Device, Telemetry, etc.)
│   │   ├── ports/                  # Interfaces defining system boundaries
│   │   └── usecases/               # Application logic (Routing Engine)
│   └── adapters/
│       ├── cloud/                  # Azure IoT Hub MQTT implementation
│       ├── southbound/             # Raw TCP / PJLink dialers
│       ├── registry/               # Local devices.json parsing
│       └── metrics/                # Prometheus metrics server
├── Dockerfile                      # Multi-stage Alpine build for the gateway
├── config.yaml                     # Application configuration
├── devices.json                    # Local Device Registry (IP map)
└── generate_certs.go               # Utility to generate Azure X.509 identities
```

## 🛠️ Prerequisites & Setup

Because this Gateway connects to Azure using a Zero-Trust X.509 Certificate model, you **must** generate your local cryptographic identity before booting the Docker containers.

1. **Generate Certificates:**
   Run the certificate helper script. This will instantly create a `cert.pem` and `key.pem` in this directory.
   ```bash
   cd edge-gateway
   go run generate_certs.go
   ```
2. **Register with Azure:**
   The script will output a Thumbprint. Go to your Azure IoT Hub portal, create a new device, select `X.509 Self-Signed`, and paste the generated Thumbprint.

*(Note: Never commit your `.pem` files to version control! They are safely ignored by the `.gitignore`).*

## 🐳 Running the Gateway

The Edge Gateway is designed to run seamlessly alongside the telemetry and simulation services via Docker Compose.

1. Build and boot the entire architecture in detached mode:
   ```bash
   docker compose up --build -d
   ```
2. Verify the Gateway has connected to Azure:
   ```bash
   docker logs -f iot_edge_gateway
   ```

## 📊 Metrics & Monitoring

Once running, the Gateway exposes a Prometheus `/metrics` endpoint on port `9090`..
You can view raw telemetry by visiting:
`http://localhost:9090/metrics`
