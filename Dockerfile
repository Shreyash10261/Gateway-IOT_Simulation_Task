# Stage 1: Build the Go binary
FROM golang:alpine AS builder

# Set the Current Working Directory inside the container
WORKDIR /build

# Copy go mod and sum files
COPY go.mod go.sum ./

# Download all dependencies. Dependencies will be cached if the go.mod and go.sum files are not changed
RUN go mod download

# Copy the source from the current directory to the Working Directory inside the container
COPY . .

# Build the Go app statically (no C dependencies)
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o gateway ./cmd/gateway

# Stage 2: A minimal alpine image to run the binary
FROM alpine:latest

# Install CA certificates so the Gateway can verify Azure's SSL certificates
RUN apk --no-cache add ca-certificates

WORKDIR /app

# Copy the Pre-built binary file from the previous stage
COPY --from=builder /build/gateway .

# Expose the Health and Metrics ports
EXPOSE 8085 9090

# Command to run the executable
ENTRYPOINT ["./gateway"]
