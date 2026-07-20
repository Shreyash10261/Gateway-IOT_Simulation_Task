# Stage 1: Build
FROM golang:1.20-alpine AS builder
WORKDIR /app

# Install git for downloading dependencies
RUN apk add --no-cache git

# Copy go mod files and download dependencies (if present)
# Using wildcard in case they are not yet initialized
COPY go.mod go.sum* ./
RUN if [ -f go.mod ]; then go mod download; fi

# Copy source code
COPY . .

# Build the Go application
# Assuming main.go will be in cmd/gateway/main.go or root
RUN if [ -f cmd/gateway/main.go ]; then \
        CGO_ENABLED=0 GOOS=linux go build -o gateway ./cmd/gateway/main.go; \
    elif [ -f main.go ]; then \
        CGO_ENABLED=0 GOOS=linux go build -o gateway main.go; \
    else \
        echo "No main.go found, creating a dummy binary for scaffolding"; \
        echo 'package main; import "fmt"; func main() { fmt.Println("Gateway Placeholder") }' > main.go; \
        CGO_ENABLED=0 GOOS=linux go build -o gateway main.go; \
    fi

# Stage 2: Minimal Runtime
FROM alpine:latest
WORKDIR /app

# Create a non-root user and group
RUN addgroup -S appgroup && adduser -S appuser -G appgroup

# Install necessary network tools for testing
RUN apk add --no-cache ca-certificates tzdata curl iproute2 tcpdump iptables iputils

# Copy the binary from builder
COPY --from=builder /app/gateway .

# Change ownership of the app directory to the non-root user
RUN chown -R appuser:appgroup /app

# Switch to the non-root user
USER appuser

# Command to run the executable
CMD ["./gateway"]
