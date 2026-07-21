FROM golang:alpine AS builder
WORKDIR /build
COPY go.mod go.sum ./

RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o gateway ./cmd/gateway


FROM alpine:latest
RUN apk --no-cache add ca-certificates curl iproute2 tcpdump iptables iputils-ping
WORKDIR /app

COPY --from=builder /build/gateway .
COPY entrypoint.sh .

RUN chmod +x ./entrypoint.sh

EXPOSE 8085 9090

ENTRYPOINT ["./entrypoint.sh"]
CMD ["./gateway"]
