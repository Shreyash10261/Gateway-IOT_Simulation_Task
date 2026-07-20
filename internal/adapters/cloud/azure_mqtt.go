package cloud

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/team/edge-gateway/internal/core/domain"
)

type AzureMQTTClient struct {
	hostname  string
	deviceID  string
	client    mqtt.Client
	callbacks []func(command domain.CloudCommand)
	connected bool
}

func NewAzureMQTTClient(hostname string, _ interface{}) *AzureMQTTClient {
	return &AzureMQTTClient{
		hostname: hostname,
		deviceID: "edge-gateway-sim",
	}
}

func (c *AzureMQTTClient) Connect() error {
	slog.Info("Connecting to Real Azure IoT Hub", "hostname", c.hostname)

	// Load X.509 Certificates
	cert, err := tls.LoadX509KeyPair("cert.pem", "key.pem")
	if err != nil {
		return fmt.Errorf("could not load x509 key pair: %w", err)
	}
	
	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}

	broker := fmt.Sprintf("tls://%s:8883", c.hostname)
	
	opts := mqtt.NewClientOptions()
	opts.AddBroker(broker)
	opts.SetClientID(c.deviceID)
	opts.SetUsername(fmt.Sprintf("%s/%s/?api-version=2021-04-12", c.hostname, c.deviceID))
	opts.SetTLSConfig(tlsConfig)
	
	opts.SetAutoReconnect(true)
	opts.SetMaxReconnectInterval(10 * time.Second)
	
	opts.SetOnConnectHandler(func(client mqtt.Client) {
		slog.Info("Successfully established TLS connection to Azure IoT Hub!")
		c.connected = true
		
		// Azure C2D topic
		topic := fmt.Sprintf("devices/%s/messages/devicebound/#", c.deviceID)
		
		// Resubscribe on every connection (crucial for auto-reconnect)
		for _, callback := range c.callbacks {
			cb := callback // capture
			client.Subscribe(topic, 1, func(client mqtt.Client, msg mqtt.Message) {
				slog.Info("Received C2D message from Azure", "topic", msg.Topic())
				var cmd domain.CloudCommand
				if err := json.Unmarshal(msg.Payload(), &cmd); err != nil {
					slog.Error("Failed to parse incoming Azure command", "err", err)
					return
				}
				cmd.IssuedAt = time.Now()
				cb(cmd)
			})
		}
		
		if len(c.callbacks) > 0 {
			slog.Info("Successfully subscribed to Azure IoT C2D topic")
		}
	})
	
	opts.SetConnectionLostHandler(func(client mqtt.Client, err error) {
		slog.Warn("Lost connection to Azure IoT Hub", "err", err)
		c.connected = false
	})

	c.client = mqtt.NewClient(opts)
	token := c.client.Connect()
	if token.Wait() && token.Error() != nil {
		return token.Error()
	}

	return nil
}

func (c *AzureMQTTClient) Disconnect() error {
	slog.Info("Disconnecting from Azure IoT Hub")
	if c.client != nil {
		c.client.Disconnect(250)
	}
	c.connected = false
	return nil
}

func (c *AzureMQTTClient) IsConnected() bool {
	return c.connected && c.client != nil && c.client.IsConnected()
}

func (c *AzureMQTTClient) SubscribeToCommands(callback func(command domain.CloudCommand)) error {
	c.callbacks = append(c.callbacks, callback)
	
	// If already connected, trigger the subscription immediately
	if c.IsConnected() {
		topic := fmt.Sprintf("devices/%s/messages/devicebound/#", c.deviceID)
		c.client.Subscribe(topic, 1, func(client mqtt.Client, msg mqtt.Message) {
			slog.Info("Received C2D message from Azure", "topic", msg.Topic())
			var cmd domain.CloudCommand
			if err := json.Unmarshal(msg.Payload(), &cmd); err != nil {
				slog.Error("Failed to parse incoming Azure command", "err", err)
				return
			}
			cmd.IssuedAt = time.Now()
			callback(cmd)
		})
		slog.Info("Successfully subscribed to Azure IoT C2D topic")
	}
	
	return nil
}

func (c *AzureMQTTClient) Publish(ctx context.Context, topic string, payload []byte) error {
	if !c.IsConnected() {
		return fmt.Errorf("cannot publish: not connected")
	}

	token := c.client.Publish(topic, 1, false, payload)
	if token.WaitTimeout(5 * time.Second) {
		if token.Error() != nil {
			return token.Error()
		}
	} else {
		return fmt.Errorf("publish timeout")
	}

	return nil
}

func (c *AzureMQTTClient) SendTelemetry(ctx context.Context, telemetry *domain.DeviceTelemetry) error {
	// Azure D2C (Device-to-Cloud) topic structure
	topic := fmt.Sprintf("devices/%s/messages/events/", c.deviceID)
	
	payload, err := json.Marshal(telemetry)
	if err != nil {
		return err
	}

	if err := c.Publish(ctx, topic, payload); err != nil {
		return err
	}

	slog.Info("Published telemetry to Azure IoT Hub", "device_id", telemetry.DeviceID)
	return nil
}
