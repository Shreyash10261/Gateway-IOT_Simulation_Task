package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/pem"
	"fmt"
	"math/big"
	"os"
	"strings"
	"time"
)

func main() {
	deviceID := "edge-gateway-sim" // This MUST match your Azure IoT Hub Device ID

	// 1. Generate Private Key (P-256 Elliptic Curve is fast and secure)
	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		fmt.Println("Error generating key:", err)
		os.Exit(1)
	}

	// 2. Create Certificate Template
	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			CommonName: deviceID, // Azure IoT Hub strictly requires the CN to match the Device ID
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().AddDate(1, 0, 0), // Valid for 1 year
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
	}

	// 3. Create the self-signed certificate
	derBytes, err := x509.CreateCertificate(rand.Reader, &template, &template, &privateKey.PublicKey, privateKey)
	if err != nil {
		fmt.Println("Error creating certificate:", err)
		os.Exit(1)
	}

	// 4. Save cert.pem
	certOut, _ := os.Create("cert.pem")
	pem.Encode(certOut, &pem.Block{Type: "CERTIFICATE", Bytes: derBytes})
	certOut.Close()

	// 5. Save key.pem
	keyBytes, _ := x509.MarshalECPrivateKey(privateKey)
	keyOut, _ := os.Create("key.pem")
	pem.Encode(keyOut, &pem.Block{Type: "EC PRIVATE KEY", Bytes: keyBytes})
	keyOut.Close()

	// 6. Calculate SHA-256 Thumbprint (Required for Azure IoT Hub registration)
	hash := sha256.Sum256(derBytes)
	thumbprint := strings.ToUpper(hex.EncodeToString(hash[:]))

	fmt.Println("✅ Successfully generated cert.pem and key.pem")
	fmt.Println("==================================================")
	fmt.Println("AZURE IOT HUB REGISTRATION DETAILS")
	fmt.Println("==================================================")
	fmt.Println("Device ID:            ", deviceID)
	fmt.Println("Authentication Type:   X.509 Self-Signed")
	fmt.Println("Primary Thumbprint:   ", thumbprint)
	fmt.Println("Secondary Thumbprint: ", thumbprint)
	fmt.Println("==================================================")
}
