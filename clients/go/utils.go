package chroma

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/pem"
	"log"
	"math"
	"math/big"
	"net"
	"os"
	"time"
)

func CreateSelfSignedCert(certPath, keyPath string) {
	// Generate a private key
	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		log.Fatalf("Failed to generate private key: %v", err)
	}

	// Prepare certificate
	notBefore := time.Now()
	notAfter := notBefore.Add(365 * 24 * time.Hour) // Valid for 1 year

	serialNumberLimit := new(big.Int).Lsh(big.NewInt(1), 128)
	serialNumber, err := rand.Int(rand.Reader, serialNumberLimit)
	if err != nil {
		log.Fatalf("Failed to generate serial number: %v", err)
	}

	template := x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			Organization: []string{"Chroma, Inc."},
			CommonName:   "localhost",
		},
		NotBefore:             notBefore,
		NotAfter:              notAfter,
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		DNSNames:              []string{"localhost"},
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
	}

	// Create the certificate
	derBytes, err := x509.CreateCertificate(rand.Reader, &template, &template, &privateKey.PublicKey, privateKey)
	if err != nil {
		log.Fatalf("Failed to create certificate: %v", err)
	}

	// Write the certificate to file
	certOut, err := os.Create(certPath)
	if err != nil {
		log.Fatalf("Failed to open cert.pem for writing: %v", err)
	}
	if err := pem.Encode(certOut, &pem.Block{Type: "CERTIFICATE", Bytes: derBytes}); err != nil {
		log.Fatalf("Failed to write data to cert.pem: %v", err)
	}
	if err := certOut.Close(); err != nil {
		log.Fatalf("Error closing cert.pem: %v", err)
	}
	log.Printf("Written %s", certPath)

	// Write the private key to file
	keyOut, err := os.OpenFile(keyPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		log.Fatalf("Failed to open key.pem for writing: %v", err)
	}
	privBytes, err := x509.MarshalPKCS8PrivateKey(privateKey)
	if err != nil {
		log.Fatalf("Unable to marshal private key: %v", err)
	}
	if err := pem.Encode(keyOut, &pem.Block{Type: "PRIVATE KEY", Bytes: privBytes}); err != nil {
		log.Fatalf("Failed to write data to key.pem: %v", err)
	}
	if err := keyOut.Close(); err != nil {
		log.Fatalf("Error closing key.pem: %v", err)
	}
	log.Printf("Written %s", keyPath)
}

func packEmbeddingSafely(embedding []float32) string {
	packed, err := packFloat32Slice(embedding)
	if err != nil {
		clamped := make([]float32, len(embedding))
		for i, v := range embedding {
			clamped[i] = clampF32(v)
		}
		packed = float32ToBytes(clamped)
	}
	return base64.StdEncoding.EncodeToString(packed)
}

func packFloat32Slice(input []float32) ([]byte, error) {
	out := make([]float32, len(input))
	copy(out, input)
	return float32ToBytes(out), nil
}

func clampF32(v float32) float32 {
	if math.IsNaN(float64(v)) { // NaN check
		return float32(math.NaN())
	}
	switch {
	case v > math.MaxFloat32:
		return float32(math.Inf(1))
	case v < -math.MaxFloat32:
		return float32(math.Inf(-1))
	default:
		return v
	}
}

func float32ToBytes(floats []float32) []byte {
	buf := make([]byte, 4*len(floats))
	for i, f := range floats {
		bits := math.Float32bits(f)
		buf[4*i+0] = byte(bits)
		buf[4*i+1] = byte(bits >> 8)
		buf[4*i+2] = byte(bits >> 16)
		buf[4*i+3] = byte(bits >> 24)
	}
	return buf
}
