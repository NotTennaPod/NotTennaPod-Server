package listen

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/private"
)

// Certificate serves the certificate in a file pair, reloaded on request
// (SIGHUP), so a renewed certificate is used without a restart (PROTOCOL §14).
type Certificate struct {
	certFile, keyFile string
	mu                sync.RWMutex
	cert              *tls.Certificate
	notAfter          time.Time
}

// LoadCertificate reads a PEM certificate chain and key.
func LoadCertificate(certFile, keyFile string) (*Certificate, error) {
	c := &Certificate{certFile: certFile, keyFile: keyFile}
	if err := c.Reload(); err != nil {
		return nil, err
	}
	return c, nil
}

// Reload reads the files again; on an error the certificate in use stays.
// The key must belong to the server's user or root and be private to its
// owner.
func (c *Certificate) Reload() error {
	keyPEM, err := private.ReadKeyFile(c.keyFile)
	if err != nil {
		return fmt.Errorf("listen: the HTTPS key: %w", err)
	}
	certPEM, err := os.ReadFile(c.certFile)
	if err != nil {
		return fmt.Errorf("listen: loading %s: %w", c.certFile, err)
	}
	pair, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return fmt.Errorf("listen: loading %s: %w", c.certFile, err)
	}
	if len(pair.Certificate) == 0 {
		return errors.New("listen: the certificate file holds no certificate")
	}
	leaf, err := x509.ParseCertificate(pair.Certificate[0])
	if err != nil {
		return err
	}
	pair.Leaf = leaf
	c.mu.Lock()
	c.cert, c.notAfter = &pair, leaf.NotAfter
	c.mu.Unlock()
	return nil
}

// Config is the TLS configuration serving the current certificate.
func (c *Certificate) Config() *tls.Config {
	return &tls.Config{
		MinVersion: tls.VersionTLS12,
		GetCertificate: func(*tls.ClientHelloInfo) (*tls.Certificate, error) {
			c.mu.RLock()
			defer c.mu.RUnlock()
			return c.cert, nil
		},
	}
}

// NotAfter is when the certificate in use expires.
func (c *Certificate) NotAfter() time.Time {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.notAfter
}

// ExpiryWarning says when the certificate expires within two weeks, or has.
func (c *Certificate) ExpiryWarning(now time.Time) string {
	left := c.NotAfter().Sub(now)
	switch {
	case left <= 0:
		return "the HTTPS certificate has expired; browsers and phones refuse the server until it is renewed"
	case left < 14*24*time.Hour:
		return fmt.Sprintf("the HTTPS certificate expires in %d days; renew it and send the server SIGHUP", int(left.Hours()/24)+1)
	}
	return ""
}
