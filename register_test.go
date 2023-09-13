package etcd

import (
	"testing"
	"time"
)

func TestNewRegister(t *testing.T) {
	_, err := NewRegister(&RegisterConfig{
		AppName:      "test-app",
		Node:         "node-01",
		Addr:         "1.1.1.1",
		TTL:          5,
		EtcdInstance: nil,
		EtcdConfig: &Config{
			Endpoints:   []string{"http://127.0.0.1:6079", "http://127.0.0.1:6179", "http://127.0.0.1:6279"},
			Username:    "root",
			Password:    "Pf*rm1D^V&hBDAKC",
			DialTimeout: 5 * time.Second,
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	go func() {
		for true {
			time.Sleep(6 * time.Second)
		}
	}()

	select {}
}
