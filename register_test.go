package etcd

import (
	"testing"
	"time"
)

func TestNewRegister(t *testing.T) {
	register, err := NewRegister(&RegisterConfig{
		AppName:      "test-app",
		Node:         "node-01",
		Addr:         "1.1.1.1",
		TTL:          5,
		EtcdInstance: nil,
		EtcdConfig: &Config{
			Endpoints:   []string{"http://127.0.0.1:10179", "http://127.0.0.1:10279", "http://127.0.0.1:10379"},
			Username:    "root",
			Password:    "Pf*rm1D^V&hBDAKC",
			DialTimeout: 5 * time.Second,
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := register.Close(); err != nil {
		t.Log(err)
	}

	time.Sleep(time.Second)
	if err := register.Register(); err != nil {
		t.Log(err)
	}

	go func() {
		for {
			time.Sleep(6 * time.Second)
		}
	}()

	select {}
}
