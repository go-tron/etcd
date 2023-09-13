package etcd

import (
	"testing"
	"time"
)

func TestClient_Put(t *testing.T) {
	c := New(&Config{
		Endpoints:   []string{"http://127.0.0.1:6079", "http://127.0.0.1:6179", "http://127.0.0.1:6279"},
		Username:    "root",
		Password:    "Pf*rm1D^V&hBDAKC",
		DialTimeout: 5 * time.Second,
	})

	if err := c.Put("/foo1/a", "bar", WithTTL(5)); err != nil {
		t.Fatal(err)
	}
	if err := c.Put("/foo1/b", "bar", WithTTL(5)); err != nil {
		t.Fatal(err)
	}

	kvs, err := c.Get("/test-etcd", WithPrefix())
	if err != nil {
		t.Fatal(err)
	}
	for _, ev := range kvs {
		t.Logf("%s : %s\n", ev.Key, ev.Value)
	}
}
