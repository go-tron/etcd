package etcd

import (
	"fmt"
	"testing"
	"time"
)

func TestNewDiscovery(t *testing.T) {
	sr, err := NewDiscovery(&DiscoveryConfig{
		AppName:      "test-app",
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
		for {
			v := <-sr.AddSubscribe()
			fmt.Println("AddSubscribe", v)
		}
	}()
	go func() {
		for v := range sr.RemoveSubscribe() {
			fmt.Println("RemoveSubscribe", v)
		}
	}()

	//go func() {
	//	for true {
	//		time.Sleep(time.Second)
	//		fmt.Println(sr.GetList())
	//	}
	//}()

	fmt.Println("start")

	select {}
}
