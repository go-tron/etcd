package etcd

import (
	"errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"sync"
	"time"
)

type ServiceConfig[T any] struct {
	AppName          string
	CreateClientFunc func(grpc.ClientConnInterface) T
	EtcdConfig       *Config
	EtcdInstance     *Client
}
type ServiceOption[T any] func(*ServiceConfig[T])

func ServiceWithAppName[T any](val string) ServiceOption[T] {
	return func(conf *ServiceConfig[T]) {
		conf.AppName = val
	}
}
func ServiceWithCreateClientFunc[T any](val func(grpc.ClientConnInterface) T) ServiceOption[T] {
	return func(conf *ServiceConfig[T]) {
		conf.CreateClientFunc = val
	}
}
func ServiceWithEtcdConfig[T any](val *Config) ServiceOption[T] {
	return func(conf *ServiceConfig[T]) {
		conf.EtcdConfig = val
	}
}
func ServiceWithEtcdInstance[T any](val *Client) ServiceOption[T] {
	return func(conf *ServiceConfig[T]) {
		conf.EtcdInstance = val
	}
}

func NewService[T any](config *ServiceConfig[T], opts ...ServiceOption[T]) *Service[T] {
	for _, apply := range opts {
		if apply != nil {
			apply(config)
		}
	}
	if config.AppName == "" {
		panic("AppName 必须设置")
	}
	if config.CreateClientFunc == nil {
		panic("CreateClientFunc 必须设置")
	}
	if config.EtcdInstance == nil {
		if config.EtcdConfig == nil {
			panic("请设置etcd实例或者连接配置")
		}
		config.EtcdInstance = New(config.EtcdConfig)
	}

	s := &Service[T]{
		Discovery: MustNewDiscovery(&DiscoveryConfig{
			AppName:      config.AppName,
			EtcdInstance: config.EtcdInstance,
		}),
		NodeList: &sync.Map{},
	}
	go func() {
		for v := range s.AddSubscribe() {
			for nodeName, addr := range v {
				val, ok := s.NodeList.LoadAndDelete(nodeName)
				if ok {
					val.(*Node[T]).Conn.Close()
				}
				node, err := NewNodeClient[T](addr, config.CreateClientFunc)
				if err == nil {
					s.NodeList.Store(nodeName, node)
				}
			}
		}
	}()
	go func() {
		for nodeName := range s.RemoveSubscribe() {
			val, ok := s.NodeList.LoadAndDelete(nodeName)
			if ok {
				val.(*Node[T]).Conn.Close()
			}
		}
	}()
	return s
}

func NewNodeClient[T any](addr string, createClientFunc func(grpc.ClientConnInterface) T) (client *Node[T], err error) {
	conn, err := grpc.Dial(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                10 * time.Second,
			Timeout:             10 * time.Second,
			PermitWithoutStream: true,
		}),
	)
	if err != nil {
		return nil, err
	}

	return &Node[T]{
		Addr:   addr,
		Conn:   conn,
		Client: createClientFunc(conn),
	}, nil
}

type Node[T any] struct {
	Addr   string
	Conn   *grpc.ClientConn
	Client T
}

type Service[T any] struct {
	*Discovery
	NodeList *sync.Map
}

func (s *Service[T]) FindNode(nodeName string) (*Node[T], error) {
	var node *Node[T]
	s.NodeList.Range(func(key, value interface{}) bool {
		if key == nodeName {
			node = value.(*Node[T])
			return false
		}
		return true
	})
	if node == nil {
		return nil, errors.New("nodeName not found")
	}
	return node, nil
}

func (s *Service[T]) GetNodeSet() map[string]*Node[T] {
	var m = make(map[string]*Node[T])
	s.NodeList.Range(func(k, v interface{}) bool {
		m[k.(string)] = v.(*Node[T])
		return true
	})
	return m
}
