package etcd

import (
	"errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"math/rand"
	"sync"
	"time"
)

type GRPCServiceConfig[T any] struct {
	AppName          string
	CreateClientFunc func(grpc.ClientConnInterface) T
	EtcdConfig       *Config
	EtcdInstance     *Client
}
type GRPCServiceOption[T any] func(*GRPCServiceConfig[T])

func GRPCServiceWithAppName[T any](val string) GRPCServiceOption[T] {
	return func(conf *GRPCServiceConfig[T]) {
		conf.AppName = val
	}
}
func GRPCServiceWithCreateClientFunc[T any](val func(grpc.ClientConnInterface) T) GRPCServiceOption[T] {
	return func(conf *GRPCServiceConfig[T]) {
		conf.CreateClientFunc = val
	}
}
func GRPCServiceWithEtcdConfig[T any](val *Config) GRPCServiceOption[T] {
	return func(conf *GRPCServiceConfig[T]) {
		conf.EtcdConfig = val
	}
}
func GRPCServiceWithEtcdInstance[T any](val *Client) GRPCServiceOption[T] {
	return func(conf *GRPCServiceConfig[T]) {
		conf.EtcdInstance = val
	}
}

func NewGRPCService[T any](config *GRPCServiceConfig[T], opts ...GRPCServiceOption[T]) *GRPCService[T] {
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

	s := &GRPCService[T]{
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
				node, err := NewGRPCNode[T](addr, config.CreateClientFunc)
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

func NewGRPCNode[T any](addr string, createClientFunc func(grpc.ClientConnInterface) T) (client *Node[T], err error) {
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

type GRPCService[T any] struct {
	*Discovery
	NodeList *sync.Map
}

func (s *GRPCService[T]) FindNode(nodeName string) (*Node[T], error) {
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

func (s *GRPCService[T]) GetNodeSet() map[string]*Node[T] {
	var m = make(map[string]*Node[T])
	s.NodeList.Range(func(k, v interface{}) bool {
		m[k.(string)] = v.(*Node[T])
		return true
	})
	return m
}

func (s *GRPCService[T]) FindRandNodeName() (string, error) {
	m := s.Discovery.GetList()
	servers := make([]string, 0, len(m))
	for k := range m {
		servers = append(servers, k)
	}
	if len(servers) == 0 {
		return "", errors.New("randNodeName not found")
	}
	i := rand.Intn(len(servers))
	return servers[i], nil
}

func (s *GRPCService[T]) FindRandNode() (*Node[T], error) {
	name, err := s.FindRandNodeName()
	if err != nil {
		return nil, err
	}
	node, err := s.FindNode(name)
	if err != nil {
		return nil, err
	}
	return node, nil
}
