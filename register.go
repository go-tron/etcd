package etcd

import (
	"context"
	"errors"
	"github.com/go-tron/config"
	"github.com/go-tron/logger"
	clientv3 "go.etcd.io/etcd/client/v3"
	"time"
)

type RegisterConfig struct {
	AppName      string
	Node         string
	Addr         string
	TTL          int64
	EtcdInstance *Client
	EtcdConfig   *Config
}

type RegisterOption func(*RegisterConfig)

func RegisterWithEtcdInstance(val *Client) RegisterOption {
	return func(conf *RegisterConfig) {
		conf.EtcdInstance = val
	}
}
func RegisterWithEtcdConfig(val *Config) RegisterOption {
	return func(conf *RegisterConfig) {
		conf.EtcdConfig = val
	}
}

func MustNewRegisterWithConfig(c *config.Config, opts ...RegisterOption) *Register {
	d, err := NewRegisterWithConfig(c, opts...)
	if err != nil {
		panic(err)
	}
	return d
}

func NewRegisterWithConfig(c *config.Config, opts ...RegisterOption) (*Register, error) {
	conf := &RegisterConfig{
		AppName: c.GetString("application.name"),
		Node:    c.GetString("cluster.podName"),
		Addr:    c.GetString("cluster.podIP"),
		TTL:     c.GetInt64("etcd.register.ttl"),
		EtcdConfig: &Config{
			Endpoints:   c.GetStringSlice("etcd.endpoints"),
			Username:    c.GetString("etcd.username"),
			Password:    c.GetString("etcd.password"),
			DialTimeout: c.GetDuration("etcd.dialTimeout"),
			Logger:      logger.NewZapWithConfig(c, "etcd", "error"),
		},
	}
	for _, apply := range opts {
		apply(conf)
	}
	return NewRegister(conf)
}

func MustNewRegister(conf *RegisterConfig, opts ...RegisterOption) *Register {
	d, err := NewRegister(conf, opts...)
	if err != nil {
		panic(err)
	}
	return d
}

func NewRegister(conf *RegisterConfig, opts ...RegisterOption) (*Register, error) {
	for _, apply := range opts {
		if apply != nil {
			apply(conf)
		}
	}
	if conf.AppName == "" {
		return nil, errors.New("AppName 必须设置")
	}
	if conf.Node == "" {
		return nil, errors.New("Node 必须设置")
	}
	if conf.Addr == "" {
		return nil, errors.New("Addr 必须设置")
	}
	if conf.EtcdInstance == nil {
		if conf.EtcdConfig == nil {
			return nil, errors.New("请设置etcd实例或者连接配置")
		}
		conf.EtcdInstance = New(conf.EtcdConfig)
	}

	if conf.TTL == 0 {
		conf.TTL = 15
	}

	sr := &Register{
		Client: conf.EtcdInstance,
		Name:   conf.AppName,
		Node:   conf.Node,
		Addr:   conf.Addr,
		TTL:    conf.TTL,
	}
	if err := sr.Register(); err == nil {
		return nil, err
	}
	return sr, nil
}

type Register struct {
	*Client
	Name      string
	Node      string
	Addr      string
	TTL       int64
	leaseID   clientv3.LeaseID
	leaseChan <-chan *clientv3.LeaseKeepAliveResponse
}

func (s *Register) Register() error {
	grant, err := s.Client.Grant(context.Background(), s.TTL)
	if err != nil {
		return err
	}
	_, err = s.Client.Client.Put(context.Background(), "/"+s.Name+"/"+s.Node, s.Addr, clientv3.WithLease(grant.ID))
	if err != nil {
		return err
	}
	leaseChan, err := s.Client.Client.KeepAlive(context.Background(), grant.ID)
	if err != nil {
		return err
	}
	s.leaseID = grant.ID
	s.leaseChan = leaseChan
	s.Watch()
	return nil
}

func (s *Register) Watch() {
	go func() {
		for _ = range s.leaseChan {
		}
		succeed := false
		for !succeed {
			if err := s.Register(); err == nil {
				succeed = true
			}
			time.Sleep(6 * time.Second)
		}
	}()
}

func (s *Register) Close() error {
	_, err := s.Client.Revoke(context.Background(), s.leaseID)
	return err
}
