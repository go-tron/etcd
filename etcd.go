package etcd

import (
	"context"
	"github.com/go-tron/config"
	"github.com/go-tron/logger"
	clientv3 "go.etcd.io/etcd/client/v3"
	"time"
)

type Config struct {
	Endpoints   []string      `json:"endpoints"`
	Username    string        `json:"username"`
	Password    string        `json:"password"`
	DialTimeout time.Duration `json:"dialTimeout"`
	Logger      logger.Logger
}

func NewWithConfig(c *config.Config) *Client {
	return New(&Config{
		Endpoints:   c.GetStringSlice("etcd.endpoints"),
		Username:    c.GetString("etcd.username"),
		Password:    c.GetString("etcd.password"),
		DialTimeout: c.GetDuration("etcd.dialTimeout"),
		Logger:      logger.NewZapWithConfig(c, "etcd", "error"),
	})
}

func New(config *Config) *Client {
	if config == nil {
		panic("config 必须设置")
	}
	if config.Endpoints == nil {
		panic("Endpoints 必须设置")
	}
	if config.Username == "" {
		panic("Username 必须设置")
	}
	if config.Password == "" {
		panic("Password 必须设置")
	}
	if config.DialTimeout == 0 {
		config.DialTimeout = time.Second * 5
	}

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   config.Endpoints,
		Username:    config.Username,
		Password:    config.Password,
		DialTimeout: config.DialTimeout,
	})
	if err != nil {
		panic(err)
	}
	return &Client{client}
}

type KeyValue struct {
	Key   string
	Value string
}

type PutConfig struct {
	TTL int64
}
type PutOption func(*PutConfig)

func WithTTL(val int64) PutOption {
	return func(conf *PutConfig) {
		conf.TTL = val
	}
}

type GetConfig struct {
	Prefix bool
}
type GetOption func(*GetConfig)

func WithPrefix() GetOption {
	return func(conf *GetConfig) {
		conf.Prefix = true
	}
}

type Client struct {
	*clientv3.Client
}

func (c *Client) Put(key, val string, opts ...PutOption) error {
	conf := &PutConfig{}
	for _, apply := range opts {
		apply(conf)
	}
	var opOptions []clientv3.OpOption
	if conf.TTL != 0 {
		grant, err := c.Grant(context.Background(), conf.TTL)
		if err != nil {
			return err
		}
		opOptions = append(opOptions, clientv3.WithLease(grant.ID))
	}
	_, err := c.Client.Put(context.Background(), key, val, opOptions...)
	return err
}

func (c *Client) Get(key string, opts ...GetOption) ([]KeyValue, error) {
	conf := &GetConfig{}
	for _, apply := range opts {
		apply(conf)
	}
	var opOptions []clientv3.OpOption
	if conf.Prefix {
		opOptions = append(opOptions, clientv3.WithPrefix())
	}

	resp, err := c.Client.Get(context.Background(), key, opOptions...)
	if err != nil {
		return nil, err
	}
	if len(resp.Kvs) == 0 {
		return nil, err
	}
	var kvs []KeyValue
	for _, kv := range resp.Kvs {
		kvs = append(kvs, KeyValue{Key: string(kv.Key), Value: string(kv.Value)})
	}
	return kvs, nil
}
