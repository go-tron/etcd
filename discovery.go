package etcd

import (
	"context"
	"errors"
	"github.com/go-tron/config"
	"github.com/go-tron/logger"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"strings"
	"sync"
)

type Discovery struct {
	*Client
	Name       string
	list       sync.Map
	addChan    chan map[string]string
	removeChan chan string
}

type DiscoveryConfig struct {
	AppName      string
	EtcdInstance *Client
	EtcdConfig   *Config
}
type DiscoveryOption func(*DiscoveryConfig)

func DiscoveryWithEtcdInstance(val *Client) DiscoveryOption {
	return func(conf *DiscoveryConfig) {
		conf.EtcdInstance = val
	}
}
func DiscoveryWithEtcdConfig(val *Config) DiscoveryOption {
	return func(conf *DiscoveryConfig) {
		conf.EtcdConfig = val
	}
}

func MustNewDiscoveryWithConfig(c *config.Config, opts ...DiscoveryOption) *Discovery {
	d, err := NewDiscoveryWithConfig(c, opts...)
	if err != nil {
		panic(err)
	}
	return d
}

func NewDiscoveryWithConfig(c *config.Config, opts ...DiscoveryOption) (*Discovery, error) {
	conf := &DiscoveryConfig{
		AppName: c.GetString("application.name"),
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
	return NewDiscovery(conf)
}

func MustNewDiscovery(conf *DiscoveryConfig, opts ...DiscoveryOption) *Discovery {
	d, err := NewDiscovery(conf, opts...)
	if err != nil {
		panic(err)
	}
	return d
}

func NewDiscovery(conf *DiscoveryConfig, opts ...DiscoveryOption) (*Discovery, error) {
	for _, apply := range opts {
		if apply != nil {
			apply(conf)
		}
	}
	if conf.AppName == "" {
		return nil, errors.New("AppName 必须设置")
	}
	if conf.EtcdInstance == nil {
		if conf.EtcdConfig == nil {
			return nil, errors.New("请设置etcd实例或者连接配置")
		}
		conf.EtcdInstance = New(conf.EtcdConfig)
	}

	d := &Discovery{
		Name:       conf.AppName,
		Client:     conf.EtcdInstance,
		list:       sync.Map{},
		addChan:    make(chan map[string]string),
		removeChan: make(chan string),
	}

	resp, err := d.Client.Client.Get(context.Background(), "/"+d.Name, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	for _, ev := range resp.Kvs {
		d.add(string(ev.Key), string(ev.Value))
	}
	go d.watcher()
	return d, nil
}

func (d *Discovery) watcher() {
	watchChan := d.Client.Watch(context.Background(), "/"+d.Name, clientv3.WithPrefix())
	for v := range watchChan {
		for _, ev := range v.Events {
			switch ev.Type {
			case mvccpb.PUT: //修改或者新增
				d.add(string(ev.Kv.Key), string(ev.Kv.Value))
			case mvccpb.DELETE: //删除
				d.remove(string(ev.Kv.Key))
			}
		}
	}
}

func (d *Discovery) AddSubscribe() chan map[string]string {
	return d.addChan
}
func (d *Discovery) RemoveSubscribe() chan string {
	return d.removeChan
}

func (d *Discovery) add(key, value string) {
	k := strings.Replace(key, "/"+d.Name+"/", "", 1)
	d.list.Store(k, value)
	go func() {
		d.addChan <- map[string]string{k: value}
	}()
}

func (d *Discovery) remove(key string) {
	k := strings.Replace(key, "/"+d.Name+"/", "", 1)
	d.list.Delete(k)
	go func() {
		d.removeChan <- k
	}()
}

func (d *Discovery) GetList() map[string]string {
	m := make(map[string]string)
	d.list.Range(func(k, v interface{}) bool {
		m[k.(string)] = v.(string)
		return true
	})
	return m
}
