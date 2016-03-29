package memcache

import (
	"fmt"
	"github.com/lysu/gomemcache/memcache"
	"time"
)

type mcPool struct {
	clients map[string]*memcache.Client
}

func newMCPool(servers []string, maxIdleConnsPerAddr int, timeout time.Duration) (*mcPool, error) {
	cs := make(map[string]*memcache.Client, len(servers))
	for _, srv := range servers {
		ss := new(memcache.ServerList)
		err := ss.SetServers(srv)
		if err != nil {
			return nil, err
		}
		c := memcache.NewFromSelector(ss, memcache.SetMaxIdleConnsPerAddr(maxIdleConnsPerAddr))
		c.Timeout = timeout
		cs[srv] = c
	}
	return &mcPool{clients: cs}, nil
}

func (p *mcPool) get(server string) (*memcache.Client, error) {
	c, ok := p.clients[server]
	if !ok {
		return nil, fmt.Errorf("Mc Server %s not found", server)
	}
	return c, nil
}
