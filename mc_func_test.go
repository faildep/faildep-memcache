package memcache_test

import (
	"fmt"
	"github.com/Shopify/toxiproxy/client"
	"github.com/lysu/gomemcache/memcache"
	"github.com/lysu/memcache"
	"github.com/stretchr/testify/assert"
	"golang.org/x/net/context"
	"sync"
	"testing"
	"time"
)

var (
	mcProxy1 *toxiproxy.Proxy
	mcProxy2 *toxiproxy.Proxy
)

func init() {

	client := toxiproxy.NewClient("http://127.0.0.1:8474")

	mcProxy1, _ = client.Proxy("tcpmc")
	if mcProxy1 == nil {
		mcProxy1 = client.NewProxy(&toxiproxy.Proxy{
			Name:     "tcpmc",
			Listen:   "127.0.0.1:21211",
			Upstream: "127.0.0.1:11211",
			Enabled:  true,
		})
		err := mcProxy1.Create()
		if err != nil {
			panic(err)
		}
	}
	mcProxy2, _ = client.Proxy("tcpmc2")
	if mcProxy2 == nil {
		mcProxy2 = client.NewProxy(&toxiproxy.Proxy{
			Name:     "tcpmc2",
			Listen:   "127.0.0.1:31211",
			Upstream: "127.0.0.1:11211",
			Enabled:  true,
		})
		err := mcProxy2.Create()
		if err != nil {
			panic(err)
		}
	}
}

func TestRetry(t *testing.T) {

	assert := assert.New(t)

	mc, err := memcache.NewMemcached([]string{"127.0.0.1:21211", "127.0.0.1:31211"}, 1, 10*time.Millisecond,
		memcache.WithRetry(3, 2, 2*time.Millisecond, 100*time.Millisecond),
		memcache.WithBreaker(0, 50*time.Millisecond, 130*time.Millisecond))
	assert.NoError(err)

	_, err = mcProxy1.SetToxic("latency", "downstream", toxiproxy.Toxic{
		"enabled": true,
		"latency": 20,
	})
	assert.NoError(err)

	_, err = mcProxy2.SetToxic("latency", "downstream", toxiproxy.Toxic{
		"enabled": false,
	})
	assert.NoError(err)

	var wg sync.WaitGroup
	wg.Add(20)
	for i := 0; i < 20; i++ {
		go func() {
			err := mc.Set("xx", "yy", 2000)
			fmt.Println(err)
			wg.Done()
		}()
	}
	wg.Wait()

}

func TestSomeoneDown(t *testing.T) {

	ctx := context.TODO()

	assert := assert.New(t)

	mc, err := memcached.NewMemcached([]string{"127.0.0.1:31211", "127.0.0.1:41211"}, 1, 10*time.Millisecond,
		memcached.WithRetry(3, 2, 2*time.Millisecond, 100*time.Millisecond),
		memcached.WithBreaker(0, 50*time.Millisecond, 130*time.Millisecond))
	assert.NoError(err)

	_, err = mcProxy1.SetToxic("latency", "downstream", toxiproxy.Toxic{
		"enabled": true,
		"latency": 20,
	})
	assert.NoError(err)

	_, err = mcProxy2.SetToxic("latency", "downstream", toxiproxy.Toxic{
		"enabled": false,
	})
	assert.NoError(err)

	var wg sync.WaitGroup
	wg.Add(20)
	for i := 0; i < 20; i++ {
		go func() {
			err := mc.Set(ctx, "xx", "yy", 2000)
			fmt.Println(err)
			wg.Done()
		}()
	}
	wg.Wait()

}

func TestDownUp(t *testing.T) {

	ctx := context.TODO()

	assert := assert.New(t)

	mc, err := memcached.NewMemcached([]string{"127.0.0.1:21211"}, 1, 10*time.Millisecond,
		memcached.WithRetry(0, 0, 2*time.Millisecond, 100*time.Millisecond),
		memcached.WithBreaker(3, 50*time.Millisecond, 130*time.Millisecond))
	assert.NoError(err)

	_, err = mcProxy1.SetToxic("latency", "downstream", toxiproxy.Toxic{
		"enabled": true,
		"latency": 20,
	})
	assert.NoError(err)

	_, err = mcProxy2.SetToxic("latency", "downstream", toxiproxy.Toxic{
		"enabled": true,
		"latency": 20,
	})
	assert.NoError(err)

	for i := 0; i < 10; i++ {
		err = mc.Set(ctx, "t1", "abc", 2000)
		fmt.Println(err)
	}

	time.Sleep(50 * time.Millisecond)

	for j := 0; j < 4; j++ {
		err = mc.Set(ctx, "t1", "abd", 2000)
		fmt.Println(err)
	}

	_, err = mcProxy1.SetToxic("latency", "downstream", toxiproxy.Toxic{
		"enabled": false,
	})
	assert.NoError(err)

	_, err = mcProxy2.SetToxic("latency", "downstream", toxiproxy.Toxic{
		"enabled": false,
	})

	time.Sleep(70 * time.Millisecond)

	err = mc.Set(ctx, "t1", "abc", 2000)
	fmt.Println(err)
	err = mc.Set(ctx, "t1", "abc", 2000)
	fmt.Println(err)

	time.Sleep(31 * time.Millisecond)
	err = mc.Set(ctx, "t1", "abc", 2000)
	fmt.Println(err)
	err = mc.Set(ctx, "t1", "abc", 2000)
	fmt.Println(err)

}
