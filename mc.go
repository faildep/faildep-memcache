// Package memcache provides a client for the memcached cache server.
// with Load balancing & Fault tolerance & ConnectionPool support
package memcache

import (
	"fmt"
	"github.com/lysu/gomemcache/memcache"
	"github.com/lysu/slb"
	"golang.org/x/net/context"
	"log"
	"strconv"
	"time"
)

const (
	stringFlag = iota
	boolFlag
	intFlag
	uintFlag
	int8Flag
	uint8Flag
	int16Flag
	uint16Flag
	int32Flag
	uint32Flag
	int64Flag
	uint64Flag
	byteArrayFlag
)

var (
	Err_Type_Not_Support = fmt.Errorf("Not Support type while encoding")
)

// Memcached presents simple fault tolerance memcached client.
type Memcached struct {
	pool *mcPool
	lb   *slb.LoadBalancer
}

type mcConf struct {
	maxIdlePerSever         int
	successiveFailThreshold uint
	trippedBaseTime         time.Duration
	trippedTimeMax          time.Duration
	trippedBackOff          slb.BackOff
	retryMaxServerPick      uint
	retryMaxRetryPerServer  uint
	retryBaseInterval       time.Duration
	retryMaxInterval        time.Duration
	retryBackOff            slb.BackOff
	rwTimeout               time.Duration
}

const (
	DefaultSuccessiveFailThreshold = 10
	DefaultTrippedBaseTime         = 20 * time.Millisecond
	DefaultTrippedTimeMax          = 200 * time.Millisecond
	DefaultRetryMaxServerPick      = 3
	DefaultRetryMaxRetryPerServer  = 0
	DefaultRetryBaseInterval       = 50 * time.Millisecond
	DefaultRetryMaxInterval        = 250 * time.Millisecond
)

// WithBreak config mc failure break params.
func WithBreaker(successiveFailThreshold uint, trippedBaseTime, trippedTimeMax time.Duration) func(o *mcConf) {
	return func(conf *mcConf) {
		conf.successiveFailThreshold = successiveFailThreshold
		conf.trippedBaseTime = trippedBaseTime
		conf.trippedTimeMax = trippedTimeMax
	}
}

// WithRetry config mc retry params.
func WithRetry(retryMaxServerPick, retryMaxPerServer uint, retryBaseInterval, retryMaxInterval time.Duration) func(o *mcConf) {
	return func(conf *mcConf) {
		conf.retryMaxServerPick = retryMaxServerPick
		conf.retryMaxRetryPerServer = retryMaxPerServer
		conf.retryBaseInterval = retryBaseInterval
		conf.retryMaxInterval = retryMaxInterval
	}
}

// NewMemcached creates memcached with given serverList and max idle connections per server which will hold in pool.
func NewMemcached(servers []string, maxIdlePerHost int, rwTimeout time.Duration, opts ...func(o *mcConf)) (*Memcached, error) {
	conf := mcConf{
		maxIdlePerSever:         maxIdlePerHost,
		successiveFailThreshold: DefaultSuccessiveFailThreshold,
		trippedBaseTime:         DefaultTrippedBaseTime,
		trippedTimeMax:          DefaultTrippedTimeMax,
		trippedBackOff:          slb.Exponential,
		retryMaxServerPick:      DefaultRetryMaxServerPick,
		retryMaxRetryPerServer:  DefaultRetryMaxRetryPerServer,
		retryBaseInterval:       DefaultRetryBaseInterval,
		retryMaxInterval:        DefaultRetryMaxInterval,
		retryBackOff:            slb.DecorrelatedJittered,
		rwTimeout:               rwTimeout,
	}
	for _, opt := range opts {
		opt(&conf)
	}
	pool, err := newMCPool(servers, conf.maxIdlePerSever, conf.rwTimeout)
	if err != nil {
		return nil, err
	}
	return &Memcached{
		pool: pool,
		lb: slb.NewLoadBalancer(servers,
			slb.WithCircuitBreaker(conf.successiveFailThreshold, conf.trippedBaseTime, conf.trippedTimeMax, conf.trippedBackOff),
			slb.WithRetry(conf.retryMaxServerPick, conf.retryMaxRetryPerServer, conf.retryBaseInterval, conf.retryMaxInterval, conf.retryBackOff),
		),
	}, nil
}

// Set writes the given item, unconditionally.
func (mc *Memcached) Set(key string, value interface{}, expire int32) error {
	start := time.Now()
	err := mc.lb.Submit(func(node *slb.Node) error {
		item, err := encodeMemcacheItem(value)
		if err != nil {
			log.Printf("[MC] Encode item Server: %s ERROR: %v", node.Server, err)
			return err
		}
		item.Key = key
		item.Expiration = expire
		client, err := mc.pool.get(node.Server)
		if err != nil {
			return err
		}
		err = client.Set(item)
		if err != nil {
			log.Printf("[MC] Set %s %v Server: %s ERORR: %v", key, value, node.Server, err)
		}
		return err
	})
	timeUsed := time.Now().Sub(start).Nanoseconds() / int64(time.Millisecond)
	if timeUsed > 0 {
		log.Printf("[MC] Get MC [SET] use %d", timeUsed)
	}
	return err
}

// Get gets the item for the given key. ErrCacheMiss is returned for a
// memcache cache miss. The key must be at most 250 bytes in length.
func (mc *Memcached) Get(key string) (interface{}, error) {
	start := time.Now()
	var result interface{}
	err := mc.lb.Submit(func(node *slb.Node) error {
		client, err := mc.pool.get(node.Server)
		if err != nil {
			log.Printf("[MC] Get conn Server: %s ERROR: %v", node.Server, err)
			return err
		}
		item, err := client.Get(key)
		if err != nil {
			if err != memcache.ErrCacheMiss {
				log.Printf("[MC] Get %v Server: %s ERORR: %v", key, node.Server, err)
			}
			return err
		}
		value, err := decodeMemcacheItem(item)
		if err != nil {
			return err
		}
		result = value
		return err
	})
	timeUsed := time.Now().Sub(start).Nanoseconds() / int64(time.Millisecond)
	if timeUsed > 0 {
		log.Printf("[MC] Get Get [GET] use %d", timeUsed)
	}
	if err != nil {
		return nil, err
	}
	return result, nil
}

// GetMulti is a batch version of Get. The returned map from keys to
// items may have fewer elements than the input slice, due to memcache
// cache misses. Each key must be at most 250 bytes in length.
// If no error is returned, the returned map will also be non-nil.
func (mc *Memcached) GetMulti(keys []string) (map[string]interface{}, error) {
	start := time.Now()
	result := make(map[string]interface{}, len(keys))
	err := mc.lb.Submit(func(node *slb.Node) error {
		client, err := mc.pool.get(node.Server)
		if err != nil {
			log.Printf("[MC] Get conn Server: %s ERROR: %v", node.Server, err)
			return err
		}
		items, err := client.GetMulti(keys)
		if err != nil {
			log.Printf("[MC] GetMulti %v Server: %s ERORR: %v", keys, node.Server, err)
			return err
		}
		for key, item := range items {
			value, err := decodeMemcacheItem(item)
			if err != nil {
				return err
			}
			result[key] = value
		}
		return nil
	})
	timeUsed := time.Now().Sub(start).Nanoseconds() / int64(time.Millisecond)
	if timeUsed > 0 {
		log.Printf("[MC] Get MC [MultGet] use %d", timeUsed)
	}
	if err != nil {
		return nil, err
	}
	return result, nil
}

// Delete deletes the item with the provided key. The error ErrCacheMiss is
// returned if the item didn't already exist in the cache.
func (mc *Memcached) Delete(key string) error {
	start := time.Now()
	err := mc.lb.Submit(func(node *slb.Node) error {
		client, err := mc.pool.get(node.Server)
		if err != nil {
			log.Printf("[MC] Get conn Server: %s ERROR: %v", node.Server, err)
			return err
		}
		err = client.Delete(key)
		if err != nil && err != memcache.ErrCacheMiss {
			log.Printf("[MC] Delete %s Server: %s ERORR: %v", key, node.Server, err)
		}
		return err
	})
	timeUsed := time.Now().Sub(start).Nanoseconds() / int64(time.Millisecond)
	if timeUsed > 0 {
		log.Printf("[MC] Get MC [DELETE] use %d", timeUsed)
	}
	return err
}

// Add writes the given item, if no value already exists for its
// key. ErrNotStored is returned if that condition is not met.
func (mc *Memcached) Add(ctx context.Context, key string, value interface{}, expire int32) error {
	start := time.Now()
	err := mc.lb.Submit(func(node *slb.Node) error {
		client, err := mc.pool.get(node.Server)
		if err != nil {
			log.Printf("[MC] Get conn Server: %s ERROR: %v", node.Server, err)
			return err
		}
		item, err := encodeMemcacheItem(value)
		if err != nil {
			if err != memcache.ErrNotStored {
				log.Printf("[MC] Encode item Server: %s ERROR: %v", node.Server, err)
			}
			return err
		}
		item.Key = key
		item.Expiration = expire
		err = client.Add(item)
		if err != nil {
			log.Printf("[MC] Add %s %v Server: %s ERORR: %v", key, value, node.Server, err)
		}
		return err

	})
	timeUsed := time.Now().Sub(start).Nanoseconds() / int64(time.Millisecond)
	if timeUsed > 0 {
		log.Printf("[MC] Get MC [ADD] use %d", timeUsed)
	}
	return err
}

// Increment atomically increments key by delta. The return value is
// the new value after being incremented or an error. If the value
// didn't exist in memcached the error is ErrCacheMiss. The value in
// memcached must be an decimal number, or an error will be returned.
// On 64-bit overflow, the new value wraps around.
func (mc *Memcached) Increment(key string, delta uint64) (uint64, error) {
	start := time.Now()
	var result uint64
	err := mc.lb.Submit(func(node *slb.Node) error {
		client, err := mc.pool.get(node.Server)
		if err != nil {
			log.Printf("[MC] Get conn Server: %s ERROR: %v", node.Server, err)
			return err
		}
		value, err := client.Increment(key, delta)
		if err != nil {
			if err != memcache.ErrCacheMiss {
				log.Printf("[MC] Increment %s %d Server: %s ERORR: %v", key, delta, node.Server, err)
			}
			return err
		}
		result = value
		return nil
	})
	timeUsed := time.Now().Sub(start).Nanoseconds() / int64(time.Millisecond)
	if timeUsed > 0 {
		log.Printf("[MC] Get MC [INCREMENT] use %d", timeUsed)
	}
	if err != nil {
		return result, err
	}
	return result, nil
}

func decodeMemcacheItem(i *memcache.Item) (v interface{}, err error) {
	switch i.Flags {
	case boolFlag:
		v, err = strconv.ParseBool(string(i.Value))
	case intFlag:
		var v1 int64
		v1, err = strconv.ParseInt(string(i.Value), 10, strconv.IntSize)
		v = int(v1)
	case uintFlag:
		var v1 uint64
		v1, err = strconv.ParseUint(string(i.Value), 10, strconv.IntSize)
		v = uint(v1)
	case int8Flag:
		var v1 int64
		v1, err = strconv.ParseInt(string(i.Value), 10, 8)
		v = int8(v1)
	case uint8Flag:
		v = i.Value[0]
	case int16Flag:
		var v1 int64
		v1, err = strconv.ParseInt(string(i.Value), 10, 16)
		v = int16(v1)
	case uint16Flag:
		var v1 uint64
		v1, err = strconv.ParseUint(string(i.Value), 10, 16)
		v = uint16(v1)
	case int32Flag:
		var v1 int64
		v1, err = strconv.ParseInt(string(i.Value), 10, 32)
		v = int32(v1)
	case uint32Flag:
		var v1 uint64
		v1, err = strconv.ParseUint(string(i.Value), 10, 32)
		v = uint32(v1)
	case int64Flag:
		v, err = strconv.ParseInt(string(i.Value), 10, 64)
	case uint64Flag:
		v, err = strconv.ParseUint(string(i.Value), 10, 64)
	case stringFlag:
		v = string(i.Value)
	case byteArrayFlag:
		v = i.Value
	default:
		err = Err_Type_Not_Support
	}
	return
}

func encodeMemcacheItem(v interface{}) (i *memcache.Item, err error) {
	i = new(memcache.Item)
	switch t := v.(type) {
	case bool:
		i.Value = []byte(strconv.FormatBool(t))
		i.Flags = boolFlag
	case int:
		i.Value = []byte(strconv.FormatInt(int64(t), 10))
		i.Flags = intFlag
	case uint:
		i.Value = []byte(strconv.FormatUint(uint64(t), 10))
		i.Flags = uintFlag
	case int8:
		i.Value = []byte(strconv.FormatInt(int64(t), 10))
		i.Flags = int8Flag
	case byte:
		i.Value = []byte{t}
		i.Flags = uint8Flag
	case int16:
		i.Value = []byte(strconv.FormatInt(int64(t), 10))
		i.Flags = int16Flag
	case uint16:
		i.Value = []byte(strconv.FormatUint(uint64(t), 10))
		i.Flags = uint16Flag
	case int32:
		i.Value = []byte(strconv.FormatInt(int64(t), 10))
		i.Flags = int32Flag
	case uint32:
		i.Value = []byte(strconv.FormatUint(uint64(t), 10))
		i.Flags = uint32Flag
	case int64:
		i.Value = []byte(strconv.FormatInt(int64(t), 10))
		i.Flags = int64Flag
	case uint64:
		i.Value = []byte(strconv.FormatUint(uint64(t), 10))
		i.Flags = uint64Flag
	case string:
		i.Value = []byte(t)
		i.Flags = stringFlag
	case []byte:
		i.Value = t
		i.Flags = byteArrayFlag
	default:
		i = nil
		err = Err_Type_Not_Support
	}
	return
}
