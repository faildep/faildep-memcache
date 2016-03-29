package memcache_test

import (
	"github.com/lysu/gomemcache/memcache"
	"github.com/lysu/memcache"
	"github.com/stretchr/testify/assert"
	"golang.org/x/net/context"
	"testing"
	"time"
)

func TestMC(t *testing.T) {

	ctx := context.TODO()

	assert := assert.New(t)

	mc, err := memcache.NewMemcached([]string{"0.0.0.0:11211", "10.10.10.114:11211"}, 2, 40*time.Millisecond)
	assert.NoError(err)

	for i := 0; i < 10; i++ {
		err := mc.Set("u1", "abc", 2000)
		assert.NoError(err)
	}

	v, err := mc.Get("u1")
	assert.NoError(err)
	assert.Equal("abc", v)

	mc2, err := memcached.NewMemcached([]string{"0.0.0.0:11211"}, 2, 40*time.Millisecond)
	assert.NoError(err)
	v2, err := mc2.Get(ctx, "u1")
	assert.NoError(err)
	assert.Equal("abc", v2)

	mc3, err := memcached.NewMemcached([]string{"10.10.10.114:11211"}, 2, 40*time.Millisecond)
	assert.NoError(err)
	v3, err := mc3.Get(ctx, "u1")
	assert.NoError(err)
	assert.Equal("abc", v3)

}
