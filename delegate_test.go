package members

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_delegate_LocalState(t *testing.T) {
	logger.Logger()
	d := &delegate{
		peer:   nil,
		shards: make(map[string]Shard),
	}
	d.shards["test"] = &orderHandler{
		orders: map[int]*order{
			1: {
				OrderID: 1,
			},
			2: {
				OrderID: 2,
			},
		},
	}
	d.shards["test2"] = &orderHandler{
		orders: map[int]*order{
			1: {
				OrderID: 3,
			},
			2: {
				OrderID: 4,
			},
		},
	}
	bs := d.LocalState(false)
	assert.NotEqual(t, len(bs), 0)

	d2 := &delegate{
		peer: nil,
		shards: map[string]Shard{
			"test":  newOrderHandler(),
			"test2": newOrderHandler(),
		},
	}
	d2.MergeRemoteState(bs, false)
	assert.Equal(t, len(d2.shards), 2)
	assert.Equal(t, len(d2.shards["test"].(*orderHandler).orders), 2)
	assert.Equal(t, len(d2.shards["test2"].(*orderHandler).orders), 2)
	// order id check
	assert.Equal(t, d2.shards["test"].(*orderHandler).orders[1].OrderID, 1)
	assert.Equal(t, d2.shards["test"].(*orderHandler).orders[2].OrderID, 2)
	assert.Equal(t, d2.shards["test2"].(*orderHandler).orders[3].OrderID, 3)
	assert.Equal(t, d2.shards["test2"].(*orderHandler).orders[4].OrderID, 4)
}

func Test_delegate_LocalState_Empty(t *testing.T) {
	d := &delegate{
		peer:   nil,
		shards: make(map[string]Shard),
	}
	bs := d.LocalState(false)
	assert.Equal(t, len(bs), 0)
}

func Test_delegate_MergeInvalidData(t *testing.T) {
	d2 := &delegate{
		peer: nil,
		shards: map[string]Shard{
			"test":  newOrderHandler(),
			"test2": newOrderHandler(),
		},
	}
	invalidData := []byte{0x00, 0x01, 0x02}
	d2.MergeRemoteState(invalidData, false)
	assert.Equal(t, len(d2.shards["test"].(*orderHandler).orders), 0)
	assert.Equal(t, len(d2.shards["test2"].(*orderHandler).orders), 0)
}
