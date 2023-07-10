package members

import (
	"context"
	"github.com/vmihailenco/msgpack/v5"
)

// Spreader is an interface for transporting messages to other nodes in the cluster.
type Spreader interface {
	// Broadcast sends a message to all nodes in the cluster.
	Broadcast([]byte) error
}

var _ Spreader = (*Channel)(nil)

// Channel is a channel for communication between shard holding nodes.
type Channel struct {
	shardName string
	peer      *Peer

	msgc  chan []byte
	stopc chan struct{}
}

type Payload struct {
	Key  string
	Data []byte
}

// NewChannel creates a new channel for the given shard.
func NewChannel(shardName string, peer *Peer) (*Channel, error) {
	c := &Channel{
		shardName: shardName,
		peer:      peer,
		msgc:      make(chan []byte, 200),
	}
	return c, nil
}

// Broadcast sends a message to all nodes in the channel.
func (c *Channel) Broadcast(msg []byte) error {
	b, err := msgpack.Marshal([]*Payload{{Key: c.shardName, Data: msg}})
	if err != nil {
		return err
	}
	if OversizedMessage(b, c.peer.membersConfig.UDPBufferSize) {
		c.peer.SendReliable(b)
	} else {
		c.peer.delegate.broadcasts.QueueBroadcast(broadcast(b))
	}
	return nil
}

// Stop stops the channel.
func (c *Channel) Stop(ctx context.Context) error {
	return nil
}
