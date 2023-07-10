package members

import (
	"context"
	"github.com/hashicorp/memberlist"
	"github.com/vmihailenco/msgpack/v5"
	"go.uber.org/zap"
	"sync"
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
	go c.Start(peer.ctx)
	return c, nil
}

// Broadcast sends a message to all nodes in the channel.
func (c *Channel) Broadcast(msg []byte) error {
	b, err := msgpack.Marshal([]*Payload{{Key: c.shardName, Data: msg}})
	if err != nil {
		return err
	}
	if OversizedMessage(b, c.peer.membersConfig.UDPBufferSize) {
		select {
		case c.msgc <- b:
		default:
		}
	} else {
		c.peer.delegate.broadcasts.QueueBroadcast(broadcast(b))
	}
	return nil
}

// OversizedMessage indicates whether or not the byte payload should be sent
// via TCP.
func OversizedMessage(b []byte, size int) bool {
	return len(b) > size/2
}

// Start listen message channel which prevents memberlist from opening too many parallel
// TCP connections to its peers.
func (c *Channel) Start(ctx context.Context) error {
	var wg sync.WaitGroup
	for {
		select {
		case b, ok := <-c.msgc:
			if !ok {
				return nil
			}
			for _, node := range c.peer.OthersNodes() {
				wg.Add(1)
				go func(n *memberlist.Node) {
					defer wg.Done()
					if err := c.peer.members.SendReliable(n, b); err != nil {
						logger.Error("channel broadcast error", zap.Error(err))
						return
					}
				}(node)
			}

			wg.Wait()
		case <-ctx.Done():
			return nil
		}
	}
}

// Stop stops the channel.
func (c *Channel) Stop(ctx context.Context) error {
	return nil
}
