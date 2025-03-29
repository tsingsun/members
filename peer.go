package members

import (
	"context"
	"fmt"
	"github.com/hashicorp/memberlist"
	"github.com/tsingsun/woocoo/pkg/log"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"sync"
	"time"
)

var logger = log.Component("memberlist")

var defaultOption = Options{
	JoinTTL:   time.Second * 1,
	JoinRetry: 3,
}

// Shard is some specified distributed data handler.
type Shard interface {
	// Name returns the name of the shard, which is used to identify the shard.
	Name() string
	// MarshalBinary marshals the shard data into a binary to sync other nodes.
	MarshalBinary() ([]byte, error)
	// Merge data from remote node MarshalBinary result. The Shard should be able to dedupe the data.
	Merge(b []byte) error
}

// Peer is a memberlist Node wrapper
type Peer struct {
	Options
	members *memberlist.Memberlist

	shards map[string]Shard
	mu     sync.RWMutex

	msgc     chan []byte
	delegate *delegate
}

func NewPeer(opts ...Option) (*Peer, error) {
	options := defaultOption
	options.MembersConfig = memberlist.DefaultLANConfig()
	for _, o := range opts {
		o(&options)
	}
	p := &Peer{
		Options: options,
		msgc:    make(chan []byte, 100),
	}

	p.apply()
	ml, err := memberlist.Create(p.MembersConfig)
	if err != nil {
		return nil, err
	}
	p.members = ml

	return p, nil
}

func (p *Peer) apply() {
	var err error
	if p.cnf != nil {
		if p.cnf.IsSet("options") {
			if err = p.cnf.Sub("options").Unmarshal(&p.Options); err != nil {
				panic(err)
			}
		}
	}
	p.MembersConfig.LogOutput = logger.Logger().IOWriter(zapcore.DebugLevel)

	p.MembersConfig.Events = &event{}
	p.delegate = newDelegate(p)
	p.MembersConfig.Delegate = p.delegate
}

func (p *Peer) Address() string {
	return p.members.LocalNode().FullAddress().Addr
}

func (p *Peer) MemberCount() int {
	return p.members.NumMembers()
}

func (p *Peer) AddShard(sd Shard) (Spreader, error) {
	return p.delegate.AddShard(sd)
}

// Join other nodes, whatever exists peers not set.
func (p *Peer) Join(ctx context.Context) error {
	t := time.NewTicker(p.JoinTTL)
	count := 0
	for {
		select {
		case <-ctx.Done():
			if p.members != nil {
				return p.Stop(context.Background())
			}
			return ctx.Err()
		case <-t.C:
			count++
			if count > p.JoinRetry {
				return fmt.Errorf("join retry timeout")
			}
			_, err := p.members.Join(p.ExistsPeers)
			if err != nil {
				continue
			}
			return nil
		}
	}
}

func (p *Peer) Start(ctx context.Context) error {
	return p.ReliableMsgHandle(ctx)
}

func (p *Peer) Stop(ctx context.Context) error {
	if p.members != nil {
		p.members.Leave(p.JoinTTL)
		p.members.Shutdown()
	}
	close(p.msgc)
	return nil
}

func (p *Peer) OthersNodes() []*memberlist.Node {
	nodes := p.members.Members()
	for i, n := range p.members.Members() {
		if n.Name == p.members.LocalNode().Name {
			nodes = append(nodes[:i], nodes[i+1:]...)
			break
		}
	}
	return nodes
}

// SendReliable uses sending large message (see OversizedMessage) to other nodes. It calls memberlist.SendReliable.
func (p *Peer) SendReliable(b []byte) {
	p.msgc <- b
}

func (p *Peer) ReliableMsgHandle(ctx context.Context) error {
	var wg sync.WaitGroup
	for {
		select {
		case b, ok := <-p.msgc:
			if !ok {
				return nil
			}
			for _, node := range p.OthersNodes() {
				wg.Add(1)
				go func(n *memberlist.Node) {
					defer wg.Done()
					if err := p.members.SendReliable(n, b); err != nil {
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

// OversizedMessage indicates whether the byte payload should be sent
// via TCP.
func OversizedMessage(b []byte, size int) bool {
	return len(b) > size/2
}
