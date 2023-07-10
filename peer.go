package members

import (
	"context"
	"errors"
	"fmt"
	"github.com/hashicorp/memberlist"
	"github.com/tsingsun/woocoo/pkg/log"
	"go.uber.org/zap/zapcore"
	"math/rand"
	"net"
	"os"
	"strconv"
	"sync"
	"time"
)

var logger = log.Component("members")

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
	members       *memberlist.Memberlist
	membersConfig *memberlist.Config

	shards map[string]Shard
	mu     sync.RWMutex

	delegate *delegate

	ctx context.Context
}

func NewPeer(opts ...Option) (*Peer, error) {
	options := defaultOption
	for _, o := range opts {
		o(&options)
	}
	p := &Peer{
		Options:       options,
		membersConfig: memberlist.DefaultLocalConfig(),
	}

	p.apply()
	ml, err := memberlist.Create(p.membersConfig)
	if err != nil {
		return nil, err
	}
	p.members = ml

	return p, nil
}

func (p *Peer) apply() {
	var err error
	if p.Cnf != nil {
		if p.Cnf.IsSet("options") {
			if err = p.Cnf.Sub("options").Unmarshal(&p.Options); err != nil {
				panic(err)
			}
		}
		if p.Cnf.IsSet("memberList") {
			if err = p.Cnf.Sub("memberList").Unmarshal(p.membersConfig); err != nil {
				panic(err)
			}
		}
	}
	if p.Options.ID != "" {
		p.membersConfig.Name = p.Options.ID
	} else {
		hostname, _ := os.Hostname()
		p.membersConfig.Name = hostname + "-" + strconv.Itoa(rand.Intn(1000))
	}
	if p.membersConfig.BindAddr != "" {
		p.membersConfig.BindAddr, err = lookupIPAddr(p.membersConfig.BindAddr, false)
		if err != nil {
			panic(err)
		}
	}
	if len(p.Options.KnownPeers) > 0 {
		rps := make([]string, 0, len(p.Options.KnownPeers))
		for _, peer := range p.Options.KnownPeers {
			ad, err := lookupIPAddr(peer, false)
			if err != nil {
				continue
			}
			rps = append(rps, ad)
		}
		p.Options.KnownPeers = rps
	}

	p.membersConfig.LogOutput = logger.Logger().IOWriter(zapcore.DebugLevel)

	p.membersConfig.Events = &event{}
	p.delegate = newDelegate(p)
	p.membersConfig.Delegate = p.delegate
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

func (p *Peer) Join(ctx context.Context) error {
	p.ctx = ctx
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
			_, err := p.members.Join(p.KnownPeers)
			if err != nil {
				continue
			}
			return nil
		}
	}
}

func (p *Peer) Stop(ctx context.Context) error {
	p.members.Leave(0)
	p.members.Shutdown()
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

func lookupIPAddr(host string, ipv6 bool) (addr string, err error) {
	host, port, err := net.SplitHostPort(host)
	if err != nil {
		var ae *net.AddrError
		if errors.As(err, &ae) {
			host = ae.Addr
		} else {
			return
		}
	}

	ips, err := net.LookupIP(host)
	if err != nil {
		return
	}
	for _, ip := range ips {
		if ipv6 && ip.To16() != nil {
			addr = ip.String()
			break
		} else if ip.To4() != nil {
			addr = ip.String()
			break
		}
	}
	if port != "" {
		addr = net.JoinHostPort(addr, port)
	}
	return
}
