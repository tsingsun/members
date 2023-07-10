package members

import (
	"github.com/hashicorp/memberlist"
	"github.com/vmihailenco/msgpack/v5"
	"go.uber.org/zap"
	"sync"
)

var _ memberlist.Delegate = (*delegate)(nil)

// delegate is an implementation of memberlist.Delegate.
//
// use queue to broadcast message,notice RetransmitMult control the number of send counts.
type delegate struct {
	broadcasts *memberlist.TransmitLimitedQueue
	peer       *Peer

	shards map[string]Shard
	mu     sync.RWMutex
}

func newDelegate(peer *Peer) *delegate {
	d := &delegate{
		peer:   peer,
		shards: make(map[string]Shard),
	}

	d.broadcasts = &memberlist.TransmitLimitedQueue{
		NumNodes:       peer.MemberCount,
		RetransmitMult: peer.membersConfig.RetransmitMult,
	}
	return d
}

func (d *delegate) AddShard(sd Shard) (Spreader, error) {
	d.mu.Lock()
	d.shards[sd.Name()] = sd
	d.mu.Unlock()

	if d.shards == nil {
		d.shards = make(map[string]Shard)
	}

	return NewChannel(sd.Name(), d.peer)
}

func (d *delegate) GetShard(name string) (Shard, bool) {
	d.mu.RLock()
	sd, ok := d.shards[name]
	d.mu.RUnlock()

	return sd, ok
}

func (d *delegate) RemoveShard(name string) error {
	d.mu.Lock()
	delete(d.shards, name)
	d.mu.Unlock()

	return nil
}

func (d *delegate) NodeMeta(limit int) []byte {
	return []byte{}
}

func (d *delegate) NotifyMsg(bytes []byte) {
	if len(bytes) == 0 {
		return
	}
	var pls []*Payload
	if err := msgpack.Unmarshal(bytes, &pls); err != nil {
		logger.Error("unmarshal payload error", zap.Error(err))
		return
	}
	for _, p := range pls {
		sd, ok := d.GetShard(p.Key)
		if !ok {
			return
		}
		if err := sd.Merge(p.Data); err != nil {
			logger.Error("merge shard data error", zap.Error(err))
			return
		}
	}
}

func (d *delegate) GetBroadcasts(overhead, limit int) [][]byte {
	return d.broadcasts.GetBroadcasts(overhead, limit)
}

func (d *delegate) LocalState(join bool) []byte {
	ps := make([]*Payload, 0, len(d.shards))
	for s, shard := range d.shards {
		b, err := shard.MarshalBinary()
		if err != nil {
			logger.Warn("LocalState: encode error", zap.Error(err), zap.String("shard", s))
			return nil
		}
		ps = append(ps, &Payload{Key: s, Data: b})
	}
	data, err := msgpack.Marshal(ps)
	if err != nil {
		logger.Warn("LocalState: encode error", zap.Error(err))
		return nil
	}
	return data
}

func (d *delegate) MergeRemoteState(buf []byte, join bool) {
	if len(buf) == 0 {
		return
	}
	ps := make([]*Payload, 0, len(d.shards))
	if err := msgpack.Unmarshal(buf, &ps); err != nil {
		logger.Warn("MergeRemoteState: decode error", zap.Error(err))
		return
	}
	for _, p := range ps {
		sd, ok := d.GetShard(p.Key)
		if !ok {
			continue
		}
		if err := sd.Merge(p.Data); err != nil {
			logger.Warn("MergeRemoteState: merge error", zap.Error(err))
			continue
		}
	}
}

type broadcast []byte

func (b broadcast) Invalidates(memberlist.Broadcast) bool {
	return false
}

func (b broadcast) Message() []byte {
	return b
}

func (b broadcast) Finished() {}
