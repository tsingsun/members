package members

import (
	"bytes"
	"errors"
	"github.com/hashicorp/memberlist"
	"github.com/vmihailenco/msgpack/v5"
	"go.uber.org/zap"
	"io"
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
		RetransmitMult: peer.MembersConfig.RetransmitMult,
	}
	return d
}

func (d *delegate) AddShard(sd Shard) (Spreader, error) {
	sdn := sd.Name()
	if sdn == "" {
		return nil, errors.New("shard name is empty")
	}
	d.mu.Lock()
	if _, ok := d.shards[sdn]; ok {
		d.mu.Unlock()
		return nil, errors.New("shard name is exist")
	}
	d.shards[sdn] = sd
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
	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	for s, shard := range d.shards {
		b, err := shard.MarshalBinary()
		if err != nil {
			logger.Warn("LocalState: encode error", zap.Error(err), zap.String("shard", s))
			return nil
		}
		if err = enc.Encode(&Payload{Key: s, Data: b}); err != nil {
			logger.Warn("LocalState: encode error", zap.Error(err))
			return nil
		}
	}
	return buf.Bytes()
}

func (d *delegate) MergeRemoteState(buf []byte, join bool) {
	if len(buf) == 0 {
		return
	}
	dec := msgpack.NewDecoder(bytes.NewReader(buf))
	for {
		var p Payload
		err := dec.Decode(&p)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			logger.Warn("MergeRemoteState: decode error", zap.Error(err))
			continue
		}
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
