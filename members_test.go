package members

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/suite"
	"github.com/tsingsun/woocoo/pkg/conf"
	"github.com/tsingsun/woocoo/pkg/log"
	"github.com/tsingsun/woocoo/testco/wctest"
	"github.com/vmihailenco/msgpack/v5"
	"sync"
	"testing"
	"time"
)

type order struct {
	OrderID   int
	OrderName string
	Attrs     map[string]*int
}

func newOrderHandler() *orderHandler {
	return &orderHandler{
		orders: make(map[int]*order),
	}
}

type orderHandler struct {
	Spreader
	orders map[int]*order
	mu     sync.RWMutex
	tag    string
}

func (o *orderHandler) Name() string {
	return "order"
}

func (o *orderHandler) MarshalBinary() ([]byte, error) {
	return msgpack.Marshal(o.orders)
}

func (o *orderHandler) Merge(b []byte) error {
	var ors []*order
	if err := msgpack.Unmarshal(b, &ors); err != nil {
		return err
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	for _, or := range ors {
		o.orders[or.OrderID] = or
		log.Info(fmt.Sprintf("%s merge order %v", o.tag, or))
	}
	return nil
}

func (o *orderHandler) Receive(ord *order) error {
	o.mu.Lock()
	o.orders[ord.OrderID] = ord
	o.mu.Unlock()
	bs, err := msgpack.Marshal([]*order{ord})
	if err != nil {
		return err
	}
	return o.Spreader.Broadcast(bs)
}

type testSuite struct {
	suite.Suite
	cnf        *conf.Configuration
	handlers   map[string]*orderHandler
	knownPeers []string
	peers      map[string]*Peer
}

func (t *testSuite) defaultCnf() {
	cnf := conf.NewFromStringMap(map[string]any{
		"memberList": map[string]any{
			"bindAddr":       "localhost",
			"bindPort":       0,
			"gossipInterval": "1m",
			"retransmitMult": 3,
		},
	})
	t.cnf = cnf
}

func TestTestSuite(t *testing.T) {
	suite.Run(t, new(testSuite))
}

func (t *testSuite) SetupSuite() {
	logger.Logger() // init logger
	t.handlers = make(map[string]*orderHandler)
	t.peers = make(map[string]*Peer)
	if t.cnf == nil {
		t.defaultCnf()
	}

	init := func(id string) {
		cnf := t.cnf.Copy()
		cnf.Parser().Set("options.id", id)
		group, err := NewPeer(WithConfiguration(cnf))
		t.Require().NoError(err)
		group.Options.KnownPeers = t.knownPeers
		orderhdl := newOrderHandler()
		orderhdl.tag = id
		err = group.Join(context.Background())
		t.Require().NoError(err)
		sd, err := group.AddShard(orderhdl)
		t.Require().NoError(err)
		orderhdl.Spreader = sd
		t.handlers[id] = orderhdl
		t.knownPeers = append(t.knownPeers, group.Address())
		t.peers[id] = group
	}
	// node 1
	init("node1")
	time.Sleep(time.Millisecond * 100)
	// node2
	init("node2")
	time.Sleep(time.Millisecond * 100)
	// node3
	init("node3")
	time.Sleep(time.Second)
}

func (t *testSuite) TearDownSuite() {
	for _, peer := range t.peers {
		_ = peer.Stop(context.Background())
	}
}

func (t *testSuite) TestBroadcast() {
	ord := &order{
		OrderID:   1,
		OrderName: "order1",
		Attrs: map[string]*int{
			"attr1": new(int),
			"attr2": new(int),
		},
	}
	handler := t.handlers["node1"]
	err := handler.Receive(ord)
	t.Require().NoError(err)
	_ = wctest.RunWait(t.T(), time.Second*2, func() error {
		return nil
	})
	t.Len(handler.orders, 1)
	handler2 := t.handlers["node2"]
	t.Len(handler2.orders, 1)
	handler3 := t.handlers["node3"]
	t.Len(handler3.orders, 1)
}
