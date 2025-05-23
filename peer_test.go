package members

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/suite"
	"github.com/tsingsun/woocoo/pkg/conf"
	"github.com/vmihailenco/msgpack/v5"
	"strings"
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
	name   string
	orders map[int]*order
	mu     sync.RWMutex
	tag    string
}

func (o *orderHandler) Name() string {
	if o.name != "" {
		return o.name
	}
	return "order"
}

func (o *orderHandler) MarshalBinary() ([]byte, error) {
	o.mu.RLock()
	defer o.mu.RUnlock()
	ors := make([]*order, 0, len(o.orders))
	for _, ord := range o.orders {
		ors = append(ors, ord)
	}
	return msgpack.Marshal(ors)
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
		logger.Info(fmt.Sprintf("%s merge order %v", o.tag, or))
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

type exOrder struct {
	orderHandler
}

type testSuite struct {
	suite.Suite
	cnf        *conf.Configuration
	handlers   map[string]*orderHandler
	knownPeers []string
	peers      map[string]*Peer
	ctx        context.Context
	cancel     context.CancelFunc
}

func (t *testSuite) defaultCnf() {
	cnf := conf.NewFromStringMap(map[string]any{
		"options": map[string]any{
			"joinRetry": 1,
			"membersConfig": map[string]any{
				"bindAddr":       "127.0.0.1",
				"bindPort":       0,
				"gossipInterval": "1m",
				"retransmitMult": 3,
				"UDPBufferSize":  100,
			},
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
	t.ctx, t.cancel = context.WithCancel(context.Background())
	init := func(id string) {
		cnf := t.cnf.Copy()
		cnf.Parser().Set("options.membersConfig.name", id)
		group, err := NewPeer(WithConfiguration(cnf))
		t.Require().NoError(err)
		group.Options.ExistsPeers = t.knownPeers

		orderhdl := newOrderHandler()
		orderhdl.tag = id
		err = group.Join(context.Background())
		t.Require().NoError(err)
		sd, err := group.AddShard(orderhdl)
		t.Require().NoError(err)
		orderhdl.Spreader = sd
		t.handlers[id] = orderhdl

		exOrderhdl := &exOrder{}
		exOrderhdl.name = "exorder"
		exOrderhdl.orders = make(map[int]*order)
		exOrderhdl.orders = map[int]*order{
			1: {
				OrderID:   10,
				OrderName: "exorder1",
			},
			2: {
				OrderID:   20,
				OrderName: "exorder2",
			},
		}
		exOrderhdl.tag = id
		exOrderhdl.Spreader, err = group.AddShard(exOrderhdl)
		t.Require().NoError(err)

		t.knownPeers = append(t.knownPeers, group.Address())
		t.peers[id] = group
		go group.Start(t.ctx)
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
	t.Require().Len(t.peers["node1"].members.Members(), 3)
}

func (t *testSuite) TearDownSuite() {
	for _, peer := range t.peers {
		_ = peer.Stop(context.Background())
	}
	t.cancel()
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
	ord.OrderID = 2
	handler.Receive(ord)
	t.Require().NoError(err)

	select {
	case <-time.After(time.Second):

	}
	t.Len(handler.orders, 2)
	handler2 := t.handlers["node2"]
	t.Len(handler2.orders, 2)
	handler3 := t.handlers["node3"]
	t.Len(handler3.orders, 2)
}

func (t *testSuite) TestOversizedMessage() {
	// make a oversized message
	ord := &order{
		OrderID:   100000,
		OrderName: strings.Repeat("abc", 100),
		Attrs: map[string]*int{
			"attr1": new(int),
			"attr2": new(int),
		},
	}
	handler := t.handlers["node1"]
	err := handler.Receive(ord)
	t.Require().Equal(handler.orders[100000].OrderID, 100000)
	time.Sleep(time.Second)
	t.Require().Equal(t.handlers["node2"].orders[100000].OrderID, 100000)
	t.Require().Equal(t.handlers["node3"].orders[100000].OrderID, 100000)
	t.Require().NoError(err)
}
