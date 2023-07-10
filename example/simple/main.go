package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/tsingsun/members"
	"github.com/tsingsun/woocoo/pkg/conf"
	"github.com/tsingsun/woocoo/pkg/log"
	"github.com/vmihailenco/msgpack/v5"
	"math/rand"
	"net/http"
	"strconv"
	"strings"
)

var (
	peers   = flag.String("peers", "", "comma seperated list of peers")
	address = flag.String("address", ":4001", "http host:port")
)

func init() {
	flag.Parse()
}

func main() {
	var ms []string
	if len(*peers) > 0 {
		ms = strings.Split(*peers, ",")
	}

	cnf := conf.NewFromStringMap(map[string]any{
		"memberList": map[string]any{
			"bindPort":       0,
			"gossipInterval": "1m",
		},
	})
	group, err := members.NewPeer(members.WithConfiguration(cnf))
	if err != nil {
		panic(err)
	}
	log.Printf("Local node %s\n", group.Address())
	group.Options.KnownPeers = ms
	if err = group.Join(context.Background()); err != nil {
		panic(err)
	}

	orderhdl := newOrderHandler(strconv.Itoa(rand.Intn(100)))
	sd, err := group.AddShard(orderhdl)
	if err != nil {
		panic(err)
	}
	orderhdl.Spreader = sd

	http.HandleFunc("/order", func(w http.ResponseWriter, r *http.Request) {
		order := &order{
			OrderID:   1,
			OrderName: "order1",
			Attrs: map[string]*int{
				"attr1": new(int),
				"attr2": new(int),
			},
		}
		if err := orderhdl.Receive(order); err != nil {
			panic(err)
		}
		w.Write([]byte("ok"))
	})
	if err := http.ListenAndServe(*address, nil); err != nil {
		panic(err)
	}
}

type order struct {
	OrderID   int
	OrderName string
	Attrs     map[string]*int
}

func newOrderHandler(tag string) *orderHandler {
	return &orderHandler{
		tag:    tag,
		orders: make(map[int]*order),
	}
}

type orderHandler struct {
	members.Spreader
	orders map[int]*order
	tag    string
}

func (o orderHandler) Name() string {
	return "order"
}

func (o orderHandler) MarshalBinary() ([]byte, error) {
	ors := make([]*order, 0, len(o.orders))
	for _, ord := range o.orders {
		ors = append(ors, ord)
	}
	return msgpack.Marshal(ors)
}

func (o orderHandler) Merge(b []byte) error {
	var ors []*order
	if err := msgpack.Unmarshal(b, &ors); err != nil {
		return err
	}
	for _, ord := range ors {
		o.orders[ord.OrderID] = ord
		log.Info(fmt.Sprintf("%s merge order %v", o.tag, ord))
	}
	return nil
}

func (o orderHandler) Receive(ord *order) error {
	bs, err := msgpack.Marshal([]*order{ord})
	if err != nil {
		return err
	}
	return o.Spreader.Broadcast(bs)
}
