# members

[![Language](https://img.shields.io/badge/Language-Go-blue.svg)](https://golang.org/)
[![Go Report Card](https://goreportcard.com/badge/github.com/tsingsun/members)](https://goreportcard.com/report/github.com/tsingsun/members)
[![codecov](https://codecov.io/gh/tsingsun/members/branch/main/graph/badge.svg)](https://codecov.io/gh/tsingsun/members)
[![Build Status](https://github.com/tsingsun/members/actions/workflows/ci.yml/badge.svg)](https://github.com/tsingsun/members/actions)
[![Release](https://img.shields.io/github/release/tsingsun/members.svg?style=flat-square)](https://github.com/tsingsun/members/releases)
[![GoDoc](https://pkg.go.dev/badge/github.com/tsingsun/members?status.svg)](https://pkg.go.dev/github.com/tsingsun/members?tab=doc)

this project is base on hashicorp memberlist library.

## Usage

```bash
go get github.com/tsingsun/members
```

```go
package main

import (
	"context"
	"flag"
	"github.com/tsingsun/members"
	"github.com/vmihailenco/msgpack/v5"
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
	group, err := members.NewPeer()
	if err != nil {
		panic(err)
	}
	group.Options.ExistsPeers = ms
	if err = group.Join(context.Background()); err != nil {
		panic(err)
	}
	// OrderHandler implement members.Shard interface
	orderhdl := &OrderHandler{
		ShardId: "order",
	}
	sd, err := group.AddShard(orderhdl)
	if err != nil {
		panic(err)
	}
	orderhdl.Spreader = sd
}

type OrderHandler struct {
	ShardId  string
	Spreader members.Spreader
	Orders   []string
}

// Name returns the name of the shard, which is used to identify the shard.
func (OrderHandler) Name() string {
	return "order"
}

// MarshalBinary marshals the shard data into a binary to sync other nodes.
func (o *OrderHandler) MarshalBinary() ([]byte, error) {
	return msgpack.Marshal(o.Orders)
}

// Merge data from remote node MarshalBinary result. The Shard should be able to dedupe the data.
func (o *OrderHandler) Merge(b []byte) error {
	var ors []string
	if err := msgpack.Unmarshal(b, &ors); err != nil {
		return err
	}
	for _, ord := range ors {
		if !strings.Contains(ord, "order") {
			continue
        }
		o.Orders = append(o.Orders, ord)
	}
	return nil
}


func (o *OrderHandler) Receive(ord string) error {
	o.Orders = append(o.Orders, ord)
	bs, err := msgpack.Marshal([]string{ord})
	if err != nil {
		return err
	}
	return o.Spreader.Broadcast(bs)
}
