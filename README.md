# members

hashicorp memberlist extension

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
	group.Options.KnownPeers = ms
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
func (OrderHandler) MarshalBinary() ([]byte, error) {
	panic("implement me")
}

// Merge data from remote node MarshalBinary result. The Shard should be able to dedupe the data.
func (OrderHandler) Merge(b []byte) error {
	panic("implement me")
}

func (o OrderHandler) Receive(ord string) error {
	bs, err := msgpack.Marshal([]string{ord})
	if err != nil {
		return err
	}
	return o.Spreader.Broadcast(bs)
}

```

plz See [example](./example)

Notice: The Best Practice is to use array object and version control to sync the data.