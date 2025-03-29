package members

import (
	"github.com/hashicorp/memberlist"
	"github.com/tsingsun/woocoo/pkg/conf"
	"time"
)

type Option func(*Options)

type Options struct {
	// ExistsPeers is the list of known peers to join
	ExistsPeers []string      `yaml:"existsPeers" json:"existsPeers"`
	JoinTTL     time.Duration `yaml:"joinTTL" json:"joinTTL"`
	JoinRetry   int           `yaml:"joinRetry" json:"joinRetry"`
	// MembersConfig is the memberlist.Config for the peer.
	// note that: bind address should be ip address, not hostname.
	MembersConfig *memberlist.Config `yaml:"membersConfig" json:"membersConfig"`

	cnf      *conf.Configuration
	Event    memberlist.EventDelegate `yaml:"-" json:"-"`
	Delegate memberlist.Delegate      `yaml:"-" json:"-"`
}

func WithConfiguration(cnf *conf.Configuration) Option {
	return func(options *Options) {
		options.cnf = cnf
	}
}
