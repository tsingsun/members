package members

import (
	"github.com/hashicorp/memberlist"
	"github.com/tsingsun/woocoo/pkg/conf"
	"time"
)

type Option func(*Options)

type Options struct {
	ID         string                   `yaml:"id" json:"id"`
	Cnf        *conf.Configuration      `yaml:"-" json:"-"`
	KnownPeers []string                 `yaml:"known" json:"known"`
	Event      memberlist.EventDelegate `yaml:"event" json:"event"`
	Delegate   memberlist.Delegate      `yaml:"delegate" json:"delegate"`
	JoinTTL    time.Duration            `yaml:"joinTTL" json:"joinTTL"`
	JoinRetry  int                      `yaml:"joinRetry" json:"joinRetry"`
}

func WithConfiguration(cnf *conf.Configuration) Option {
	return func(options *Options) {
		options.Cnf = cnf
	}
}
