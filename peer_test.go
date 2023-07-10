package members

import (
	"context"
	"testing"
	"time"
)

func Test_lookupIPAddr(t *testing.T) {
	type args struct {
		ctx  context.Context
		host string
		ipv6 bool
	}
	tests := []struct {
		name     string
		args     args
		wantAddr string
		wantErr  bool
	}{
		{
			name: "ipv4",
			args: args{
				ctx:  context.Background(),
				host: "localhost:8080",
			},
			wantAddr: "127.0.0.1:8080",
			wantErr:  false,
		},
		{
			name: "timeout",
			args: args{
				host: "localhostxx:8080",
			},
			wantAddr: "",
			wantErr:  true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.args.ctx == nil {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
				defer cancel()
				tt.args.ctx = ctx
			}
			gotAddr, err := lookupIPAddr(tt.args.host, tt.args.ipv6)
			if (err != nil) != tt.wantErr {
				t.Errorf("lookupIPAddr() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotAddr != tt.wantAddr {
				t.Errorf("lookupIPAddr() gotAddr = %v, want %v", gotAddr, tt.wantAddr)
			}
		})
	}
}
