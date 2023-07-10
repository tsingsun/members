package members

import "github.com/hashicorp/memberlist"

var _ memberlist.EventDelegate = (*event)(nil)

type event struct {
}

func (e event) NotifyJoin(node *memberlist.Node) {

}

func (e event) NotifyLeave(node *memberlist.Node) {

}

func (e event) NotifyUpdate(node *memberlist.Node) {

}
