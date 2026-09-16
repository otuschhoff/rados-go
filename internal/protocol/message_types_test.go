package protocol

import "testing"

func TestManagerMessageTypes(t *testing.T) {
	if MessageMgrMap != 0x704 {
		t.Fatalf("mgr map type = %#x", MessageMgrMap)
	}
	if MessageMgrCommand != 0x709 {
		t.Fatalf("mgr command type = %#x", MessageMgrCommand)
	}
	if MessageMgrCommandReply != 0x70a {
		t.Fatalf("mgr command reply type = %#x", MessageMgrCommandReply)
	}
}
