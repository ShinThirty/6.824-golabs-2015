package pbservice

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"net/rpc"
	"sync"
	"time"

	"6.824/viewservice"
)

type Clerk struct {
	mu        sync.Mutex
	id        int64 // unique identifer of the clerk
	vs        *viewservice.Clerk
	view      viewservice.View
	nextReqID int64
	peer      string // Only used for inter-server communication
}

// this may come in handy.
func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(vshost string, me string) *Clerk {
	ck := new(Clerk)
	ck.id = nrand()
	ck.vs = viewservice.MakeClerk(me, vshost)
	ck.view = viewservice.View{Viewnum: 0}
	ck.nextReqID = 1
	return ck
}

func MakeInterServerClerk(peer string) *Clerk {
	ck := new(Clerk)
	ck.peer = peer
	return ck
}

// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the reply's contents are only valid if call() returned true.
//
// you should assume that call() will return an
// error after a while if the server is dead.
// don't provide your own time-out mechanism.
//
// please use call() to send all RPCs, in client.go and server.go.
// please don't change this function.
func call(srv string, rpcname string,
	args interface{}, reply interface{}) bool {
	c, errx := rpc.Dial("unix", srv)
	if errx != nil {
		return false
	}
	defer c.Close()

	err := c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

// fetch a key's value from the current primary;
// if they key has never been set, return "".
// Get() must keep trying until it either the
// primary replies with the value or the primary
// says the key doesn't exist (has never been Put().
func (ck *Clerk) Get(key string) string {
	if ck.view.Viewnum == 0 {
		ck.updateView()
	}
	ck.mu.Lock()
	args := GetArgs{Key: key, ClerkID: ck.id, RequestID: ck.nextReqID}
	ck.nextReqID += 1
	ck.mu.Unlock()

	for {
		args.Viewnum = ck.view.Viewnum
		var reply GetReply
		ok := call(ck.view.Primary, "PBServer.Get", &args, &reply)
		if ok {
			switch reply.Err {
			case OK, ErrNoKey:
				return reply.Value
			case ErrWrongServer, ErrWrongViewnum:
				ck.updateView()
			}
		} else {
			ck.updateView()
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// send a Put or Append RPC
func (ck *Clerk) PutAppend(key string, value string, op string) {
	if ck.view.Viewnum == 0 {
		ck.updateView()
	}
	ck.mu.Lock()
	args := PutAppendArgs{Op: Op(op), Key: key, Value: value, ClerkID: ck.id, RequestID: ck.nextReqID}
	ck.nextReqID += 1
	ck.mu.Unlock()

	for {
		args.Viewnum = ck.view.Viewnum
		var reply PutAppendReply
		ok := call(ck.view.Primary, "PBServer.PutAppend", &args, &reply)
		if ok {
			switch reply.Err {
			case OK:
				return
			case ErrWrongServer, ErrWrongViewnum:
				ck.updateView()
			}
		} else {
			ck.updateView()
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// primary forward putappend requests to backup
func (ck *Clerk) ForwardGet(args *GetArgs) *GetReply {
	var reply GetReply

	ok := call(ck.peer, "PBServer.ForwardGet", &args, &reply)
	if !ok {
		reply.Err = ErrWrongViewnum
	}

	return &reply
}

// primary forward putappend requests to backup
func (ck *Clerk) ForwardPutAppend(args *PutAppendArgs) *PutAppendReply {
	var reply PutAppendReply

	ok := call(ck.peer, "PBServer.ForwardPutAppend", &args, &reply)
	if !ok {
		reply.Err = ErrWrongViewnum
	}

	return &reply
}

// tell the primary to update key's value.
// must keep trying until it succeeds.
func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

// tell the primary to append to key's value.
// must keep trying until it succeeds.
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

// send the entire database to another peer, keeps trying until succeeded
func (ck *Clerk) Sync(viewnum uint, data map[string]string) {
	args := SyncArgs{Viewnum: viewnum, Data: data}

	for {
		var reply SyncReply
		ok := call(ck.peer, "PBServer.Sync", &args, &reply)
		if ok && reply.Err == OK {
			break
		} else {
			time.Sleep(100 * time.Millisecond)
		}
	}
}

func (ck *Clerk) updateView() {
	for {
		view, ok := ck.vs.Get()
		if ok {
			ck.view = view
			return
		} else {
			time.Sleep(100 * time.Millisecond)
		}
	}
}
