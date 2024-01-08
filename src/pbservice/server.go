package pbservice

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"6.824/viewservice"
)

type PBServer struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	me         string
	vs         *viewservice.Clerk

	activeView                       viewservice.View
	data                             map[string]string
	highestHandledRequestIDByClerkID map[int64]int64
	pr                               *Clerk // Primary will use this clerk to sync data and forward requests to the backup
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	if args.Viewnum != pb.activeView.Viewnum {
		reply.Err = ErrWrongViewnum
		return nil
	}

	if pb.me != pb.activeView.Primary {
		reply.Err = ErrWrongServer
		return nil
	}

	err := pb.mayForwardGet(args)
	if err != OK {
		reply.Err = err
		return nil
	}

	value, ok := pb.data[args.Key]
	if !ok {
		reply.Err = ErrNoKey
		return nil
	}

	reply.Value = value
	reply.Err = OK
	return nil
}

func (pb *PBServer) ForwardGet(args *GetArgs, reply *GetReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	if args.Viewnum != pb.activeView.Viewnum {
		reply.Err = ErrWrongViewnum
		return nil
	}

	if pb.me != pb.activeView.Backup {
		reply.Err = ErrWrongServer
		return nil
	}

	reply.Err = OK
	return nil
}

func (pb *PBServer) mayForwardGet(args *GetArgs) Err {
	if pb.pr == nil {
		return OK
	} else {
		reply := pb.pr.ForwardGet(args)
		return reply.Err
	}
}

func (pb *PBServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	if args.RequestID <= pb.highestHandledRequestIDByClerkID[args.ClerkID] {
		reply.Err = OK
		return nil
	}

	if args.Viewnum != pb.activeView.Viewnum {
		reply.Err = ErrWrongViewnum
		return nil
	}

	if pb.me != pb.activeView.Primary {
		reply.Err = ErrWrongServer
		return nil
	}

	err := pb.mayForwardPutAppend(args)
	if err != OK {
		reply.Err = err
		return nil
	}

	pb.doPutAppend(args)
	pb.highestHandledRequestIDByClerkID[args.ClerkID] = args.RequestID
	reply.Err = OK
	return nil
}

func (pb *PBServer) doPutAppend(args *PutAppendArgs) {
	switch args.Op {
	case Put:
		pb.data[args.Key] = args.Value
	case Append:
		oldValue, ok := pb.data[args.Key]
		if !ok {
			oldValue = ""
		}
		pb.data[args.Key] = oldValue + args.Value
	}
}

func (pb *PBServer) ForwardPutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	if args.RequestID <= pb.highestHandledRequestIDByClerkID[args.ClerkID] {
		reply.Err = OK
		return nil
	}

	if args.Viewnum != pb.activeView.Viewnum {
		reply.Err = ErrWrongViewnum
		return nil
	}

	if pb.me != pb.activeView.Backup {
		reply.Err = ErrWrongViewnum
		return nil
	}

	pb.doPutAppend(args)
	pb.highestHandledRequestIDByClerkID[args.ClerkID] = args.RequestID
	reply.Err = OK
	return nil
}

func (pb *PBServer) mayForwardPutAppend(args *PutAppendArgs) Err {
	if pb.pr == nil {
		return OK
	} else {
		reply := pb.pr.ForwardPutAppend(args)
		return reply.Err
	}
}

func (pb *PBServer) Sync(args *SyncArgs, reply *SyncReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	if args.Viewnum < pb.activeView.Viewnum {
		// outdated requests and should be ignored and not retried
		reply.Err = OK
		return nil
	}

	if args.Viewnum > pb.activeView.Viewnum {
		// more up-to-date requests and should be retried
		reply.Err = ErrWrongViewnum
		return nil
	}

	for k, v := range args.Data {
		pb.data[k] = v
	}
	for k, v := range args.HighestHandledRequestIDByClerkID {
		pb.highestHandledRequestIDByClerkID[k] = v
	}
	reply.Err = OK
	return nil
}

// ping the viewserver periodically.
// if view changed:
//
//	transition to new view.
//	manage transfer of state from primary to new backup.
func (pb *PBServer) tick() {
	pb.mu.Lock()
	ackedViewnum := pb.activeView.Viewnum
	vs := pb.vs
	pb.mu.Unlock()

	nextView, err := vs.Ping(ackedViewnum)
	if err != nil {
		return
	}

	if ackedViewnum < nextView.Viewnum {
		pb.mu.Lock()
		pb.activeView = nextView
		pb.mayBecomePrimary()
		pb.mayBecomeBackup()
		pb.mu.Unlock()
	}
}

func (pb *PBServer) mayBecomePrimary() {
	if pb.me == pb.activeView.Primary {
		if pb.activeView.Backup != "" {
			pb.pr = MakeInterServerClerk(pb.activeView.Backup)
			pb.syncData()
		}
	}
}

func (pb *PBServer) mayBecomeBackup() {
	if pb.me == pb.activeView.Backup {
		pb.pr = nil
	}
}

func (pb *PBServer) syncData() {
	data := make(map[string]string)
	for k, v := range pb.data {
		data[k] = v
	}
	pb.pr.Sync(pb.activeView.Viewnum, data)
}

// tell the server to shut itself down.
// please do not change these two functions.
func (pb *PBServer) kill() {
	atomic.StoreInt32(&pb.dead, 1)
	pb.l.Close()
}

// call this to find out if the server is dead.
func (pb *PBServer) isdead() bool {
	return atomic.LoadInt32(&pb.dead) != 0
}

// please do not change these two functions.
func (pb *PBServer) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&pb.unreliable, 1)
	} else {
		atomic.StoreInt32(&pb.unreliable, 0)
	}
}

func (pb *PBServer) isunreliable() bool {
	return atomic.LoadInt32(&pb.unreliable) != 0
}

func StartServer(vshost string, me string) *PBServer {
	pb := new(PBServer)
	pb.me = me
	pb.vs = viewservice.MakeClerk(me, vshost)
	pb.activeView = viewservice.View{Viewnum: 0}
	pb.data = make(map[string]string)
	pb.highestHandledRequestIDByClerkID = make(map[int64]int64)
	pb.pr = nil

	rpcs := rpc.NewServer()
	rpcs.Register(pb)

	os.Remove(pb.me)
	l, e := net.Listen("unix", pb.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	pb.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for !pb.isdead() {
			conn, err := pb.l.Accept()
			if err == nil && !pb.isdead() {
				if pb.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if pb.isunreliable() && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && !pb.isdead() {
				fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
				pb.kill()
			}
		}
	}()

	go func() {
		for !pb.isdead() {
			pb.tick()
			time.Sleep(viewservice.PingInterval)
		}
	}()

	return pb
}
