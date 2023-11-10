package viewservice

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type ViewServer struct {
	mu       sync.Mutex
	l        net.Listener
	dead     int32 // for testing
	rpccount int32 // for testing
	me       string

	lastPingTimeByServer map[string]time.Time
	lastAckedView        View
	nextView             View
}

// server Ping RPC handler.
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {
	vs.mu.Lock()
	defer vs.mu.Unlock()

	serverID := args.Me
	serverViewnum := args.Viewnum

	if vs.nextViewGenerated() {
		vs.mayAckNextView(serverID, serverViewnum)
	}
	vs.lastPingTimeByServer[serverID] = time.Now()

	if vs.lastAckedView.Viewnum == 0 {
		if !vs.nextViewGenerated() {
			vs.nextView = View{Viewnum: 1, Primary: serverID, Backup: ""}
		}
		reply.View = vs.nextView
	} else {
		if !vs.nextViewGenerated() {
			vs.mayRestartPrimary(serverID, serverViewnum)
			vs.mayRestartBackup(serverID, serverViewnum)
			vs.mayJoinAsBackup(serverID, serverViewnum)
		}
		reply.View = vs.nextView
	}

	return nil
}

func (vs *ViewServer) mayAckNextView(serverID string, serverViewnum uint) {
	if serverID == vs.nextView.Primary && serverViewnum >= vs.nextView.Viewnum {
		vs.lastAckedView = vs.nextView
	}
}

func (vs *ViewServer) nextViewGenerated() bool {
	return vs.nextView.Viewnum > vs.lastAckedView.Viewnum
}

func (vs *ViewServer) mayRestartPrimary(serverID string, serverViewnum uint) {
	if serverID == vs.lastAckedView.Primary && serverViewnum == 0 {
		vs.nextView = View{Viewnum: vs.lastAckedView.Viewnum + 1, Primary: vs.lastAckedView.Backup, Backup: vs.lastAckedView.Primary}
	}
}

func (vs *ViewServer) mayRestartBackup(serverID string, serverViewnum uint) {
	if serverID == vs.lastAckedView.Backup && serverViewnum == 0 {
		vs.nextView = View{Viewnum: vs.lastAckedView.Viewnum + 1, Primary: vs.lastAckedView.Primary, Backup: vs.lastAckedView.Backup}
	}
}

func (vs *ViewServer) mayJoinAsBackup(serverID string, serverViewnum uint) {
	if serverID != vs.lastAckedView.Primary && serverID != vs.lastAckedView.Backup {
		if vs.lastAckedView.Backup == "" {
			vs.nextView = View{Viewnum: vs.lastAckedView.Viewnum + 1, Primary: vs.lastAckedView.Primary, Backup: serverID}
		}
	}
}

// server Get() RPC handler.
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {
	vs.mu.Lock()
	defer vs.mu.Unlock()

	reply.View = vs.nextView

	return nil
}

// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
func (vs *ViewServer) tick() {
	vs.mu.Lock()
	defer vs.mu.Unlock()

	if vs.nextViewGenerated() {
		return
	}

	deadServers := make([]string, 0)
	for serverID, lastPingTime := range vs.lastPingTimeByServer {
		elapsed := time.Since(lastPingTime)
		if elapsed > DeadPings*PingInterval {
			deadServers = append(deadServers, serverID)
		}
	}

	for _, deadServerID := range deadServers {
		delete(vs.lastPingTimeByServer, deadServerID)
		vs.mayPromoteBackup(deadServerID)
		vs.mayRemoveBackup(deadServerID)
	}
}

func (vs *ViewServer) mayPromoteBackup(deadServerID string) {
	if deadServerID == vs.lastAckedView.Primary {
		vs.nextView = View{Viewnum: vs.lastAckedView.Viewnum + 1, Primary: vs.lastAckedView.Backup, Backup: ""}
	}
}

func (vs *ViewServer) mayRemoveBackup(deadServerID string) {
	if deadServerID == vs.lastAckedView.Backup {
		vs.nextView = View{Viewnum: vs.lastAckedView.Viewnum + 1, Primary: vs.lastAckedView.Primary, Backup: ""}
	}
}

// tell the server to shut itself down.
// for testing.
// please don't change these two functions.
func (vs *ViewServer) Kill() {
	atomic.StoreInt32(&vs.dead, 1)
	vs.l.Close()
}

// has this server been asked to shut down?
func (vs *ViewServer) isdead() bool {
	return atomic.LoadInt32(&vs.dead) != 0
}

// please don't change this function.
func (vs *ViewServer) GetRPCCount() int32 {
	return atomic.LoadInt32(&vs.rpccount)
}

func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me
	vs.lastPingTimeByServer = make(map[string]time.Time)
	vs.lastAckedView = View{Viewnum: 0}
	vs.nextView = View{Viewnum: 0}

	// tell net/rpc about our RPC server and handlers.
	rpcs := rpc.NewServer()
	rpcs.Register(vs)

	// prepare to receive connections from clients.
	// change "unix" to "tcp" to use over a network.
	os.Remove(vs.me) // only needed for "unix"
	l, e := net.Listen("unix", vs.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	vs.l = l

	// please don't change any of the following code,
	// or do anything to subvert it.

	// create a thread to accept RPC connections from clients.
	go func() {
		for !vs.isdead() {
			conn, err := vs.l.Accept()
			if err == nil && !vs.isdead() {
				atomic.AddInt32(&vs.rpccount, 1)
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && !vs.isdead() {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	// create a thread to call tick() periodically.
	go func() {
		for !vs.isdead() {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}
