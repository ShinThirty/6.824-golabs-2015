package pbservice

const (
	OK              = "OK"
	ErrNoKey        = "ErrNoKey"
	ErrWrongServer  = "ErrWrongServer"
	ErrWrongViewnum = "ErrWrongViewnum"
	Put             = "Put"
	Append          = "Append"
)

type Err string
type Op string

// Put or Append
type PutAppendArgs struct {
	Viewnum   uint
	Op        Op
	Key       string
	Value     string
	ClerkID   int64
	RequestID int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Viewnum   uint
	Key       string
	ClerkID   int64
	RequestID int64
}

type GetReply struct {
	Err   Err
	Value string
}

type SyncArgs struct {
	Viewnum                          uint
	Data                             map[string]string
	HighestHandledRequestIDByClerkID map[int64]int64
}

type SyncReply struct {
	Err Err
}
