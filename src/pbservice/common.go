package pbservice

const (
	OK              = "OK"
	ErrNoKey        = "ErrNoKey"
	ErrWrongServer  = "ErrWrongServer"
	ErrWrongViewnum = "ErrWrongViewnum"
	ErrWrongVersion = "ErrWrongVersion"
	Put             = "Put"
	Append          = "Append"
)

type Err string
type Op string

type Value struct {
	Version int64 // version number that is incremented everytime the value is changed
	Value   string
}

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

type ForwardPutAppendArgs struct {
	Args           PutAppendArgs
	PrimaryVersion int64 // current value version in the primary to bound the version difference between primary and backup to 1
}

type ForwardPutAppendReply struct {
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
	Data                             map[string]Value
	HighestHandledRequestIDByClerkID map[int64]int64
}

type SyncReply struct {
	Err Err
}
