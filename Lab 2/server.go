package kvsrv

import (
	"log"
	"sync"
)

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type ServerStatus struct {
	SN    int64
	Reply string
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	mp map[string]string

	lastRequest map[int64]*ServerStatus
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	key := args.Key
	if v, ok := kv.mp[key]; ok {
		reply.Value = v
	}
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	id := args.ID

	key, value := args.Key, args.Value

	if args.RepeatTimes != 0 {
		if lastRequest, ok := kv.lastRequest[id]; ok {
			if args.SN == lastRequest.SN {
				return
			}
		}
	}

	kv.mp[key] = value
	kv.lastRequest[id] = &ServerStatus{SN: args.SN}
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	id := args.ID

	key, value := args.Key, args.Value
	if args.RepeatTimes != 0 {
		if lastRequest, ok := kv.lastRequest[id]; ok {
			if args.SN == lastRequest.SN {
				reply.Value = lastRequest.Reply
				return
			}
		}
	}

	oldValue := kv.mp[key]
	kv.mp[key] += value
	reply.Value = oldValue

	kv.lastRequest[id] = &ServerStatus{args.SN, oldValue}
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.mp = make(map[string]string)
	kv.lastRequest = make(map[int64]*ServerStatus)

	return kv
}
