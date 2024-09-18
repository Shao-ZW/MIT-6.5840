package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}


type KVServer struct {
	mu 				sync.Mutex
	kvmap 			map[string]string
	appendrecords	map[int]map[int]string
	// Your definitions here.
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	reply.Value = kv.kvmap[args.Key]
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.kvmap[args.Key] = args.Value
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	for k := range kv.appendrecords[args.Clientid] {
		if k != args.Id {
			delete(kv.appendrecords[args.Clientid], k)
		}
	}
	
	if _, ok := kv.appendrecords[args.Clientid]; !ok {
		reply.Value = kv.kvmap[args.Key]
		kv.appendrecords[args.Clientid] = make(map[int]string)
		kv.appendrecords[args.Clientid][args.Id] = reply.Value
		kv.kvmap[args.Key] = kv.kvmap[args.Key] + args.Value
	} else {
		if value, ok := kv.appendrecords[args.Clientid][args.Id]; !ok {
			reply.Value = kv.kvmap[args.Key]
			kv.appendrecords[args.Clientid][args.Id] = reply.Value
			kv.kvmap[args.Key] = kv.kvmap[args.Key] + args.Value
		} else {
			reply.Value = value
		}
	}

}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	kv.kvmap = make(map[string]string)
	kv.appendrecords = make(map[int]map[int]string)
	// You may need initialization code here.

	return kv
}
