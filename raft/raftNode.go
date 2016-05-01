package raft
import "time"
import "github.com/cs733-iitb/cluster"
import "github.com/cs733-iitb/cluster/mock"
import "github.com/cs733-iitb/log"
import "reflect"
import "encoding/gob"
import "math/rand"
import "strconv"

type RaftNode struct{
	id int
	ElectionTimeout int
	HeartbeatTimeout int
	LogDir string
	eventCh chan interface{}
	timeoutCh chan interface{}
	commitCh chan interface{}
	ShutDownCh chan interface{}
	t *time.Timer
	server *mock.MockServer
	Sm *StateMachine
}
type shutDown struct{}
type rConfig struct{
	ElectionTimeout int
	HeartbeatTimeout int
	Id int // this node's id. One of the cluster's entries should match.
	LogDir string // Log file directory for this node
}

//methods for running raftNodes
func (rn *RaftNode) processEvents() {
	gob.Register(TimeoutEv{})
	gob.Register(VoteReqEv{})
	gob.Register(VoteRespEv{})
	gob.Register(AppendEntriesReqEv{})
	gob.Register(AppendEntriesRespEv{})
	gob.Register(alarm{})
	gob.Register(AppendEv{})
	gob.Register(LogStoreEv{})
	gob.Register(CommitEv{})
	for {
		select {	
		case <- rn.timeoutCh:
			tev := TimeoutEv{}
			actions := rn.Sm.ProcessEvent(tev)
			rn.doActions(actions)	
		case aev := <- rn.eventCh:
			actions := rn.Sm.ProcessEvent(aev)
			rn.doActions(actions)	
		case ev := <-rn.server.Inbox():
			actions := rn.Sm.ProcessEvent(ev.Msg)
			rn.doActions(actions)		
		case <- rn.ShutDownCh:
			rn.Sm.State = "follower"
			rn.server.Close()
			rn.t.Stop()
			return
		}
	}
}
func (rn *RaftNode) doActions(actions []interface{}){
	for i,v := range actions{
		Evtype := reflect.TypeOf(v).Name()
		switch(Evtype){
		case "Send":
			vv:=v.(Send)
			rn.server.Outbox() <- &cluster.Envelope{Pid: vv.peerId, MsgId: int64(i), Msg: vv.event}

		case "alarm":
			vv:=v.(alarm)
			rn.t.Stop()
			rn.t = time.AfterFunc(time.Duration(vv.sec) * time.Millisecond, func(){
			rn.t.Stop()
			rn.timeoutCh <- TimeoutEv{}
			})

		case "AppendEv":

		case "CommitEv":
			rn.commitCh <- v
		}	
	}
}


// making raftnodes : init the raft node layer

func makeRafts() ([]*RaftNode, *mock.MockCluster, error){
	
	clconfig := cluster.Config{Peers:[]cluster.PeerConfig{
		{Id:1}, {Id:2}, {Id:3}, {Id:4}, {Id:5},
	}}
	cluster, err := mock.NewCluster(clconfig)
	if err != nil {return nil, nil, err}
	nodes := make([]*RaftNode, len(clconfig.Peers))
	raftConfig := rConfig{
		ElectionTimeout: 1000, // milliseconds
		HeartbeatTimeout: 200,
	}

	for id := 1; id <= 5; id++ {
		raftNode := New(id, raftConfig, cluster) 
		nodes[id-1] = raftNode
	}

	return nodes, cluster, nil
}



// Create or Initialize a raft node, and give the corresponding "Server" object from the
	// cluster to help it communicate with the others.

func New(myid int, config rConfig, cluster *mock.MockCluster) (raftNode *RaftNode) {
	raftNode = new(RaftNode)
	raftNode.id = myid
	rand.Seed( time.Now().UTC().UnixNano() * int64(myid))
	raftNode.ElectionTimeout = rand.Intn(500)+config.ElectionTimeout
	raftNode.HeartbeatTimeout = config.HeartbeatTimeout
	raftNode.eventCh = make(chan interface{}, 100000)
	raftNode.timeoutCh = make(chan interface{}, 100000)
	raftNode.commitCh = make(chan interface{}, 100000)
	raftNode.ShutDownCh = make(chan interface{}, 100000)
	raftNode.t = time.AfterFunc(time.Duration(raftNode.ElectionTimeout) * time.Millisecond, func(){})
	raftNode.LogDir = "mylog" + strconv.Itoa(myid)
	var Sm *StateMachine
	raftNode.server = cluster.Servers[myid]
		pid := cluster.Servers[myid].Pid()
		peer := cluster.Servers[myid].Peers()
		lg := mkLog(raftNode.LogDir)
		ni := []int{0, 0, 0, 0, 0, 0}
		mi := []int{-1, -1, -1, -1, -1, -1}
		vr := []int{0, 0, 0, 0, 0, 0}
		nres := make([]int, 10000, 10000)
		Sm = &StateMachine{id: pid, peers: peer, State: "follower", 
							Term: 0,
							log: lg,
							votedFor: 0,
							commitIndex: -1,
							lastApplied: -1,
							LastLogTerm: 0,
							LastLogIndex: -1, 
							nextIndex: ni,
							matchIndex: mi,
							majority: 2,
							numVotesGranted: 0, numVotesDenied: 0,
							votesRcvd: vr, // one per peer
							noAppendRes: nres,
							ElectionTimeout: raftNode.ElectionTimeout, 
							HeartbeatTimeout: config.HeartbeatTimeout}	
		raftNode.Sm = Sm
	go func(){
		raftNode.processEvents()
	}()
	return raftNode
}
var rnodes []*RaftNode
var clust *mock.MockCluster


//RaftNode Interface methods

func Start() ([]*RaftNode, *mock.MockCluster){
	rnodes,clust,_ = makeRafts()
	rnodes[0].timeoutCh <- TimeoutEv{}
	return rnodes,clust
}

func (rn *RaftNode) Append(adata []byte) {
	rn.eventCh <- AppendEv{Data: adata}
}

func LeaderId(rafts []*RaftNode) (int){
	for _,rn := range rafts{
		if rn.Sm.State == "leader"{
			return rn.id
		}
	}
	return -1
}

func GetLeaderId() (int){
	return LeaderId(rnodes)
}

func (rn *RaftNode) Id() (int){
	return rn.id 
}

func (rn *RaftNode) CommitChannel() (chan interface{}){
	return rn.commitCh
}

func (rn *RaftNode) CommittedIndex() (int){
	return rn.Sm.commitIndex
}

func (rn *RaftNode) Shutdown(){
	rn.ShutDownCh <- shutDown{}
}

func KillLeader(){
	ldr := LeaderId(rnodes)
	rnodes[ldr-1].Shutdown()
}

func mkLog(logdir string) (*log.Log) {
	lg, _ := log.Open(logdir)
	return lg
}