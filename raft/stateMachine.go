package raft

import "math"
import "github.com/cs733-iitb/log"
import "encoding/json"

type LogEntries struct{
		Term int
		Data []byte
	}
type VoteReqEv struct {
	CandidateId int
	Term int
	LastLogIndex int
	LastLogTerm int
	From int
}
type VoteRespEv struct {
	Term int
	voteGranted bool
	From int
}
type TimeoutEv struct {
}
type alarm struct{
	sec int
}
type AppendEv struct{
	Data []byte
}
type CommitEv struct{
	index int
	Data LogEntries
	Err error
}
type Send struct{
	peerId int
	event interface{}
}
type AppendEntriesReqEv struct {
	Term int
	LeaderId int
	prevLogIndex int
	prevLogTerm int
	entries LogEntries
	leaderCommit int
	From int
	// etc
}
type AppendEntriesRespEv struct {
	Term int
	lastLogTerm int
	success bool
	From int
	// etc
}
type LogStoreEv struct{
	index int
	entry LogEntries
	From int
}

type StateMachine struct {
	id int // server id
	peers []int // other server ids
	Term int 
	votedFor int
	log *log.Log
	commitIndex int
	lastApplied int
	LastLogTerm int
	LastLogIndex int
	nextIndex []int
	matchIndex []int
	majority int
	State string
	numVotesGranted, numVotesDenied int
	votesRcvd []int // one per peer
	noAppendRes []int
	ElectionTimeout int
	HeartbeatTimeout int
	LeaderId int
}



func (sm *StateMachine) ProcessEvent (ev interface{}) []interface{}{
	var actions []interface{}
	switch ev.(type) {

	case AppendEntriesReqEv:
		cmd := ev.(AppendEntriesReqEv)
		if sm.State == "follower"{
			if cmd.entries.Data != nil{
				if cmd.prevLogIndex != -1{
					var entrie LogEntries
					e,_ := sm.log.Get(int64(cmd.prevLogIndex))
					json.Unmarshal(e,&entrie)
					if cmd.Term < sm.Term || entrie.Term != cmd.prevLogTerm{
						actions = append(actions, Send{cmd.From, AppendEntriesRespEv{Term: sm.Term, success: false, From: sm.id}})
					}else{
						if sm.LastLogIndex > cmd.prevLogIndex{
							sm.log.TruncateToEnd(int64(cmd.prevLogIndex+1))
						}
						d, _ := json.Marshal(cmd.entries)
						sm.log.Append(d)
						sm.LastLogIndex = int(sm.log.GetLastIndex())
						sm.nextIndex[sm.id]++
						sm.matchIndex[sm.id]++

						if cmd.leaderCommit > sm.commitIndex{
							sm.commitIndex = int(math.Min(float64(cmd.leaderCommit), float64(cmd.prevLogIndex+1)))
							var entrie LogEntries
							e,_ := sm.log.Get(int64(sm.commitIndex))
							json.Unmarshal(e,&entrie)
							actions = append(actions, CommitEv{sm.commitIndex, entrie, nil})
						}
						if cmd.Term > sm.Term{
							sm.Term = cmd.Term
							sm.votedFor = 0
						}
						actions = append(actions, Send{cmd.From, AppendEntriesRespEv{Term: sm.Term, lastLogTerm: cmd.entries.Term,  success: true, From: sm.id}})
						actions = append(actions, alarm{sec: sm.ElectionTimeout})
					}
				}else{
					if cmd.Term < sm.Term {
						actions = append(actions, Send{cmd.From, AppendEntriesRespEv{Term: sm.Term, success: false, From: sm.id}})
					}else{
						if sm.LastLogIndex > cmd.prevLogIndex{
							sm.log.TruncateToEnd(int64(cmd.prevLogIndex+1))
						}
						d, _ := json.Marshal(cmd.entries)
						sm.log.Append(d)
						sm.LastLogIndex = int(sm.log.GetLastIndex())
						sm.nextIndex[sm.id]++
						sm.matchIndex[sm.id]++

						if cmd.leaderCommit > sm.commitIndex{
							sm.commitIndex = int(math.Min(float64(cmd.leaderCommit), float64(cmd.prevLogIndex+1)))
							var entrie LogEntries
							e,_ := sm.log.Get(int64(sm.commitIndex))
							json.Unmarshal(e,&entrie)
							actions = append(actions, CommitEv{sm.commitIndex, entrie, nil})
						}
						if cmd.Term > sm.Term{
							sm.Term = cmd.Term
							sm.votedFor = 0
						}
						actions = append(actions, Send{cmd.From, AppendEntriesRespEv{Term: sm.Term, lastLogTerm: cmd.entries.Term, success: true, From: sm.id}})
						actions = append(actions, alarm{sec: sm.ElectionTimeout})
					}
				}
			}else{
				actions = append(actions, alarm{sec: sm.ElectionTimeout})
				if cmd.leaderCommit > sm.commitIndex{
					for i:=sm.commitIndex+1;i<=cmd.leaderCommit;i++{
						var entrie LogEntries
						e,_ := sm.log.Get(int64(i))
						json.Unmarshal(e,&entrie)
						actions = append(actions, CommitEv{i, entrie, nil})

					}		
					sm.commitIndex = cmd.leaderCommit
			    }
			    if sm.LeaderId != cmd.LeaderId{
			    	sm.LeaderId = cmd.LeaderId
			    }
			}
		}

		if sm.State == "candidate"{
			sm.State = "follower"
			sm.Term = cmd.Term
			sm.votedFor = 0
			actions = append(actions, Send{cmd.From, AppendEntriesRespEv{Term: sm.Term, success: true, From: sm.id}})
			actions = append(actions, alarm{sec: sm.ElectionTimeout})
		}
		


	case AppendEntriesRespEv:
		cmd := ev.(AppendEntriesRespEv)
		if sm.State == "leader"{
			if cmd.success == true{
				var ent LogEntries
				et,_ := sm.log.Get(int64(sm.LastLogIndex))
				json.Unmarshal(et,&ent)
				if sm.nextIndex[cmd.From] == sm.LastLogIndex || cmd.lastLogTerm == ent.Term{
					sm.noAppendRes[sm.nextIndex[cmd.From]]++
					sm.matchIndex[cmd.From] = sm.nextIndex[cmd.From]
					sm.nextIndex[cmd.From] = sm.nextIndex[cmd.From] + 1
					sm.majority = 2
					if sm.matchIndex[cmd.From] > sm.commitIndex{
						if sm.noAppendRes[sm.matchIndex[cmd.From]] >= sm.majority{
							if sm.matchIndex[cmd.From] > sm.commitIndex+1{
								for i:=sm.commitIndex+1 ; i<sm.matchIndex[cmd.From] ; i++{
									var entrie LogEntries
									e,_ := sm.log.Get(int64(i))
									json.Unmarshal(e,&entrie)
									actions = append(actions, CommitEv{i, entrie, nil})
								}
							}
							var entrie LogEntries
							e,_ := sm.log.Get(int64(sm.matchIndex[cmd.From]))
							json.Unmarshal(e,&entrie)
							actions = append(actions, CommitEv{sm.matchIndex[cmd.From], entrie, nil})
							sm.commitIndex = sm.matchIndex[cmd.From]
						}
					}
				}else{
					sm.matchIndex[cmd.From]=sm.nextIndex[cmd.From]
					sm.nextIndex[cmd.From]++
					prev := sm.nextIndex[cmd.From] - 1
					var entrie LogEntries
					e,_ := sm.log.Get(int64(prev))
					json.Unmarshal(e,&entrie)
					var entrie1 LogEntries
					e1,_ := sm.log.Get(int64(prev+1))
					json.Unmarshal(e1,&entrie1)
					actions = append(actions, Send{cmd.From ,AppendEntriesReqEv{Term: sm.Term, LeaderId: sm.id, prevLogIndex: prev, prevLogTerm: entrie.Term, entries: entrie1, leaderCommit: sm.commitIndex, From: sm.id}})
					actions = append(actions, alarm{sec: sm.ElectionTimeout})
				}
			}else{
				if sm.Term < cmd.Term{
					sm.State = "follower"
					sm.Term = cmd.Term
					sm.votedFor = 0
				}else{
					sm.nextIndex[cmd.From]--
					prev := sm.nextIndex[cmd.From] - 1
					sm.matchIndex[cmd.From] = prev
					var entrie LogEntries
					e,_ := sm.log.Get(int64(prev))
					json.Unmarshal(e,&entrie)
					var entrie1 LogEntries
					e1,_ := sm.log.Get(int64(prev+1))
					json.Unmarshal(e1,&entrie1)
					actions = append(actions, Send{cmd.From ,AppendEntriesReqEv{Term: sm.Term, LeaderId: sm.id, prevLogIndex: prev, prevLogTerm: entrie.Term, entries: entrie1, leaderCommit: sm.commitIndex, From: sm.id}})
					actions = append(actions, alarm{sec: sm.ElectionTimeout})
				}
				//decrement nextIndex and retry 
			}
		}


	// RequestVote RPC
	case VoteReqEv:
		cmd := ev.(VoteReqEv)
		var voteGranted bool
		if sm.State == "follower"{
			if cmd.Term < sm.Term{
				voteGranted = false
			}else{
					if cmd.LastLogTerm > sm.LastLogTerm{		
						sm.Term = cmd.Term
						sm.votedFor = cmd.CandidateId
						voteGranted = true
					}else if cmd.LastLogTerm == sm.LastLogTerm && cmd.LastLogIndex >= sm.LastLogIndex{		
						sm.Term = cmd.Term
						sm.votedFor = cmd.CandidateId
						voteGranted = true
					}else{
						voteGranted = false
					}
			}
			actions = append(actions, Send{cmd.From, VoteRespEv{Term: sm.Term, voteGranted: voteGranted, From: sm.id}})
			actions = append(actions, alarm{sec: sm.ElectionTimeout})

		}else{										//leader or candidate
				if sm.Term < cmd.Term{
					sm.Term = cmd.Term
					sm.votedFor = 0
					sm.State = "follower" 
					actions = append(actions, alarm{sec: sm.ElectionTimeout})    
				}
		}



	case VoteRespEv:
		cmd := ev.(VoteRespEv)
		if sm.State == "candidate"{
			if sm.Term < cmd.Term{
				sm.Term = cmd.Term
				sm.votedFor = 0
				sm.State = "follower"
				actions = append(actions, alarm{sec: sm.ElectionTimeout})
			}else{
				if cmd.voteGranted == true{
					sm.numVotesGranted++
					sm.votesRcvd[cmd.From] = 1		// 1 for peer has voted yes
				}else if cmd.voteGranted == false{
					sm.numVotesDenied++		
					sm.votesRcvd[cmd.From] = -1		// -1 for peer has voted no
				}else{
					sm.votesRcvd[cmd.From] = 0		// 0 for peer has not voted
				}
				sm.majority = 3
				if sm.numVotesGranted >= sm.majority{
					sm.State = "leader"
					sm.nextIndex[sm.id] = sm.LastLogIndex + 1
					for _,i := range sm.peers{
						actions = append(actions, Send{i ,AppendEntriesReqEv{Term: sm.Term, LeaderId: sm.id, leaderCommit: sm.commitIndex, From: sm.id}}) //to all
						sm.nextIndex[i] = sm.LastLogIndex + 1
					}
					actions = append(actions, alarm{sec: sm.HeartbeatTimeout})  //heartbeat timer
				}
			}	
		}	



	case TimeoutEv:
		if sm.State == "follower" || sm.State == "candidate"{
			sm.State = "candidate"
			sm.Term++
			sm.votedFor = sm.id
			sm.numVotesGranted++
			sm.votesRcvd[sm.id] = 1
			actions = append(actions, alarm{sec: sm.ElectionTimeout})
			for _,i := range sm.peers{
				if sm.LastLogIndex > -1{
					var entrie LogEntries
					e,_ := sm.log.Get(int64(sm.LastLogIndex))
					json.Unmarshal(e,&entrie)
					sm.LastLogTerm = entrie.Term
				}
				actions = append(actions, Send{i, VoteReqEv{Term: sm.Term, CandidateId: sm.id, LastLogIndex: sm.LastLogIndex, LastLogTerm: sm.LastLogTerm, From: sm.id}})//to all			
			}
			
		}else{
			for _,i := range sm.peers{
				actions = append(actions, Send{i, AppendEntriesReqEv{Term: sm.Term, LeaderId: sm.id, leaderCommit: sm.commitIndex, From: sm.id}})   //hearbeat timeout
				actions = append(actions, alarm{sec: sm.HeartbeatTimeout})
			}
			
		}



	case AppendEv:
		cmd := ev.(AppendEv)
		if sm.State == "follower"{
			 //forward to leader
		}else if sm.State == "leader"{
			nxt := sm.nextIndex[sm.id]   
			entry := LogEntries{sm.Term, cmd.Data}
			d, _ := json.Marshal(entry)
			sm.log.Append(d)
			sm.LastLogIndex = int(sm.log.GetLastIndex())
			sm.nextIndex[sm.id]++
			sm.matchIndex[sm.id]++
			sm.noAppendRes[nxt] = 0
			var tem int
			for   _,i := range sm.peers{
				if (nxt-1) == -1{
					tem = 0
				}else{
					var entrie LogEntries
					e,_ := sm.log.Get(int64(nxt-1))
					json.Unmarshal(e,&entrie)
					tem = entrie.Term
				}
				actions = append(actions, Send{i, AppendEntriesReqEv{Term: sm.Term, LeaderId: sm.id, prevLogIndex: nxt-1, prevLogTerm: tem, entries: entry, leaderCommit: sm.commitIndex, From: sm.id}})   //to all
				actions = append(actions, alarm{sec: sm.HeartbeatTimeout})
			}
		}

	case Send:


	case LogStoreEv:
	default: println ("Unrecognized")
	}
	return actions
}