package raft

import "testing"
import "time"
import "strconv"

func Test_RaftNode(t *testing.T){

	nodes,_ := Start()
	
	time.Sleep(time.Second*1)


	// multiple appends
	ldr := LeaderId(nodes)

	// append loop
	for i:=1;i<=5;i++{
		nodes[ldr-1].Append([]byte("foo"+ strconv.Itoa(i)))
	}

		time.Sleep(5*time.Second)

	// commit checking loop
	for i:=1;i<=5;i++{
		for _, rnode:= range nodes {
			select { // to avoid blocking on channel.
				case ev := <- rnode.CommitChannel():
					ci:=ev.(CommitEv)
					if ci.Err != nil {t.Fatal(ci.Err)}
					if string(ci.Data.Data) != ("foo"+ strconv.Itoa(i)) {
						t.Fatal("Got different data")
					}
				default: t.Fatal("Expected message on all nodes")
			}
		}
	} 

	//timeout test
	nodes[2].timeoutCh <- TimeoutEv{}
	time.Sleep(time.Millisecond*500)
	ldr = LeaderId(nodes)
	nodes[ldr-1].Append([]byte("pooja"))
	time.Sleep(500*time.Millisecond)
	for _,rnode:= range nodes {
			select { // to avoid blocking on channel.
				case ev := <- rnode.CommitChannel():
					ci:=ev.(CommitEv)
					if ci.Err != nil {t.Fatal(ci.Err)}
					if string(ci.Data.Data) != "pooja" {
						t.Fatal("Got different data in timeout testcase")
					}
				default: t.Fatal("Expected message on all nodes timeout testcase")
			}
		}

	// shutdown test
	oldldr := LeaderId(nodes)
	nodes[oldldr-1].Shutdown()
	time.Sleep(1500*time.Millisecond)
	newldr := LeaderId(nodes)
	nodes[newldr-1].Append([]byte("NewLeader"))
	time.Sleep(500*time.Millisecond)
	for i:=1; i<=5; i++ {
		if i!=oldldr{
				select { // to avoid blocking on channel.
					case ev := <- nodes[i-1].CommitChannel():
						ci:=ev.(CommitEv)
						if ci.Err != nil {t.Fatal(ci.Err)}
						if string(ci.Data.Data) != "NewLeader" {
							t.Fatal("Got different data in shutdown testcase")
						}
					default: t.Fatal("Expected message on all nodes shutdown testcase")
				}
		}
	}
	nodes[newldr-1].Shutdown()
	time.Sleep(1500*time.Millisecond)
	nxtnewldr := LeaderId(nodes)
	nodes[nxtnewldr-1].Append([]byte("NextNewLeader"))
	time.Sleep(500*time.Millisecond)
	for i:=1; i<=5; i++ {
		if i!=oldldr && i!=newldr{
				select { // to avoid blocking on channel.
					case ev := <- nodes[i-1].CommitChannel():
						ci:=ev.(CommitEv)
						if ci.Err != nil {t.Fatal(ci.Err)}
						if string(ci.Data.Data) != "NextNewLeader" {
							t.Fatal("Got different data in next shutdown testcase")
						}
					default: t.Fatal("Expected message on all nodes next shutdown testcase")
				}
		}
	}
	for i:=1; i<=5; i++{
		if i!=ldr && i!=newldr{
			nodes[i-1].Shutdown()
		}
	}
	time.Sleep(time.Second*0)
}
/*func Test_Partition(t *testing.T){
	
	nodes,clust := Start()
	time.Sleep(time.Second*1)
	clust.Partition([]int{1,2,3},[]int{4,5})

		time.Sleep(200*time.Millisecond)
		ldr := LeaderId(nodes[:3])
		nodes[ldr-1].Append([]byte("In Partition"))
		time.Sleep(500 * time.Millisecond)
		for i:=1; i<=3; i++ {
				select { // to avoid blocking on channel.
					case ev := <- nodes[i-1].CommitChannel():
						ci:=ev.(CommitEv)
						if ci.Err != nil {t.Fatal(ci.Err)}
						if string(ci.Data.Data) != "In Partition" {
							t.Fatal("Got different data in partition testcase")
						}
					default: t.Fatal("Expected message on all nodes partition testcase")
				}
		}
		
		clust.Heal()
		time.Sleep(time.Duration(200)*time.Millisecond)
		ldr = LeaderId(nodes)
		
		nodes[ldr-1].Append([]byte("After Heal"))
		time.Sleep(time.Duration(2000) * time.Millisecond)

		for i:=4; i<=5; i++ {
				select { // to avoid blocking on channel.
					case ev := <- nodes[i-1].CommitChannel():
						ci:=ev.(CommitEv)
						if ci.Err != nil {t.Fatal(ci.Err)}
						if string(ci.Data.Data) != "In Partition" {
							t.Fatal("Got different data in partition testcase")
						}
					default: t.Fatal("Expected message on all nodes partition testcase")
				}
		}

}*/