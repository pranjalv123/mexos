package paxos

import "testing"
import "runtime"
import "fmt"
import "strconv"
//import "os"
import "time"
import "math/rand"

const startport = 22222

func netport(i int) string {
	s := "127.0.0.1:"
	s += strconv.Itoa(startport+i)
	return s
}

func TestNetworkBasic(t *testing.T) {
  runtime.GOMAXPROCS(4)

  const npaxos = 3
  var pxa []*Paxos = make([]*Paxos, npaxos)
  var pxh []string = make([]string, npaxos)
  defer cleanup(pxa)

  for i := 0; i < npaxos; i++ {
    pxh[i] = netport(i)
  }
  for i := 0; i < npaxos; i++ {
    pxa[i] = Make(pxh, i, nil, true)
  }

  fmt.Printf("Test: Single proposer ...\n")

  pxa[0].Start(0, "hello")
  waitForDecision(t, pxa, 0, npaxos)

  fmt.Printf("  ... Passed\n")

  fmt.Printf("Test: Many proposers, same value ...\n")

  for i := 0; i < npaxos; i++ {
    pxa[i].Start(1, 77)
  }
  waitForDecision(t, pxa, 1, npaxos)

  fmt.Printf("  ... Passed\n")

  fmt.Printf("Test: Many proposers, different values ...\n")

  pxa[0].Start(2, 100)
  pxa[1].Start(2, 101)
  pxa[2].Start(2, 102)
  waitForDecision(t, pxa, 2, npaxos)

  fmt.Printf("  ... Passed\n")

  fmt.Printf("Test: Out-of-order instances ...\n")

  pxa[0].Start(7, 700)
  pxa[0].Start(6, 600)
  pxa[1].Start(5, 500)
  waitForDecision(t, pxa, 7, npaxos)
  pxa[0].Start(4, 400)
  pxa[1].Start(3, 300)
  waitForDecision(t, pxa, 6, npaxos)
  waitForDecision(t, pxa, 5, npaxos)
  waitForDecision(t, pxa, 4, npaxos)
  waitForDecision(t, pxa, 3, npaxos)

  if pxa[0].Max() != 7 {
    t.Fatalf("wrong Max()")
  }

  fmt.Printf("  ... Passed\n")
}

func TestNetworkDeaf(t *testing.T) {
  runtime.GOMAXPROCS(4)

  const npaxos = 5
  var pxa []*Paxos = make([]*Paxos, npaxos)
  var pxh []string = make([]string, npaxos)
  defer cleanup(pxa)

  for i := 0; i < npaxos; i++ {
    pxh[i] = netport(i)
  }
  for i := 0; i < npaxos; i++ {
    pxa[i] = Make(pxh, i, nil, true)
  }

  fmt.Printf("Test: Deaf proposer ...\n")

  pxa[0].Start(0, "hello")
  waitForDecision(t, pxa, 0, npaxos)


  //os.Remove(pxh[0])
  //os.Remove(pxh[
	pxa[0].deaf = true
	pxa[npaxos-1].deaf = true

  pxa[1].Start(1, "goodbye")
  waitForDecisionMajority(t, pxa, 1)
  time.Sleep(1 * time.Second)
  if numDecided(t, pxa, 1) != npaxos - 2 {
    t.Fatalf("a deaf peer heard about a decision")
  }

  pxa[0].Start(1, "xxx")
  waitForDecision(t, pxa, 1, npaxos-1)
  time.Sleep(1 * time.Second)
  if numDecided(t, pxa, 1) != npaxos - 1 {
    t.Fatalf("a deaf peer heard about a decision")
  }

  pxa[npaxos-1].Start(1, "yyy")
  waitForDecision(t, pxa, 1, npaxos)

  fmt.Printf("  ... Passed\n")
}

func TestNetworkForget(t *testing.T) {
  runtime.GOMAXPROCS(4)

  const npaxos = 6
  var pxa []*Paxos = make([]*Paxos, npaxos)
  var pxh []string = make([]string, npaxos)
  defer cleanup(pxa)
  
  for i := 0; i < npaxos; i++ {
    pxh[i] = netport(i)
  }
  for i := 0; i < npaxos; i++ {
    pxa[i] = Make(pxh, i, nil, true)
  }

  fmt.Printf("Test: Forgetting ...\n")

  // initial Min() correct?
  for i := 0; i < npaxos; i++ {
    m := pxa[i].Min()
    if m > 0 {
      t.Fatalf("wrong initial Min() %v", m)
    }
  }

  pxa[0].Start(0, "00")
  pxa[1].Start(1, "11")
  pxa[2].Start(2, "22")
  pxa[0].Start(6, "66")
  pxa[1].Start(7, "77")

  waitForDecision(t, pxa, 0, npaxos)

  // Min() correct?
  for i := 0; i < npaxos; i++ {
    m := pxa[i].Min()
    if m != 0 {
      t.Fatalf("wrong Min() %v; expected 0", m)
    }
  }

  waitForDecision(t, pxa, 1, npaxos)

  // Min() correct?
  for i := 0; i < npaxos; i++ {
    m := pxa[i].Min()
    if m != 0 {
      t.Fatalf("wrong Min() %v; expected 0", m)
    }
  }

  // everyone Done() -> Min() changes?
  for i := 0; i < npaxos; i++ {
    pxa[i].Done(0)
  }
  for i := 1; i < npaxos; i++ {
    pxa[i].Done(1)
  }
  for i := 0; i < npaxos; i++ {
    pxa[i].Start(8 + i, "xx")
  }
  allok := false
  for iters := 0; iters < 12; iters++ {
    allok = true
    for i := 0; i < npaxos; i++ {
      s := pxa[i].Min()
      if s != 1 {
        allok = false
      }
    }
    if allok {
      break
    }
    time.Sleep(1 * time.Second)
  }
  if allok != true {
    t.Fatalf("Min() did not advance after Done()")
  }

  fmt.Printf("  ... Passed\n")
}

func TestNetworkManyForget(t *testing.T) {
  runtime.GOMAXPROCS(4)

  const npaxos = 3
  var pxa []*Paxos = make([]*Paxos, npaxos)
  var pxh []string = make([]string, npaxos)
  defer cleanup(pxa)
  
  for i := 0; i < npaxos; i++ {
    pxh[i] = netport(i)
  }
  for i := 0; i < npaxos; i++ {
    pxa[i] = Make(pxh, i, nil, true)
    pxa[i].unreliable = true
  }

  fmt.Printf("Test: Lots of forgetting ...\n")

  const maxseq = 20
  done := false

  go func() {
    na := rand.Perm(maxseq)
    for i := 0; i < len(na); i++ {
      seq := na[i]
      j := (rand.Int() % npaxos)
      v := rand.Int() 
      pxa[j].Start(seq, v)
      runtime.Gosched()
    }
  }()

  go func() {
    for done == false {
      seq := (rand.Int() % maxseq)
      i := (rand.Int() % npaxos)
      if seq >= pxa[i].Min() {
        decided, _ := pxa[i].Status(seq)
        if decided {
          pxa[i].Done(seq)
        }
      }
      runtime.Gosched()
    }
  }()

  time.Sleep(5 * time.Second)
  done = true
  for i := 0; i < npaxos; i++ {
    pxa[i].unreliable = false
  }
  time.Sleep(2 * time.Second)

  for seq := 0; seq < maxseq; seq++ {
    for i := 0; i < npaxos; i++ {
      if seq >= pxa[i].Min() {
        pxa[i].Status(seq)
      }
    }
  }

  fmt.Printf("  ... Passed\n")
}

//
// does paxos forgetting actually free the memory?
//
func TestNetworkForgetMem(t *testing.T) {
  runtime.GOMAXPROCS(4)

  fmt.Printf("Test: Paxos frees forgotten instance memory ...\n")

  const npaxos = 3
  var pxa []*Paxos = make([]*Paxos, npaxos)
  var pxh []string = make([]string, npaxos)
  defer cleanup(pxa)
  
  for i := 0; i < npaxos; i++ {
    pxh[i] = netport(i)
  }
  for i := 0; i < npaxos; i++ {
    pxa[i] = Make(pxh, i, nil, true)
  }

  pxa[0].Start(0, "x")
  waitForDecision(t, pxa, 0, npaxos)

  runtime.GC()
  var m0 runtime.MemStats
  runtime.ReadMemStats(&m0)
  // m0.Alloc about a megabyte

  for i := 1; i <= 10; i++ {
    big := make([]byte, 1000000)
    for j := 0; j < len(big); j++ {
      big[j] = byte('a' + rand.Int() % 26)
    }
    pxa[0].Start(i, string(big))
    waitForDecision(t, pxa, i, npaxos)
  }

  runtime.GC()
  var m1 runtime.MemStats
  runtime.ReadMemStats(&m1)
  // m1.Alloc about 90 megabytes

  for i := 0; i < npaxos; i++ {
    pxa[i].Done(10)
  }
  for i := 0; i < npaxos; i++ {
    pxa[i].Start(11 + i, "z")
  }
  time.Sleep(3 * time.Second)
  for i := 0; i < npaxos; i++ {
    if pxa[i].Min() != 11 {
      t.Fatalf("expected Min() %v, got %v\n", 11, pxa[i].Min())
    }
  }

  runtime.GC()
  var m2 runtime.MemStats
  runtime.ReadMemStats(&m2)
  // m2.Alloc about 10 megabytes

  if m2.Alloc > (m1.Alloc / 2) {
    t.Fatalf("memory use did not shrink enough")
  }

  fmt.Printf("  ... Passed\n")
}

func TestNetworkRPCCount(t *testing.T) {
  runtime.GOMAXPROCS(4)

  fmt.Printf("Test: RPC counts aren't too high ...\n")

  const npaxos = 3
  var pxa []*Paxos = make([]*Paxos, npaxos)
  var pxh []string = make([]string, npaxos)
  defer cleanup(pxa)

  for i := 0; i < npaxos; i++ {
    pxh[i] = netport(i)
  }
  for i := 0; i < npaxos; i++ {
    pxa[i] = Make(pxh, i, nil, true)
  }

  ninst1 := 5
  seq := 0
  for i := 0; i < ninst1; i++ {
    pxa[0].Start(seq, "x")
    waitForDecision(t, pxa, seq, npaxos)
    seq++
  }

  time.Sleep(2 * time.Second)

  total1 := 0
  for j := 0; j < npaxos; j++ {
    total1 += pxa[j].rpcCount
  }

  // per agreement:
  // 3 prepares
  // 3 accepts
  // 3 decides
  expected1 := ninst1 * npaxos * npaxos
  if total1 > expected1 {
    t.Fatalf("too many RPCs for serial Start()s; %v instances, got %v, expected %v",
      ninst1, total1, expected1)
  }

  ninst2 := 5
  for i := 0; i < ninst2; i++ {
    for j := 0; j < npaxos; j++ {
      go pxa[j].Start(seq, j + (i * 10))
    }
    waitForDecision(t, pxa, seq, npaxos)
    seq++
  }

  time.Sleep(2 * time.Second)

  total2 := 0
  for j := 0; j < npaxos; j++ {
    total2 += pxa[j].rpcCount
  }
  total2 -= total1

  // worst case per agreement:
  // Proposer 1: 3 prep, 3 acc, 3 decides.
  // Proposer 2: 3 prep, 3 acc, 3 prep, 3 acc, 3 decides.
  // Proposer 3: 3 prep, 3 acc, 3 prep, 3 acc, 3 prep, 3 acc, 3 decides.
  expected2 := ninst2 * npaxos * 15
  if total2 > expected2 {
    t.Fatalf("too many RPCs for concurrent Start()s; %v instances, got %v, expected %v",
      ninst2, total2, expected2)
  }

  fmt.Printf("  ... Passed\n")
}

//
// many agreements (without failures)
//
func TestNetworkMany(t *testing.T) {
  runtime.GOMAXPROCS(4)

  fmt.Printf("Test: Many instances ...\n")

  const npaxos = 3
  var pxa []*Paxos = make([]*Paxos, npaxos)
  var pxh []string = make([]string, npaxos)
  defer cleanup(pxa)

  for i := 0; i < npaxos; i++ {
    pxh[i] = netport(i)
  }
  for i := 0; i < npaxos; i++ {
    pxa[i] = Make(pxh, i, nil, true)
    pxa[i].Start(0, 0)
  }

  const ninst = 50
  for seq := 1; seq < ninst; seq++ {
    // only 5 active instances, to limit the
    // number of file descriptors.
    for seq >= 5 && numDecided(t, pxa, seq - 5) < npaxos {
      time.Sleep(20 * time.Millisecond)
    }
    for i := 0; i < npaxos; i++ {
      pxa[i].Start(seq, (seq * 10) + i)
    }
  }

  for {
    done := true
    for seq := 1; seq < ninst; seq++ {
      if numDecided(t, pxa, seq) < npaxos {
        done = false
      }
    }
    if done {
      break
    }
    time.Sleep(100 * time.Millisecond)
  }

  fmt.Printf("  ... Passed\n")
}

//
// a peer starts up, with proposal, after others decide.
// then another peer starts, without a proposal.
// 
func TestNetworkOld(t *testing.T) {
  runtime.GOMAXPROCS(4)

  fmt.Printf("Test: Minority proposal ignored ...\n")

  const npaxos = 5
  var pxa []*Paxos = make([]*Paxos, npaxos)
  var pxh []string = make([]string, npaxos)
  defer cleanup(pxa)

  for i := 0; i < npaxos; i++ {
    pxh[i] = netport(i)
  }

  pxa[1] = Make(pxh, 1, nil, true)
  pxa[2] = Make(pxh, 2, nil, true)
  pxa[3] = Make(pxh, 3, nil, true)
  pxa[1].Start(1, 111)

  waitForDecisionMajority(t, pxa, 1)

  pxa[0] = Make(pxh, 0, nil, true)
  pxa[0].Start(1, 222)

  waitForDecision(t, pxa, 1, 4)

  if false {
    pxa[4] = Make(pxh, 4, nil, true)
    waitForDecision(t, pxa, 1, npaxos)
  }

  fmt.Printf("  ... Passed\n")
}

//
// many agreements, with unreliable RPC
//
func TestNetworkManyUnreliable(t *testing.T) {
  runtime.GOMAXPROCS(4)

  fmt.Printf("Test: Many instances, unreliable RPC ...\n")

  const npaxos = 3
  var pxa []*Paxos = make([]*Paxos, npaxos)
  var pxh []string = make([]string, npaxos)
  defer cleanup(pxa)

  for i := 0; i < npaxos; i++ {
    pxh[i] = netport(i)
  }
  for i := 0; i < npaxos; i++ {
    pxa[i] = Make(pxh, i, nil, true)
    pxa[i].unreliable = true
    pxa[i].Start(0, 0)
  }

  const ninst = 50
  for seq := 1; seq < ninst; seq++ {
    // only 3 active instances, to limit the
    // number of file descriptors.
    for seq >= 3 && numDecided(t, pxa, seq - 3) < npaxos {
      time.Sleep(20 * time.Millisecond)
    }
    for i := 0; i < npaxos; i++ {
      pxa[i].Start(seq, (seq * 10) + i)
    }
  }

  for {
    done := true
    for seq := 1; seq < ninst; seq++ {
      if numDecided(t, pxa, seq) < npaxos {
        done = false
      }
    }
    if done {
      break
    }
    time.Sleep(100 * time.Millisecond)
  }
  
  fmt.Printf("  ... Passed\n")
}

/*
func pp(tag string, src int, dst int) string {
  s := "/var/tmp/824-"
  s += strconv.Itoa(os.Getuid()) + "/"
  s += "px-" + tag + "-"
  s += strconv.Itoa(os.Getpid()) + "-"
  s += strconv.Itoa(src) + "-"
  s += strconv.Itoa(dst)
  return s
}

func cleanpp(tag string, n int) {
  for i := 0; i < n; i++ {
    for j := 0; j < n; j++ {
      ij := pp(tag, i, j)
      os.Remove(ij)
    }
  }
}*/

func part(t *testing.T, pxa []*Paxos , p1 []int, p2 []int, p3 []int) {
	//set all inititally unreachable
	for _,p := range pxa {
		for i,_ := range p.reachable {
			p.reachable[i] = false
		}
	}

	//then create connections
	for _, h := range p1 {
		for _, r := range p1 {
			pxa[h].reachable[r] = true
		}
	}
	for _, h := range p2 {
		for _, r := range p2 {
			pxa[h].reachable[r] = true
		}
	}
	for _, h := range p3 {
		for _, r := range p3 {
			pxa[h].reachable[r] = true
		}
	}
}

func TestNetworkPartition(t *testing.T) {
	runtime.GOMAXPROCS(4)

	//tag := "partition"
	const npaxos = 5
	//defer cleanpp(tag, npaxos)
	var pxa []*Paxos = make([]*Paxos, npaxos)
	var pxh []string = make([]string, npaxos)
	defer cleanup(pxa)

	for i := 0; i < npaxos; i++ {
		pxh[i] = netport(i) //TODO: fixme
	}
	for i := 0; i < npaxos; i++ {
		pxa[i] = Make(pxh, i, nil, true)
	}
	
	//defer part(t, tag, npaxos, []int{}, []int{}, []int{})
	seq := 0
	
	fmt.Printf("Test: No decision if partitioned ...\n")

	part(t, pxa, []int{0,2}, []int{1,3}, []int{4})
	pxa[1].Start(seq, 111)
	checkMaxDecided(t, pxa, seq, 0)
	
	fmt.Printf("  ... Passed\n")
	
	fmt.Printf("Test: Decision in majority partition ...\n")
	
	part(t, pxa, []int{0}, []int{1,2,3}, []int{4})
	time.Sleep(2 * time.Second)
	waitForDecisionMajority(t, pxa, seq)
	fmt.Printf("  ... Passed\n")
	
	fmt.Printf("Test: All agree after full heal ...\n")
	pxa[0].Start(seq, 1000) // poke them
	pxa[4].Start(seq, 1004)
	part(t, pxa, []int{0,1,2,3,4}, []int{}, []int{})
	
	waitForDecision(t, pxa, seq, npaxos)
	fmt.Printf("  ... Passed\n")

	fmt.Printf("Test: One peer switches partitions ...\n")
	for iters := 0; iters < 20; iters++ {
		seq++

		part(t, pxa, []int{0,1,2}, []int{3,4}, []int{})
		pxa[0].Start(seq, seq * 10)
		pxa[3].Start(seq, (seq * 10) + 1)
		waitForDecisionMajority(t, pxa, seq)
		if numDecided(t, pxa, seq) > 3 {
			t.Fatalf("too many decided")
		}
		
		part(t, pxa, []int{0,1}, []int{2,3,4}, []int{})
		waitForDecision(t, pxa, seq, npaxos)
	}
	fmt.Printf("  ... Passed\n")
	
	fmt.Printf("Test: One peer switches partitions, unreliable ...\n")
	for iters := 0; iters < 20; iters++ {
		seq++
		
		for i := 0; i < npaxos; i++ {
			pxa[i].unreliable = true
		}
		
		part(t, pxa, []int{0,1,2}, []int{3,4}, []int{})
		for i := 0; i < npaxos; i++ {
			pxa[i].Start(seq, (seq * 10) + i)
		}
		waitForDecision(t, pxa, seq, 3)
		if numDecided(t, pxa, seq) > 3 {
			t.Fatalf("too many decided")
		}
		
		part(t, pxa, []int{0,1}, []int{2,3,4}, []int{})
		
		for i := 0; i < npaxos; i++ {
			pxa[i].unreliable = false
		}
		
		waitForDecision(t, pxa, seq, 5)
	}
	fmt.Printf("  ... Passed\n")
}

func TestNetworkLots(t *testing.T) {
	runtime.GOMAXPROCS(4)
	
	fmt.Printf("Test: Many requests, changing partitions ...\n")

	//tag := "lots"
	const npaxos = 5
	//defer cleanpp(tag, npaxos)
	var pxa []*Paxos = make([]*Paxos, npaxos)
	var pxh []string = make([]string, npaxos)
	defer cleanup(pxa)

	for i := 0; i < npaxos; i++ {
		pxh[i] = netport(i) 
	}
	for i := 0; i < npaxos; i++ {
		pxa[i] = Make(pxh, i, nil, true)
		pxa[i].unreliable = true
	}
	//defer part(t, tag, npaxos, []int{}, []int{}, []int{})
	
	done := false
	
	// re-partition periodically
	ch1 := make(chan bool)
	go func() {
		defer func(){ ch1 <- true }()
		for done == false {
			var a [npaxos]int
			for i := 0; i < npaxos; i++ {
				a[i] = (rand.Int() % 3)
			}
			pa := make([][]int, 3)
			for i := 0; i < 3; i++ {
				pa[i] = make([]int, 0)
				for j := 0; j < npaxos; j++ {
					if a[j] == i {
						pa[i] = append(pa[i], j)
					}
				}
			}
			part(t, pxa, pa[0], pa[1], pa[2])
			time.Sleep(time.Duration(rand.Int63() % 200) * time.Millisecond)
		}
	}()
	seq := 0

	// periodically start a new instance
	ch2 := make(chan bool)
	go func () {
		defer func() { ch2 <- true } ()
		for done == false {
			// how many instances are in progress?
			nd := 0
			for i := 0; i < seq; i++ {
				if numDecided(t, pxa, i) == npaxos {
					nd++
				}
			}
			if seq - nd < 10 {
				for i := 0; i < npaxos; i++ {
					pxa[i].Start(seq, rand.Int() % 10)
				}
				seq++
			}
			time.Sleep(time.Duration(rand.Int63() % 300) * time.Millisecond)
		}
	}()
	
	// periodically check that decisions are consistent
	ch3 := make(chan bool)
	go func() {
		defer func() { ch3 <- true }()
		for done == false {
			for i := 0; i < seq; i++ {
				numDecided(t, pxa, i)
			}
			time.Sleep(time.Duration(rand.Int63() % 300) * time.Millisecond)
		}
	}()

	time.Sleep(20 * time.Second)
	done = true
	<- ch1
	<- ch2
	<- ch3
	
	// repair, then check that all instances decided.
	for i := 0; i < npaxos; i++ {
		pxa[i].unreliable = false
	}
	part(t, pxa, []int{0,1,2,3,4}, []int{}, []int{})
	time.Sleep(5 * time.Second)
	
	for i := 0; i < seq; i++ {
		waitForDecisionMajority(t, pxa, i)
	}
	
  fmt.Printf("  ... Passed\n")
}
	
