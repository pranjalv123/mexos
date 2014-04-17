package mapreduce

import "container/list"
import "fmt"

type WorkerInfo struct {
	address string
	busy bool
	alive bool
	// You can add definitions here.
}

// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
	l := list.New()
	for _, w := range mr.Workers {
		DPrintf("DoWork: shutdown %s\n", w.address)
		args := &ShutdownArgs{}
		var reply ShutdownReply
		ok := call(w.address, "Worker.Shutdown", args, &reply)
		if ok == false {
			fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
		} else {
			l.PushBack(reply.Njobs)
		}
	}
	return l
}

func (mr *MapReduce) findFreeWorker() string {
	for _, w := range mr.Workers {
		if w.busy == false && w.alive == true{
			return w.address
		}
	}
	return ""
}

func (mr *MapReduce) trackBusiness (query chan string, done chan string, update chan int, quit chan int, finish chan int, updel chan int, jobsLeft int) {
	f := mr.findFreeWorker()
	for  {
		for ; f == "" ;  {
			select {
			case <- update:
				f = mr.findFreeWorker()
				
			case  <- done:
				jobsLeft --
				if jobsLeft == 0 {
					quit <- 0
					finish <- 0
					return
				}
				f = mr.findFreeWorker()	
			}
		}
		select {
		case (query <- f):
			f = mr.findFreeWorker()
			fmt.Println("Jobs remaining", jobsLeft)
		case  <- done:
			jobsLeft --
			if jobsLeft == 0 {
				quit <- 0
				finish <- 0
				return
			}
			f = mr.findFreeWorker()
		case <- update:
			f = mr.findFreeWorker()
		case <- updel:
			f = mr.findFreeWorker()
		}
	}
}

func (mr *MapReduce) manageWorkers(update chan int, del chan string, updel chan int) {
	mr.Workers = make(map[string]*WorkerInfo)
	for {
		select {
		case  reg := <- mr.registerChannel:
			fmt.Println("Adding server!") 
			mr.Workers[reg] = new(WorkerInfo)
			mr.Workers[reg].address = reg
			mr.Workers[reg].alive = true
			cnt := 0
			for _, s := range mr.Workers {
				if s.alive { cnt++ }
			}
			fmt.Println("Living servers (add)", cnt)
			update <- 0
		case reg := <- del:
			mr.Workers[reg].alive = false
			cnt := 0
			for _, s := range mr.Workers {
				if s.alive { cnt++ }
			}
			fmt.Println("Living servers (del)", cnt)
			select {
			case updel <- 0:
				fmt.Println("sent updel (del)", cnt)
			default:
				fmt.Println("didn't send updel (del)", cnt)
			}

		}
	}
}

func (mr *MapReduce) launchTask(i int, args DoJobArgs, loc string, done chan string, add chan int, del chan string) {
	reply := new(DoJobReply)
	w := mr.Workers[loc]
	fmt.Println("Assigned job ", i, " to ", loc)
	call(w.address, "Worker.DoJob", args, reply)
	if (!reply.OK) {
		fmt.Println("Failed task! Returning to queue, worker ", w.address, " task ", i)
		del <- loc
		add <- i
	} else {
		mr.Workers[loc].busy = false	
		mr.Workers[loc].alive = true	
		done <- loc
		fmt.Println("Finished job ", i, "!")
	}
}

func (mr *MapReduce) jobPool(query chan int, add chan int, quit chan int ) {
	jobs := list.New()
	for {
		if (jobs.Len() > 0) {
			select{
			case query <- jobs.Front().Value.(int):
				jobs.Remove(jobs.Front())
				fmt.Println("Queue has ", jobs.Len(), " jobs")
			case j := <- add:
				jobs.PushBack(j)
			case <- quit:
				fmt.Println("Exiting with jobs in queue...")
				return
				
			}
		} else {
			select {
			case j := <- add:
				jobs.PushBack(j)
			case <- quit:
				fmt.Println("Job pool shutting down")
				return
			}
		}
	}
}

func (mr *MapReduce) runTask(numTasks int, numOther int, update chan int, updel chan int, del chan string, op JobType){
	query := make(chan string)
	jobs := make(chan int)
	done := make(chan string)
	add := make(chan int)
	quit := make(chan int)
	finish := make(chan int)
	go mr.trackBusiness(query, done, update, quit, finish, updel, numTasks)
	go mr.jobPool(jobs, add, quit)
	for i := 0; i < numTasks; i ++ {
		add <- i
	}
	for {
		fmt.Println("Waiting for job or quit")
		select{
		case j := <- jobs:
			args :=	DoJobArgs{
				File:mr.file,
				Operation:op,
				JobNumber:j,
				NumOtherPhase:numOther,
			}
			loc := <-query
			if loc == "" {
				fmt.Println("ERROR - blank loc")
			}
			mr.Workers[loc].busy = true
			go mr.launchTask(j, args, loc, done, add, del)

		case <- finish:
			fmt.Println("Done Waiting for exit signal")
			return
		}
	}
}

func (mr *MapReduce) RunMaster() *list.List {
	update := make(chan int)
	del := make(chan string)
	updel := make(chan int)
	go mr.manageWorkers(update, del, updel)
	mr.runTask(mr.nMap, mr.nReduce, update, updel, del, Map)
	mr.runTask(mr.nReduce, mr.nMap, update, updel, del, Reduce)
	return mr.KillWorkers()
}
