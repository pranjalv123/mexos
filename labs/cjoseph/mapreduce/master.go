package mapreduce
import "container/list"
import "fmt"
//import "log"
//import "strconv"

type WorkerInfo struct {
	address string
	// You can add definitions here.
}

type ReplyInfo struct{
	job int
	OK bool
	reply *DoJobReply
}

// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
  l := list.New()
  for _, w := range mr.Workers {
    DPrintf("DoWork: shutdown %s\n", w.address)
    args := &ShutdownArgs{}
    var reply ShutdownReply;
    ok := call(w.address, "Worker.Shutdown", args, &reply)
    if ok == false {
      fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
    } else {
      l.PushBack(reply.Njobs)
    }
  }
  return l
}

func (mr *MapReduce) RunMaster() *list.List {
	
	//start registraion thread
	go ReceiveRegister(mr)
	completed := 0 //tally of completed jobs
	
	//create list of outstanding jobs
	jobs := list.New() 
	for i := 0; i < mr.nMap; i++{
		jobs.PushFront(i)
	}

	//loop for the map stage
	for completed != mr.nMap {
		listen(mr, Map, &completed, jobs)
	}
	
	//start reduce jobs once maps are done
	completed = 0
	jobs.Init() //clear list
	for i := 0; i < mr.nReduce; i++{
		jobs.PushFront(i)
	}
	for completed != mr.nReduce {
		listen(mr, Reduce, &completed, jobs)
	}
	
	//yaaaay we're done
	return mr.KillWorkers()
}

func listen(mr *MapReduce, stage JobType, completed *int, jobs *list.List){
	
	NumOther := 0
	switch stage {
	case Map:
		NumOther = mr.nReduce
	case Reduce:
		NumOther = mr.nMap
	}

	if jobs.Len() != 0 {
		select{
		//wait for worker responses
		case r := <-mr.responses:
			HandleResponse(r, completed, jobs)

		//wait for available if none are available
		case id := <- mr.available:
			w := mr.Workers[id]
			//pop off a job id
			j := jobs.Remove(jobs.Front()).(int)
			args := &DoJobArgs{mr.file, stage, j, NumOther}
			go SendRPC(mr, w, args, j)	
		}	
	} else {
		r := <-mr.responses
		HandleResponse(r, completed, jobs)
	}
}

func HandleResponse(r *ReplyInfo, completed *int, jobs *list.List){
	if r.OK {
		//add to our completed count
		*completed++
	} else {
		//add job back to queue, it failed.
		jobs.PushFront(r.job)
	}
}

func ReceiveRegister(mr *MapReduce){
	//a blocking listen for new worker registration
	for {
		addr := <- mr.registerChannel
		//add new worker to map. key is integer cause I can't think of anything better
		w := WorkerInfo{addr}
		mr.Workers[addr] = &w
		//announce available worker on channel
		mr.available <- addr
	}
}

func SendRPC(mr *MapReduce, w *WorkerInfo, args *DoJobArgs, job int){
	var reply DoJobReply
	success := call(w.address, "Worker.DoJob", args, &reply)
	//put reply on channel, regardless of success
	mr.responses <- &ReplyInfo{job, success, &reply}

	if success{
		//put worker on channel available
		mr.available <- w.address	
	} else {
		//de-register this dead worker
		delete(mr.Workers, w.address)
	}
}
