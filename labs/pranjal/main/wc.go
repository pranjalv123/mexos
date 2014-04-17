package main

import "os"
import "fmt"
import "mapreduce"
import "container/list"
import "strings"
import "strconv"
import "unicode"

// our simplified version of MapReduce does not supply a
// key to the Map function, as in the paper; only a value,
// which is a part of the input file contents
func Map(value string) *list.List {

	fs := strings.FieldsFunc(value, func(r rune) bool {
		return !unicode.IsLetter(r)
	})
	var l = new(list.List)
	counts := make(map[string]int)
	for _, wd := range fs {
		counts[wd]++
	}
	for k, v := range counts {
		l.PushBack(mapreduce.KeyValue{Key: k, Value: strconv.Itoa(v)})
	}
	return l
}

// iterate over list and add values
func Reduce(key string, values *list.List) string {
	total := 0
	for v := values.Front(); v != nil; v = v.Next() {
		i, e := strconv.Atoi(v.Value.(string))
		if e != nil {
			fmt.Println(e.Error())
		}
		total += i
	}
	return fmt.Sprint(total)
}

// Can be run in 3 ways:
// 1) Sequential (e.g., go run wc.go master x.txt sequential)
// 2) Master (e.g., go run wc.go master x.txt localhost:7777)
// 3) Worker (e.g., go run wc.go worker localhost:7777 localhost:7778 &)
func main() {
	if len(os.Args) != 4 {
		fmt.Printf("%s: see usage comments in file\n", os.Args[0])
	} else if os.Args[1] == "master" {
		if os.Args[3] == "sequential" {
			mapreduce.RunSingle(5, 3, os.Args[2], Map, Reduce)
		} else {
			mr := mapreduce.MakeMapReduce(5, 3, os.Args[2], os.Args[3])
			// Wait until MR is done
			<-mr.DoneChannel
		}
	} else {
		mapreduce.RunWorker(os.Args[2], os.Args[3], Map, Reduce, 100)
	}
}
