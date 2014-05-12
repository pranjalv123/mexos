package main

import "fmt"
import "net"
import "net/rpc"
import "log"
import "strconv"
import "io"
import "bufio"
import "github.com/jmhodges/levigo"
import "encoding/binary"
import "bytes"
import "encoding/gob"
import "math/rand"

const port = 3222
const fill = false
var dbReadOptions  *levigo.ReadOptions
var dbWriteOptions *levigo.WriteOptions
var dbOpts         *levigo.Options
var db             *levigo.DB

type Args struct {}
type Reply struct {
	Port int
}

type Blank struct{}

func call(srv string, rpcname string, args interface{},
        reply interface{}) bool {
        fmt.Printf("sending tpc rpc to %s\n", srv)
        c, errx := rpc.Dial("tcp", srv)
        if errx != nil {
                fmt.Printf("there a connection error!")
                return false
        }
        fmt.Printf("connection opened!")
        defer c.Close()
	
        err := c.Call(rpcname, args, reply)
	if err == nil {
		fmt.Printf("call succeeded")
		return true
	}
        fmt.Printf("call failed")
        return false
}

func (bl *Blank) Xfer(args *Args, reply *Reply) error {
	fmt.Printf("Xfer called\n")
        reply.Port = port
	
	go func() {
		l, e := net.Listen("tcp", ":"+strconv.Itoa(port))
		if e != nil {
			log.Fatal("listen error: ", e)
		}

		c, err := l.Accept()
		if err != nil {
			log.Fatal("Could not accept connections!")
		}
		fmt.Println("listening\n")

		defer l.Close()
		
		finished := false
		
		reader := bufio.NewReader(c)
		buffer := make([]byte, 1024)

		log.Printf("Starting recovery\n")
	readloop:
		for !finished {
			command, err := reader.ReadByte()
			if err != nil {
				fmt.Println("Recovery read error")
				c.Close()
				break readloop
			}
			switch command {
			case 0 : // done
				c.Close()
				finished = true
			case 1 : // key/value pair
				keylen, _ := binary.ReadVarint(reader)
				if int64(len(buffer)) < keylen {
					buffer = make([]byte, keylen)
				}
				io.ReadFull(reader, buffer[:keylen])
				key := buffer[:keylen]
				
				vallen, _ := binary.ReadVarint(reader)
				if int64(len(buffer)) < vallen {
					buffer = make([]byte, vallen)
				}
				_, err := io.ReadFull(reader, buffer[:vallen])
				val := buffer[:vallen]
				
				if (err != nil) {
					fmt.Println("Recovery read error")
					c.Close()
					break readloop
					// this goes back to resuming recovery
				}
				err2 := db.Put(dbWriteOptions, key, val)
				if err2 != nil {
					fmt.Printf("error writing to database\n")
				} 
			}
		}
		log.Printf("Ended recovery\n")
		
		
	}()
	
        return nil
}


func main() {
	//open database
	openDB()

	if fill == true {
		//fill database
		nItems := 300000 //makes a db of 1.15GB
		keySize := 32
		valSize := 4096
		for i := 0; i < nItems; i++ {
			k := paddedRandIntString(keySize)
			v:= paddedRandIntString(valSize)
			var buffer bytes.Buffer
			enc := gob.NewEncoder(&buffer)
			enc.Encode(v)
			
			err := db.Put(dbWriteOptions, []byte(k), buffer.Bytes())
			if err != nil {
				fmt.Printf("error writing to database\n")
			} 
		}

		//then try to send it all
		args := &Args{}
		var reply Reply
		ok := false
		for !ok {
			ok = call("10.0.0.101:3333", "Xfer", args, reply)
		}
		fmt.Println("Sending data to 10.0.0.101:%d\n",reply.Port)
		go func () {
			conn, err := net.Dial("tcp", "10.0.0.101:"+strconv.Itoa(reply.Port))
			if err != nil {
				log.Printf("Couldn't connect to recovering servar %s",
					"10.0.0.101:"+strconv.Itoa(reply.Port))
				return
			}
			writer := bufio.NewWriter(conn)
			
			iterator := db.NewIterator(dbReadOptions)
			iterator.SeekToFirst()
			
			intbuf := make([]byte, 16)
			
			for iterator.Valid() {
				keyBytes := iterator.Key()
				//key := string(keyBytes)
				
				writer.WriteByte(1)
				
				n := binary.PutVarint(intbuf, int64(len(keyBytes)))
				writer.Write(intbuf[:n])
				writer.Write(keyBytes)
				
				valBytes := iterator.Value()
				n = binary.PutVarint(intbuf, int64(len(valBytes)))
				writer.Write(intbuf[:n])
				_, err := writer.Write(valBytes)
				
				if err != nil {
					log.Printf("Some writer error!")
					conn.Close()
					// keep lock held for recovery reattempt
					return
				}
				iterator.Next()
			}
			
			writer.WriteByte(0)
			writer.Flush()
			conn.Close()
		}()
	} else {
		bl := new(Blank)
		rpcs := rpc.NewServer()
		rpcs.Register(bl)
		l, e := net.Listen("tcp", ":3333")
		if e != nil {
			log.Fatal("listen error: ", e)
		}

		go rpcs.Accept(l)
		fmt.Println("accept thhread reunning")
		select {}
	}
}

func openDB() {
	dbOpts := levigo.NewOptions()
	dbOpts.SetCompression(levigo.NoCompression)
	dbOpts.SetCreateIfMissing(true)
	dbName := "testDB"
	// Open database (create it if it doesn't exist)
	var err error
	db, err = levigo.Open(dbName, dbOpts)
	if err != nil {
		fmt.Printf("Error opening database: %v\n", err)
	} else {
		fmt.Printf(" Database opened successfully\n")
	}
	// Create options for reading/writing entries
	dbReadOptions = levigo.NewReadOptions()
	dbWriteOptions = levigo.NewWriteOptions()
	dbReadOptions.SetFillCache(true)

}

func paddedRandIntString(size int) string {
	data := make([]byte, size)
	s := strconv.Itoa(rand.Int())
	for i := range data {
		data[i] = s[i % len(s)]
	}
	return string(data[:])
}
