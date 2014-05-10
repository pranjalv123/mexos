package main
import "github.com/jmhodges/levigo"
import "fmt"
import "math/rand"
import "time"
func trace(s string) (string, time.Time) {
    fmt.Println("START:", s)
    return s, time.Now()
}

func un(s string, startTime time.Time) {
    endTime := time.Now()
    fmt.Println("  END:", s, "ElapsedTime in seconds:", endTime.Sub(startTime))
}
func db_create() (*levigo.DB) {
	defer un(trace("db create"))

	opts := levigo.NewOptions()
	opts.SetCache(levigo.NewLRUCache(3<<30))
	opts.SetCreateIfMissing(true)
	db, _ := levigo.Open("/tmp/leveldb", opts)
	ro := levigo.NewReadOptions()
	wo := levigo.NewWriteOptions()
	db.Put(wo, []byte("key1"), []byte("val1"))
	data, _ := db.Get(ro, []byte("key1"))
	fmt.Println(string(data));
	return db
}

func gen_data(size uint64) ([]byte) {
	defer un(trace("gen data"))
	
	data := make([]byte, size)
//	src := new(rand.Rand)
	for i := range data {
		data[i] = byte(rand.Int31() & 0xff)
	}
	return data
}

func write_data(data []byte, db *levigo.DB, nitems int, keysize int, valsize int) {
	defer un(trace("write data"))
	wo := levigo.NewWriteOptions()	
	keystart := 0
	valstart := 0
	for i:=0; i < nitems; i++ {
		db.Put(wo, 
			data[keystart:keystart + keysize], 
			data[valstart:valstart + valsize])
		keystart %= (len(data) - keysize)
		valstart %= (len(data) - valsize)
	}
	fmt.Println("Wrote ", nitems, "items with key size", keysize, "and value size", valsize)
}

func main() {
	db := db_create()
	data := gen_data(1 << 24)
	write_data(data, db, 10000000, 16, 256)
}
