package main
import "github.com/jmhodges/levigo"
import "fmt"

func main() {
	opts := levigo.NewOptions()
	opts.SetCache(levigo.NewLRUCache(3<<30))
	opts.SetCreateIfMissing(true)
	db, _ := levigo.Open("/tmp/leveldb", opts)
	ro := levigo.NewReadOptions()
	wo := levigo.NewWriteOptions()
	db.Put(wo, []byte("key1"), []byte("val1"))
	data, _ := db.Get(ro, []byte("key1"))
	fmt.Println(string(data));

}
