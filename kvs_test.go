package kvs

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"
)

type aStruct struct {
	Numbers *[]int
}

func TestBasic(t *testing.T) {
	os.Remove("kvs-test.db")
	db, err := Open("svs-test.db")
	if err != nil {
		t.Fatal(err)
	}
	// put a key
	if err := db.Put("key1", "value1"); err != nil {
		t.Fatal(err)
	}
	// get it back
	var val string
	if err := db.Get("key1", &val); err != nil {
		t.Fatal(err)
	} else if val != "value1" {
		t.Fatalf("got \"%s\", expected \"value1\"", val)
	}
	// put it again with same value
	if err := db.Put("key1", "value1"); err != nil {
		t.Fatal(err)
	}
	// get it back again
	if err := db.Get("key1", &val); err != nil {
		t.Fatal(err)
	} else if val != "value1" {
		t.Fatalf("got \"%s\", expected \"value1\"", val)
	}
	// get something we know is not there
	if err := db.Get("no.such.key", &val); fmt.Sprintf("%v", err) != "Key not found" {
		t.Fatalf("got \"%s\", expected absence", val)
	}
	// delete our key
	if err := db.Delete("key1"); err != nil {
		t.Fatal(err)
	}
	// delete it again
	if err := db.Delete("key1"); fmt.Sprintf("%v", err) != "Key not found" {
		t.Fatalf("delete returned %v, expected not found error", err)
	}
	if err := db.Get("key1", &val); fmt.Sprintf("%v", err) != "Key not found" {
		t.Fatal(err)
	}
	if err := db.Put("key1", "value1"); err != nil {
		t.Fatal(err)
	}
	if err := db.Delete("key1"); err != nil {
		t.Fatal(err)
	}
	if err := db.Get("key1", &val); fmt.Sprintf("%v", err) != "Key not found" {
		t.Fatal(err)
	}
	if err := db.Get("", &val); fmt.Sprintf("%v", err) != "Key not found" {
		t.Fatal(err)
	}
	// done
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRichTypes(t *testing.T) {
	var inval1 = map[string]string{
		"100 meters": "Florence GRIFFITH-JOYNER",
		"200 meters": "Florence GRIFFITH-JOYNER",
		"400 meters": "Marie-José PÉREC",
		"800 meters": "Nadezhda OLIZARENKO",
	}
	var outval1 = make(map[string]string)
	testGetPut(t, inval1, &outval1)
	var inval2 = aStruct{
		Numbers: &[]int{100, 200, 400, 800},
	}
	var outval2 aStruct
	testGetPut(t, inval2, &outval2)
}

func testGetPut(t *testing.T, inval interface{}, outval interface{}) {
	os.Remove("kvs-test.db")
	db, err := Open("kvs-test.db")
	if err != nil {
		t.Fatal(err)
	}
	input, err := json.Marshal(inval)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Put("test.key", inval); err != nil {
		t.Fatal(err)
	}
	if err := db.Get("test.key", outval); err != nil {
		t.Fatal(err)
	}
	output, err := json.Marshal(outval)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(input, output) {
		t.Fatal("differences encountered")
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestNil(t *testing.T) {
	os.Remove("kvs-test.db")
	db, err := Open("kvs-test.db")
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Put("key1", nil); fmt.Sprintf("%v", err) != "Bad value" {
		t.Fatalf("got %v, expected Bad Value error", err)
	}
	if err := db.Put("key1", "value1"); err != nil {
		t.Fatal(err)
	}
	// can Get() into a nil value
	if err := db.Get("key1", nil); err != nil {
		t.Fatal(err)
	}
	db.Close()
}

func TestGoroutines(t *testing.T) {
	os.Remove("kvs-test.db")
	db, err := Open("kvs-test.db")
	if err != nil {
		t.Fatal(err)
	}
	rand.Seed(time.Now().UnixNano())
	var wg sync.WaitGroup
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func() {
			switch rand.Intn(3) {
			case 0:
				if err := db.Put("key1", "value1"); err != nil {
					t.Fatal(err)
				}
			case 1:
				var val string
				if err := db.Get("key1", &val); err != nil && fmt.Sprintf("%v", err) != "Key not found" {
					t.Fatal(err)
				}
			case 2:
				if err := db.Delete("key1"); err != nil && fmt.Sprintf("%v", err) != "Key not found" {
					t.Fatal(err)
				}
			}
			wg.Done()
		}()
	}
	wg.Wait()
}

func BenchmarkPut(b *testing.B) {
	os.Remove("kvs-bench.db")
	db, err := Open("kvs-bench.db")
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := db.Put(fmt.Sprintf("key%d", i), "this.is.a.value"); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	db.Close()
}

func BenchmarkPutGet(b *testing.B) {
	os.Remove("kvs-bench.db")
	db, err := Open("kvs-bench.db")
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := db.Put(fmt.Sprintf("key%d", i), "this.is.a.value"); err != nil {
			b.Fatal(err)
		}
	}
	for i := 0; i < b.N; i++ {
		var val string
		if err := db.Get(fmt.Sprintf("key%d", i), &val); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	db.Close()
}

func BenchmarkPutDelete(b *testing.B) {
	os.Remove("kvs-bench.db")
	db, err := Open("kvs-bench.db")
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := db.Put(fmt.Sprintf("key%d", i), "this.is.a.value"); err != nil {
			b.Fatal(err)
		}
	}
	for i := 0; i < b.N; i++ {
		if err := db.Delete(fmt.Sprintf("key%d", i)); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	db.Close()
}
