package kvs

import (
	"bytes"
	"encoding/json"
	"os"
	"testing"
)

type aStruct struct {
	Numbers *[]int
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
	os.Remove("skv-test.db")
	db, err := Open("skv-test.db")
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
