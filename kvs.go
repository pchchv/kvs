package kvs

import "github.com/boltdb/bolt"

type Store struct {
	db *bolt.DB
}
