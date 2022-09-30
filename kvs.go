package kvs

import (
	"bytes"
	"encoding/gob"
	"errors"
	"time"

	"github.com/boltdb/bolt"
)

// Key value store
// Use the Open() method to create one, and Close() it when done
type Store struct {
	// TODO: Remove dependence on boltDB. Implement all necessary functionality
	db *bolt.DB
}

// Open a key-value store
// "path" is the full path to the database file, any leading directories must have been created already
// File is created with mode 0640 if needed
// Because of BoltDB restrictions, only one process may open the file at a time
// Attempts to open the file from another process will fail with a timeout error
func Open(path string) (*Store, error) {
	opts := &bolt.Options{
		Timeout: 75 * time.Millisecond,
	}
	if db, err := bolt.Open(path, 0640, opts); err != nil {
		return nil, err
	} else {
		err := db.Update(func(tx *bolt.Tx) error {
			_, err := tx.CreateBucketIfNotExists([]byte("kv"))
			return err
		})
		if err != nil {
			return nil, err
		} else {
			return &Store{db: db}, nil
		}
	}
}

// Put the entry in the storage
// The passed value is gob-encoded and stored
// The key can be an empty string, but the value cannot be nil - if it is, an error is returned
func (kvs *Store) Put(key string, value interface{}) error {
	if value == nil {
		return errors.New("Bad value")
	}
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(value); err != nil {
		return err
	}
	return kvs.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte("kv")).Put([]byte(key), buf.Bytes())
	})
}

// Get the entry from the store
// "value" must be a pointer-typed
// If the key is missing in the store, Get returns an error
// The value passed to Get() can be nil,
// in which case any value read from the store is silently discarded
func (kvs *Store) Get(key string, value interface{}) error {
	return kvs.db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket([]byte("kv")).Cursor()
		if k, v := c.Seek([]byte(key)); k == nil || string(k) != key {
			return errors.New("Key not found")
		} else if value == nil {
			return nil
		} else {
			d := gob.NewDecoder(bytes.NewReader(v))
			return d.Decode(value)
		}
	})
}

// Delete the entry with the given key
// If no such key is present in the store, it returns error
func (kvs *Store) Delete(key string) error {
	return kvs.db.Update(func(tx *bolt.Tx) error {
		c := tx.Bucket([]byte("kv")).Cursor()
		if k, _ := c.Seek([]byte(key)); k == nil || string(k) != key {
			return errors.New("Key not found")
		} else {
			return c.Delete()
		}
	})
}

// Closes the key-value store file
func (kvs *Store) Close() error {
	return kvs.db.Close()
}
