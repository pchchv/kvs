package kvs

import (
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

// Closes the key-value store file
func (kvs *Store) Close() error {
	return kvs.db.Close()
}
