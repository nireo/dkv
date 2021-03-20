package replica

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"time"

	"github.com/nireo/dkv/db"
)

// Next represents the response body from the next replication key route
type Next struct {
	Key   string
	Value string
}

type replicationQueue struct {
	db         *db.DB
	masterAddr string
}

// Loop retrieves new keys from the master and adds them
func Loop(db *db.DB, masterAddr string) {
	c := &replicationQueue{db: db, masterAddr: masterAddr}
	for {
		curr, err := c.loop()
		if err != nil {
			log.Printf("Loop error: %v", err)
			time.Sleep(time.Second)
			continue
		}

		if !curr {
			time.Sleep(time.Millisecond * 100)
		}
	}
}

// loops over the replication keys and adds to the replication bucket while
// deleting the keys from the replication queue.
func (r *replicationQueue) loop() (curr bool, err error) {
	resp, err := http.Get("http://" + r.masterAddr + "/next")
	if err != nil {
		return false, err
	}

	var res Next
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return false, err
	}
	defer resp.Body.Close()

	if res.Key == "" {
		return false, nil
	}

	if err := r.db.SetOnReplica(res.Key, []byte(res.Value)); err != nil {
		return false, err
	}

	if err := r.deleteFromQueue(res.Key, res.Value); err != nil {
		log.Printf("could not delete from queue")
	}

	return true, nil
}

// deleteFromReplicationQueue takes in a key-value pair and removes it from the queue
// we need the value to be correct such that the replication value is not stale.
func (r *replicationQueue) deleteFromQueue(key, value string) error {
	u := url.Values{}
	u.Set("key", key)
	u.Set("value", value)
	log.Printf("deleting %q", key)

	resp, err := http.Get("http://" + r.masterAddr + "/del-rep?" + u.Encode())
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent {
		log.Printf("error deleting key from replication queue")
		return fmt.Errorf("got wrong status code. want=%d got=%d",
			http.StatusNoContent, resp.StatusCode)
	}

	return nil
}
