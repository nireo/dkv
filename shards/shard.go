package shards

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
)

// Shard represents the config entry for a single shard
type Shard struct {
	Index   int    `json:"index"`
	Name    string `json:"name"`
	Address string `json:"address"`
}

// Config represents the data inside of the shard config file with multiple shard entries
type Config struct {
	Shards []Shard `json:"shards"`
}

// Shards represents the configuration of a server, but it also includes the amount of shards
// and the address listings.
type Shards struct {
	Amount    int
	Index     int
	Addresses map[int]string
}

// ParseConfigFile opens the shards file and parses the json information into the Config struct.
func ParseConfigFile(path string) (*Config, error) {
	file, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("error opening file, err: %s", err)
	}

	var conf Config
	if err := json.Unmarshal(file, &conf); err != nil {
		return nil, fmt.Errorf("error unmarshalling json, err: %s", err)
	}

	return &conf, nil
}

// ParseConfigShards returns a Shards struct from the config given a shard name
func (c *Config) ParseConfigShards(shardName string) (*Shards, error) {
	index := -1
	addresses := make(map[int]string)

	for _, s := range c.Shards {
		if _, ok := addresses[s.Index]; ok {
			return nil, fmt.Errorf("duplicated shards index: %d", s.Index)
		}

		addresses[s.Index] = s.Address
		if s.Name == shardName {
			index = s.Index
		}
	}

	for i := 0; i < len(c.Shards); i++ {
		if _, ok := addresses[i]; !ok {
			return nil, fmt.Errorf("shards with index %d was not found", i)
		}
	}

	if index < 0 {
		return nil, fmt.Errorf("shards %s was not found", shardName)
	}

	return &Shards{
		Addresses: addresses,
		Amount:    len(c.Shards),
		Index:     index,
	}, nil
}

// GetShardIndex is the sharding function which desides in which the shard the key-value
// should go into
func (s *Shards) GetShardIndex(key string) int {
	h := fnv.New64()
	h.Write([]byte(key))

	return int(h.Sum64() % uint64(s.Amount))
}
