package config

type Shard struct {
	Index   int    `json:"index"`
	Name    string `json:"name"`
	Address string `json:"address"`
}

type Config struct {
	Shards []Shard `json:"shards"`
}
