package shards

import (
	"reflect"
	"testing"
)

func TestShardFileParsing(t *testing.T) {
	conf, err := ParseConfigFile("./test_config.json")
	if err != nil {
		t.Errorf("error parsing shards, err: %s", err)
	}

	testCases := []struct {
		name    string
		index   int
		address string
	}{
		{"sh1", 0, "localhost:8080"},
		{"sh2", 1, "localhost:8081"},
	}

	for i, tc := range testCases {
		if tc.name != conf.Shards[i].Name {
			t.Errorf("names are not equal. got=%s, want=%s", conf.Shards[i].Name, tc.name)
		}

		if tc.index != conf.Shards[i].Index {
			t.Errorf("indices different. got=%d want=%d", conf.Shards[i].Index, tc.index)
		}

		if tc.address != conf.Shards[i].Address {
			t.Errorf("addresses differ. got=%s want=%s", conf.Shards[i].Address, tc.address)
		}
	}
}

func TestShardParsing(t *testing.T) {
	conf, err := ParseConfigFile("./test_config.json")
	if err != nil {
		t.Errorf("error parsing shards, err: %s", err)
	}

	got, err := conf.ParseConfigShards("sh1")
	if err != nil {
		t.Errorf("could not parse shards shards: %s", err)
	}

	want := &Shards{
		Amount: 2,
		Index:  0,
		Addresses: map[int]string{
			0: "localhost:8080",
			1: "localhost:8081",
		},
	}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("shards doesn't match. got=%v want=%v", got, want)
	}
}
