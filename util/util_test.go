package util

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestConvertSize(t *testing.T) {

	size, sizeGB, err := ConvertSize("0b")
	if err != nil {
		t.Fatalf("Couldn't parse zero. Error: %v", err)
	}

	if size != "0" {
		t.Fatalf("Size is: %v. Expected 0", size)
	}

	if sizeGB != "0" {
		t.Fatalf("SizeGB is: %v. Expected 0", sizeGB)
	}

	size, sizeGB, err = ConvertSize("1024b")
	if err != nil {
		t.Fatalf("Couldn't parse zero. Error: %v", err)
	}

	if size != "1024" {
		t.Fatalf("Size is: %v. Expected 0", size)
	}

	if sizeGB != "1" {
		t.Fatalf("SizeGB is: %v. Expected 1", sizeGB)
	}

	size, sizeGB, err = ConvertSize("1024")
	if size != "1024" {
		t.Fatalf("Size is: %v. Expected 0", size)
	}

	if sizeGB != "1" {
		t.Fatalf("SizeGB is: %v. Expected 1", sizeGB)
	}
}

func TestReplicaName(t *testing.T) {
	assert := require.New(t)

	assert.Equal("replica-XX", ReplicaName("tcp://replica-XX:9502", "tt"))
	assert.Equal("replica-XX", ReplicaName("tcp://replica-XX.volume-tt:9502", "tt"))
}
