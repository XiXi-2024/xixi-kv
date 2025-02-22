package utils

import (
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestAvailableDiskSize(t *testing.T) {
	dir, _ := os.MkdirTemp("", "datafile")
	size, err := AvailableDiskSize(dir)
	t.Log(size/1024/1024/1024, "G")
	assert.Nil(t, err)
	assert.True(t, size > 0)
}
