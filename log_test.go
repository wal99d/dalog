package dalog

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLog(t *testing.T) {
	//create a file
	f, err := os.CreateTemp(".", "tempfile_***")
	require.NoError(t, err)
	//bytes to be written
	data := []byte("hello wolrd")
	width := len(data) + RecordLength

	//append them to temp file
	store, err := NewStore(f)
	if err != nil {
		panic(err)
	}
	numberOfBytesWritten, _, _ := store.Append(data)
	require.Equal(t, uint64(width), numberOfBytesWritten)
	readData, _ := store.Read(0)

	require.Equal(t, string(data), string(readData))
	store.Close()

}
