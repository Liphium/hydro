package bridge

import (
	"io"
	"log"
	"os"
	"sync"
)

var (
	logger     *log.Logger
	logOutput  io.Writer
	outputLock sync.Mutex
	stdoutPipe *io.PipeWriter
	pipeReader *io.PipeReader
	copyDone   chan struct{}
)

func init() {
	// Start with discarded output
	logOutput = io.Discard
	logger = log.New(logOutput, "[bridge] ", log.LstdFlags)
}

// ConnectLogs enables bridge logging to stdout
func ConnectLogs() {
	outputLock.Lock()
	defer outputLock.Unlock()

	if stdoutPipe != nil {
		return // Already connected
	}

	pipeReader, stdoutPipe = io.Pipe()
	logger.SetOutput(stdoutPipe)
	copyDone = make(chan struct{})

	go func() {
		io.Copy(os.Stdout, pipeReader)
		close(copyDone)
	}()
}

// DisconnectLogs disables bridge logging to stdout
func DisconnectLogs() {
	outputLock.Lock()
	defer outputLock.Unlock()

	if stdoutPipe == nil {
		return // Already disconnected
	}

	stdoutPipe.Close()
	<-copyDone // Wait for copy to finish
	pipeReader.Close()

	stdoutPipe = nil
	pipeReader = nil
	logger.SetOutput(io.Discard)
}
