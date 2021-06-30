package vfilter

import (
	"context"
	"io"
	"log"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"www.velocidex.com/golang/vfilter/utils"
)

type logWriter struct {
	io.Writer
	logs []string
}

func (self *logWriter) Write(b []byte) (int, error) {
	self.logs = append(self.logs, string(b))
	return self.Writer.Write(b)
}

func (self *logWriter) Contains(t *testing.T, member string) {
	for _, line := range self.logs {
		if strings.Contains(line, member) {
			return
		}
	}

	assert.Fail(t, member)
}

func (self *logWriter) NotContains(t *testing.T, member string) {
	for _, line := range self.logs {
		if strings.Contains(line, member) {
			assert.Fail(t, member)
		}
	}
}

func TestLogging(t *testing.T) {
	scope := makeTestScope()
	logger := &logWriter{Writer: os.Stdout}
	scope.SetLogger(log.New(logger, "Log: ", log.Ldate|log.Ltime|log.Lshortfile))

	vql, err := Parse("SELECT X, X.Foo, Y, Y.Foo FROM foreach(row=[dict(X=1),])")
	assert.NoError(t, err)

	ctx := context.Background()
	for row := range vql.Eval(ctx, scope) {
		utils.Debug(row)
	}

	logger.Contains(t, "Symbol Y not found")
	logger.Contains(t, "While resolving Y.Foo Symbol Y not found")

	logger.NotContains(t, "Symbol X not found")
	logger.NotContains(t, "While resolving X.Foo Symbol X not found")
}
