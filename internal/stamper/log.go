package stamper

import (
	"bytes"
	"crypto/sha256"
	"fmt"

	"github.com/timothygk/stamper/internal/assert"
)

type LogEntry struct {
	LogId     uint64
	ClientId  uint64
	RequestId uint64
	Body      []byte
	PHash     []byte
}

type Logs struct {
	logs []LogEntry
}

func newLogs() *Logs {
	sl := &Logs{}
	sl.logs = make([]LogEntry, 0, 1000)
	return sl
}

func (sl *Logs) LastLog() *LogEntry {
	if len(sl.logs) == 0 {
		return nil
	}
	return sl.At(uint64(len(sl.logs)))
}

func (sl *Logs) At(logId uint64) *LogEntry {
	assert.Assertf(logId > 0, "logId must be positive, found: %d", logId)
	index := int(logId) - 1
	assert.Assertf(sl.logs[index].LogId == logId, "Mismatch logId, expected:%d found:%d", logId, sl.logs[index].LogId)
	return &sl.logs[index]
}

func (sl *Logs) Append(entry LogEntry) {
	assert.Assertf(
		uint64(len(sl.logs))+1 == entry.LogId,
		"Next log id should match len + 1, found len:%d logId:%d",
		len(sl.logs),
		entry.LogId,
	)
	// TODO: make this hashing logic backward compatible in case there's changes?
	hasher := sha256.New()
	if len(sl.logs) > 0 {
		hasher.Write(sl.logs[len(sl.logs)-1].PHash)
	}
	fmt.Fprintf(hasher, " %d %d %d %x", entry.LogId, entry.ClientId, entry.RequestId, entry.Body)
	entry.PHash = hasher.Sum(nil)
	sl.logs = append(sl.logs, entry)
}

func (sl *Logs) Replace(newLogs []RequestLog) {
	if len(newLogs) == 0 {
		return
	}

	// assumption: newLogs is ordered by its log id
	sl.TruncateFrom(newLogs[0].LogId)

	// reinit & reappend
	for i := range newLogs {
		assert.Assertf(newLogs[i].LogId == uint64(len(sl.logs))+1, "Expected sequential, found logId:%d len:%d", newLogs[i].LogId, len(sl.logs))
		sl.Append(LogEntry{
			ClientId:  newLogs[i].ClientId,
			RequestId: newLogs[i].RequestId,
			LogId:     newLogs[i].LogId,
			Body:      bytes.Clone(newLogs[i].Body),
		})
	}
}

func (sl *Logs) TruncateFrom(logId uint64) {
	// logs >= logId will be removed
	index := int(logId) - 1
	// truncate index..last
	if index < len(sl.logs) {
		sl.logs = sl.logs[:index]
	}
}

func (sl *Logs) Iterate(fromLogId uint64, toLogId uint64, handler func(entry *LogEntry)) {
	for logId := fromLogId; logId <= toLogId; logId++ {
		handler(sl.At(logId))
	}
}

func (sl *Logs) CopyLogs(fromLogId, toLogId uint64) []RequestLog {
	result := make([]RequestLog, 0, int(toLogId+1-fromLogId))
	sl.Iterate(fromLogId, toLogId, func(entry *LogEntry) {
		result = append(result, RequestLog{
			ClientId:  entry.ClientId,
			RequestId: entry.RequestId,
			LogId:     entry.LogId,
			Body:      entry.Body,
		})
	})
	return result
}
