package stamper

import "github.com/timothygk/stamper/internal/assert"

type LogEntry struct {
	LogId     uint64
	ClientId  uint64
	RequestId uint64
	Body      []byte
}

type LogHooks interface {
	OnInit()
	OnAppend(*LogEntry)
}

type Logs struct {
	logs  []LogEntry
	hooks LogHooks
}

func newLogs(hooks LogHooks) *Logs {
	sl := &Logs{hooks: hooks}
	sl.init(1000)
	return sl
}

func (sl *Logs) init(capacity int) {
	sl.logs = make([]LogEntry, 0, capacity)
	if sl.hooks != nil {
		sl.hooks.OnInit()
	}
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
	sl.logs = append(sl.logs, entry)
	if sl.hooks != nil {
		sl.hooks.OnAppend(&entry)
	}
}

func (sl *Logs) Replace(newLogs []RequestLog) {
	// reinit & reappend
	sl.init(len(newLogs))
	for i := range newLogs {
		sl.Append(LogEntry{
			ClientId:  newLogs[i].ClientId,
			RequestId: newLogs[i].RequestId,
			LogId:     newLogs[i].LogId,
			Body:      newLogs[i].Body,
		})
	}
}

func (sl *Logs) CopyLogs() []RequestLog {
	result := make([]RequestLog, len(sl.logs))
	for i := range sl.logs {
		result[i].ClientId = sl.logs[i].ClientId
		result[i].RequestId = sl.logs[i].RequestId
		result[i].LogId = sl.logs[i].LogId
		result[i].Body = sl.logs[i].Body
	}
	return result
}
