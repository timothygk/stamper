package stamper

import (
	"bytes"
	"encoding/json"
	"io"

	"github.com/timothygk/stamper/internal/assert"
)

type JsonEncoderDecoder struct{}

func (JsonEncoderDecoder) Encode(w io.Writer, msg *Envelope) error {
	assert.Assert(msg != nil, "Should not have nil msg")
	return json.NewEncoder(w).Encode(struct {
		Cmd        CmdType `json:"cmd"`
		FromNodeId int     `json:"from_node_id"`
		Payload    any     `json:"payload"`
	}{
		Cmd:        msg.Cmd,
		FromNodeId: msg.FromNodeId,
		Payload:    msg.Payload,
	})
}

func (JsonEncoderDecoder) Decode(r io.Reader) (*Envelope, error) {
	msg := struct {
		Cmd        CmdType         `json:"cmd"`
		FromNodeId int             `json:"from_node_id"`
		Payload    json.RawMessage `json:"payload"`
	}{}

	err := json.NewDecoder(r).Decode(&msg)
	if err != nil {
		return nil, err
	}

	envelope := &Envelope{
		Cmd:        msg.Cmd,
		FromNodeId: msg.FromNodeId,
	}

	switch msg.Cmd {
	case CmdTypeRequest:
		envelope.Payload = &Request{}
	case CmdTypePrepare:
		envelope.Payload = &Prepare{}
	case CmdTypePrepareOk:
		envelope.Payload = &PrepareOk{}
	case CmdTypeReply:
		envelope.Payload = &Reply{}
	case CmdTypeCommit:
		envelope.Payload = &Commit{}
	case CmdTypeStartViewChange:
		envelope.Payload = &StartViewChange{}
	case CmdTypeDoViewChange:
		envelope.Payload = &DoViewChange{}
	case CmdTypeStartView:
		envelope.Payload = &StartView{}
	case CmdTypeRecovery:
		envelope.Payload = &Recovery{}
	case CmdTypeRecoveryResponse:
		envelope.Payload = &RecoveryResponse{}
	case CmdTypeGetState:
		envelope.Payload = &GetState{}
	case CmdTypeNewState:
		envelope.Payload = &NewState{}
	default:
		return nil, ErrUnknownCmdType
	}

	err = json.Unmarshal(msg.Payload, envelope.Payload)
	return envelope, err
}

func (e *Envelope) JsonStr() string {
	if e == nil {
		return "<nil>"
	}

	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetIndent("", "")
	enc.Encode(map[string]any{
		"Cmd":        e.Cmd.String(),
		"FromNodeId": e.FromNodeId,
		"Payload":    e.Payload,
	})
	buf.Truncate(buf.Len() - 1)
	return buf.String()
}
