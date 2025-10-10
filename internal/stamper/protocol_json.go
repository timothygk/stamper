package stamper

import (
	"bytes"
	"encoding/json"
	"io"
)

type JsonEncoderDecoder struct{}

func (JsonEncoderDecoder) Encode(w io.Writer, msg *Envelope) error {
	if msg == nil {
		return json.NewEncoder(w).Encode(nil)
	}
	return json.NewEncoder(w).Encode(struct {
		Cmd        CmdType `json:"cmd"`
		EnvelopeId uint64  `json:"envelope_id"`
		Payload    any     `json:"payload"`
	}{
		Cmd:        msg.Cmd,
		EnvelopeId: msg.EnvelopeId,
		Payload:    msg.Payload,
	})
}

func (JsonEncoderDecoder) Decode(r io.Reader) (*Envelope, error) {
	msg := struct {
		Cmd        CmdType         `json:"cmd"`
		EnvelopeId uint64          `json:"envelope_id"`
		Payload    json.RawMessage `json:"payload"`
	}{}

	err := json.NewDecoder(r).Decode(&msg)
	if err != nil {
		return nil, err
	}

	envelope := &Envelope{
		Cmd:        msg.Cmd,
		EnvelopeId: msg.EnvelopeId,
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
	case CmdTypeAck:
		envelope.Payload = &Ack{}
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
	enc.Encode(map[string]interface{}{
		"Cmd":        e.Cmd.String(),
		"EnvelopeId": e.EnvelopeId,
		"Payload":    e.Payload,
	})
	buf.Truncate(buf.Len() - 1)
	return buf.String()
}
