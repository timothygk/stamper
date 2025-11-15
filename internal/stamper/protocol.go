package stamper

import (
	"errors"
	"io"
)

var (
	ErrUnknownCmdType = errors.New("Unknown command type")
)

// EncoderDecoder the encode and decode interface allowing different formats
type EncoderDecoder interface {
	Encode(w io.Writer, msg *Envelope) error
	Decode(r io.Reader) (*Envelope, error)
}

type ProtocolError int

const (
	ProtocolErrorNone             ProtocolError = 0
	ProtocolErrorReplicaNotReady  ProtocolError = 1
	ProtocolErrorNotPrimaryNode   ProtocolError = 2
	ProtocolErrorExpiredRequestId ProtocolError = 3
)

// Error implements error interface
func (e ProtocolError) Error() string {
	switch e {
	case ProtocolErrorReplicaNotReady:
		return "ReplicaNotReady"
	case ProtocolErrorNotPrimaryNode:
		return "NotPrimaryNode"
	case ProtocolErrorExpiredRequestId:
		return "ExpiredRequestId"
	default:
		return "UNKNOWN_ERROR"
	}
}

type CmdType int

const (
	CmdTypeRequest          CmdType = 1
	CmdTypePrepare          CmdType = 2
	CmdTypePrepareOk        CmdType = 3
	CmdTypeReply            CmdType = 4
	CmdTypeCommit           CmdType = 5
	CmdTypeStartViewChange  CmdType = 6
	CmdTypeDoViewChange     CmdType = 7
	CmdTypeStartView        CmdType = 8
	CmdTypeRecovery         CmdType = 9
	CmdTypeRecoveryResponse CmdType = 10
	CmdTypeGetState         CmdType = 11
	CmdTypeNewState         CmdType = 12
)

func (c CmdType) String() string {
	switch c {
	case CmdTypeRequest:
		return "Request"
	case CmdTypePrepare:
		return "Prepare"
	case CmdTypePrepareOk:
		return "PrepareOk"
	case CmdTypeReply:
		return "Reply"
	case CmdTypeCommit:
		return "Commit"
	case CmdTypeStartViewChange:
		return "StartViewChange"
	case CmdTypeDoViewChange:
		return "DoViewChange"
	case CmdTypeStartView:
		return "StartView"
	case CmdTypeRecovery:
		return "Recovery"
	case CmdTypeRecoveryResponse:
		return "RecoveryResponse"
	case CmdTypeGetState:
		return "GetState"
	case CmdTypeNewState:
		return "NewState"
	default:
		return "UNKNOWN"
	}
}

type Request struct {
	ClientId    uint64
	RequestId   uint64 // request-number from client
	RequestBody []byte // the op
}

type Prepare struct {
	ViewId        uint64   // view-number
	LogId         uint64   // op-number assigned by primary node
	CommitId      uint64   // primary node commit-number
	ClientRequest *Request // the request from client
}

type PrepareOk struct {
	ViewId uint64 // view-number
	LogId  uint64 // op-number that's been ack-ed by the backup replica
	NodeId int    // node-id of the backup replica
}

type Reply struct {
	ViewId       uint64 // view-number
	RequestId    uint64 // request-number from client
	ResponseBody []byte // result of op upcall
	Error        ProtocolError
}

type Commit struct {
	ViewId   uint64 // view-number
	CommitId uint64 // commit-number from the primary node
}

type StartViewChange struct {
	ViewId   uint64 // view-number
	NodeId   int    // node-id of the backup replica
	CommitId uint64 // node commit-id
}

type RequestLog struct {
	ClientId  uint64
	RequestId uint64 // request-number from client
	LogId     uint64 // op-number
	Body      []byte // the op to be upcall-ed
}

type DoViewChange struct {
	ViewId           uint64       // view-number
	Logs             []RequestLog // current replica logs
	LastNormalViewId uint64       // the latest view-number where node status is normal
	LastLogId        uint64       // op-number of the replica
	CommitId         uint64       // commit-number of the replica
	NodeId           int          // node-id of the replica
}

type StartView struct {
	ViewId    uint64       // view-number
	Logs      []RequestLog // the new logs
	LastLogId uint64       // op-number
	CommitId  uint64       // commit-number
}

type Recovery struct {
	NodeId int    // node-id of the replica
	Nonce  uint64 // the nonce
}

type RecoveryResponse struct {
	ViewId     uint64       // view-number
	Nonce      uint64       // the nonce
	Logs       []RequestLog // the new logs
	LastLogId  uint64       // op-number, empty if it is not from the primary node
	CommitId   uint64       // commit-number, empty if it is not from the primary node
	FromNodeId int          // source node-id
}

type GetState struct {
	ViewId    uint64 // view-number
	LastLogId uint64 // op-number of the requester
}

type NewState struct {
	ViewId    uint64       // view-number
	Logs      []RequestLog // the new logs
	LastLogId uint64       // op-number
	CommitId  uint64       // commit-number
}

// Envelope the envelope structure used for communications
type Envelope struct {
	Cmd        CmdType
	FromNodeId int
	Payload    any
}
