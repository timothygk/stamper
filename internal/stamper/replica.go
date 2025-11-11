package stamper

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"net"
	"sort"
	"sync"
	"time"

	"github.com/timothygk/stamper/internal/assert"
	"github.com/timothygk/stamper/internal/logging"
	"github.com/timothygk/stamper/internal/timepkg"
)

type replicaStatus int

const (
	ReplicaStatusNormal     replicaStatus = 1
	ReplicaStatusViewChange replicaStatus = 2
	ReplicaStatusRecovering replicaStatus = 3
)

type clientTable struct {
	requestId uint64 // last request-number
	reply     *Reply
}

type eventType int

const (
	eventTypeReplicaClosing  eventType = 0
	eventTypeConnAccepted    eventType = 1
	eventTypeConnClosed      eventType = 2
	eventTypeSend            eventType = 3
	eventTypeHeartbeatCommit eventType = 4
	eventTypeResendPrepares  eventType = 5
	eventTypeViewChangeTimer eventType = 6
	eventTypeInitRecovery    eventType = 7
	eventTypeMsgRecv         eventType = 100
)

func (e eventType) String() string {
	switch e {
	case eventTypeReplicaClosing:
		return "replica_closing"
	case eventTypeConnAccepted:
		return "conn_accepted"
	case eventTypeConnClosed:
		return "conn_closed"
	case eventTypeSend:
		return "send"
	case eventTypeHeartbeatCommit:
		return "heartbeat_commit"
	case eventTypeResendPrepares:
		return "resend_prepares"
	case eventTypeViewChangeTimer:
		return "view_change_timer"
	case eventTypeMsgRecv:
		return "msg_recv"
	case eventTypeInitRecovery:
		return "init_recovery"
	default:
		return "UNKNOWN"
	}
}

type event struct {
	etype        eventType
	envelope     *Envelope
	conn         net.Conn
	targetNodeId int
	commitId     uint64
}

func (e *event) String() string {
	return fmt.Sprintf(
		"etype:%s, envelope:%s, conn:%p, nodeId:%d, commitId:%d",
		e.etype.String(),
		e.envelope.JsonStr(),
		e.conn,
		e.targetNodeId,
		e.commitId,
	)
}

type ReplicaConfig struct {
	SendRetryDuration       time.Duration
	CommitDelayDuration     time.Duration
	ViewChangeDelayDuration time.Duration
	RecoveryRetryDuration   time.Duration
	NodeId                  int
	ServerAddrs             []string // configurations, currently it is static
}

type Replica struct {
	// config
	config ReplicaConfig

	// time
	tt timepkg.Time

	// network/communication related
	encdec      EncoderDecoder
	createConn  func(toAddr string) (net.Conn, error)
	conns       []net.Conn
	serverConns []net.Conn
	clientConns map[uint64]net.Conn // map of client id -> connection
	connCloseWg sync.WaitGroup
	r           *rand.Rand
	events      chan event

	// states
	viewId    uint64
	status    replicaStatus
	lastLogId uint64
	logs      *Logs
	commitId  uint64
	clients   map[uint64]*clientTable
	upcall    func([]byte) []byte

	// internal states
	closing           chan struct{}
	closed            chan struct{}
	waitingPrepares   map[uint64]*Prepare // logId -> prepare request
	lastPreparedLogId []uint64            // node -> last prepared log id
	lastCommitAt      time.Time           // last commit time
	// view-change related
	lastNormalViewId  uint64
	lastChangedViewId []uint64 // last view change id
	viewChangeTimer   timepkg.Timer
	doViewChangeSet   map[uint64]map[int]*DoViewChange // viewId -> {nodeId -> doViewChange}
	// recovery related
	recoveryNonce     uint64
	recoveryResponses []*RecoveryResponse
}

// NewReplica create a new replica
func NewReplica(
	config ReplicaConfig,
	tt timepkg.Time,
	encdec EncoderDecoder,
	createConn func(string) (net.Conn, error),
	r *rand.Rand,
	upcall func([]byte) []byte,
	recovery bool,
) *Replica {
	replica := &Replica{
		config:     config,
		tt:         tt,
		encdec:     encdec,
		createConn: createConn,
		// TODO: better connection management?
		conns:             make([]net.Conn, 0, 10),
		serverConns:       make([]net.Conn, len(config.ServerAddrs)),
		clientConns:       make(map[uint64]net.Conn),
		r:                 r,
		events:            make(chan event, 1000),
		viewId:            0,
		status:            ReplicaStatusNormal,
		lastLogId:         0,
		logs:              newLogs(),
		commitId:          0,
		clients:           make(map[uint64]*clientTable),
		upcall:            upcall,
		closing:           make(chan struct{}),
		closed:            make(chan struct{}),
		waitingPrepares:   make(map[uint64]*Prepare),
		lastPreparedLogId: make([]uint64, len(config.ServerAddrs)),
		lastNormalViewId:  0,
		lastChangedViewId: make([]uint64, len(config.ServerAddrs)),
		doViewChangeSet:   make(map[uint64]map[int]*DoViewChange),
	}
	if recovery {
		replica.status = ReplicaStatusRecovering
		replica.recoveryNonce = r.Uint64()
		replica.recoveryResponses = make([]*RecoveryResponse, len(config.ServerAddrs))
		replica.events <- event{etype: eventTypeInitRecovery}
	}
	go replica.heartbeatLoop()
	go replica.resendPreparesLoop()
	go replica.loop()
	replica.viewChangeTimer = tt.AfterFunc(config.ViewChangeDelayDuration, func() {
		select {
		case <-replica.closing:
		default:
			replica.events <- event{etype: eventTypeViewChangeTimer}
		}
	})
	return replica
}

func (r *Replica) Status() replicaStatus {
	return r.status
}

func (r *Replica) ViewId() uint64 {
	return r.viewId
}

func (r *Replica) CommitId() uint64 {
	return r.commitId
}

func (r *Replica) LastLogId() uint64 {
	return r.lastLogId
}

func (r *Replica) LogHash(logId uint64) []byte {
	if logId == 0 {
		return []byte{}
	}
	return r.logs.At(logId).PHash
}

// Accept accept a network connection
func (r *Replica) Accept(conn net.Conn) {
	// fmt.Printf("Accepted connection %p from %s\n", conn, conn.RemoteAddr().String())
	r.events <- event{
		etype: eventTypeConnAccepted,
		conn:  conn,
	}
}

// Close closes the handler and all its states
func (r *Replica) Close() error {
	logging.Logf("[Node %d] closing...\n", r.config.NodeId)
	close(r.closing)
	r.events <- event{etype: eventTypeReplicaClosing}
	r.connCloseWg.Wait()
	clear(r.clientConns)
	<-r.closed
	logging.Logf("[Node %d] closed...\n", r.config.NodeId)
	return nil
}

func (r *Replica) heartbeatLoop() {
	ticker := r.tt.Tick(r.config.CommitDelayDuration)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.Chan():
			r.events <- event{etype: eventTypeHeartbeatCommit}
		case <-r.closing:
			return
		}
	}
}

func (r *Replica) resendPreparesLoop() {
	ticker := r.tt.Tick(r.config.SendRetryDuration)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.Chan():
			r.events <- event{etype: eventTypeResendPrepares}
		case <-r.closing:
			return
		}
	}
}

func (r *Replica) loop() {
	logging.Logf("[Node %d] start loop...\n", r.config.NodeId)
EventLoop:
	for e := range r.events {
		// logging.Logf("%v [Node %d] Event %s\n", r.tt.Now(), r.config.NodeId, e.String())
		switch e.etype {
		case eventTypeReplicaClosing:
			for _, conn := range r.serverConns {
				if conn != nil {
					// logging.Logf("[Node %d] closing conn %p\n", r.config.NodeId, conn)
					conn.Close()
				}
			}
			for _, conn := range r.conns {
				if conn != nil {
					// logging.Logf("[Node %d] closing conn %p\n", r.config.NodeId, conn)
					conn.Close()
				}
			}
			break EventLoop
		case eventTypeConnAccepted:
			r.conns = append(r.conns, e.conn)
			r.connCloseWg.Go(func() { r.listen(e.conn, -1) }) // create goroutine here
		case eventTypeConnClosed:
			if e.targetNodeId != -1 {
				// logging.Logf("[Node %d] closing conn %p\n", r.config.NodeId, r.serverConns[e.targetNodeId])
				err := r.serverConns[e.targetNodeId].Close()
				if err != nil {
					// TODO: log
				}
				r.serverConns[e.targetNodeId] = nil // dereference
			} else {
				for i, conn := range r.conns {
					if conn == e.conn {
						r.conns[i] = nil
					}
				}
				// logging.Logf("[Node %d] closing conn %p\n", r.config.NodeId, e.conn)
				err := e.conn.Close()
				if err != nil {
					// TODO: log
				}
			}
		case eventTypeSend:
			r.handleSend(e.targetNodeId, e.envelope)
		case eventTypeHeartbeatCommit:
			r.handleHeartbeatCommit()
		case eventTypeResendPrepares:
			r.handleResendPrepares()
		case eventTypeViewChangeTimer:
			r.handleViewChangeTimer()
		case eventTypeInitRecovery:
			r.handleInitRecovery()
		case eventTypeMsgRecv:
			r.handleMsgRecv(e.conn, e.envelope)
		default:
			assert.Assertf(false, "Unknown event type %d", e.etype)
		}
	}
	close(r.closed)
	// logging.Logf("[Node %d] Closed\n", r.config.NodeId)
}

func (r *Replica) listen(conn net.Conn, targetNodeId int) {
	defer func() {
		r.events <- event{
			etype:        eventTypeConnClosed,
			conn:         conn,
			targetNodeId: targetNodeId,
		}
	}()
	// logging.Logf("[Node %d] listen conn %p targetNodeId: %d\n", r.config.NodeId, conn, targetNodeId)
	for {
		select {
		case <-r.closing:
			return
		default:
			envelope, err := r.encdec.Decode(conn)
			if err != nil {
				return
			}
			assert.Assert(envelope != nil, "envelope should not be nil")
			r.events <- event{
				etype:    eventTypeMsgRecv,
				envelope: envelope,
				conn:     conn,
			}
		}
	}
}

func (r *Replica) newEnvelope(cmd CmdType, payload any) *Envelope {
	return &Envelope{
		Cmd:        cmd,
		FromNodeId: r.config.NodeId,
		Payload:    payload,
	}
}

func (r *Replica) sendReply(clientId uint64, reply *Reply) {
	conn := r.clientConns[clientId]
	// logging.Logf("Send reply from server %d to client %d, conn %p..\n", r.config.NodeId, clientId, conn)
	if conn != nil {
		envelope := r.newEnvelope(CmdTypeReply, reply)
		// logging.Logf("Send reply from server %d to client %d, conn %p %s..\n", r.config.NodeId, clientId, conn, envelope.JsonStr())
		err := r.encdec.Encode(conn, envelope)
		if err != nil {
			// on failure, client should ask for the reply again
			delete(r.clientConns, clientId)
			conn.Close()
		}
	}
}

func (r *Replica) sendTo(targetNodeId int, cmd CmdType, payload any) {
	assert.Assertf(targetNodeId != r.config.NodeId, "Should not send to self, cmd:%s", cmd.String())

	// create envlope & wait for ack
	envelope := r.newEnvelope(cmd, payload)
	r.events <- event{
		etype:        eventTypeSend,
		envelope:     envelope,
		conn:         nil,
		targetNodeId: targetNodeId,
	}
}

func (r *Replica) broadcast(cmd CmdType, payload any) {
	for nodeId := range r.config.ServerAddrs {
		if nodeId != r.config.NodeId {
			r.sendTo(nodeId, cmd, payload)
		}
	}
}

func (r *Replica) handleSend(targetNodeId int, envelope *Envelope) {
	assert.Assertf(targetNodeId != r.config.NodeId, "Should not send payload to the same node, node_id:%d\n", targetNodeId)
	assert.Assert(envelope != nil, "Should not send with nil envelope")

	if r.serverConns[targetNodeId] == nil {
		conn, err := r.createConn(r.config.ServerAddrs[targetNodeId])
		if err != nil {
			// TODO: log this
			return
		}
		r.serverConns[targetNodeId] = conn
		go r.listen(conn, targetNodeId)
	}

	conn := r.serverConns[targetNodeId]
	err := r.encdec.Encode(conn, envelope)
	if err != nil {
		// TODO: log this
		conn.Close()
		r.serverConns[targetNodeId] = nil
		return
	}
}

func (r *Replica) handleHeartbeatCommit() {
	if r.status != ReplicaStatusNormal {
		return
	}
	if r.primaryNode() != r.config.NodeId {
		return // skip if this is not the primary node
	}
	if r.lastCommitAt.Add(r.config.CommitDelayDuration).After(r.tt.Now()) {
		return // skip, there's a recent heartbeat
	}

	// broadcast the latest commit rather than the requested commit id
	r.broadcast(CmdTypeCommit, &Commit{
		ViewId:   r.viewId,
		CommitId: r.commitId,
	})
	r.lastCommitAt = r.tt.Now()
}

func (r *Replica) handleViewChangeTimer() {
	// always reset timer
	defer r.viewChangeTimer.Reset(r.config.ViewChangeDelayDuration)

	if r.status == ReplicaStatusRecovering {
		return
	}
	if r.status == ReplicaStatusNormal && r.config.NodeId == r.primaryNode() { // skip view change if it is the primary node
		return
	}
	r.initViewChange(r.viewId + 1)
	toViewId := r.quorumAddStartViewChange(r.viewId, r.config.NodeId)
	assert.Assert(toViewId < r.viewId, "First view change timer should not have F replies from other replicas")
}

func (r *Replica) handleInitRecovery() {
	if r.status != ReplicaStatusRecovering {
		return
	}

	r.broadcast(CmdTypeRecovery, &Recovery{
		NodeId: r.config.NodeId,
		Nonce:  r.recoveryNonce,
	})

	r.tt.AfterFunc(r.config.RecoveryRetryDuration, func() {
		select {
		case <-r.closing:
		default:
			r.events <- event{etype: eventTypeInitRecovery}
		}
	})
}

func (r *Replica) initViewChange(viewId uint64) {
	// fmt.Printf("%v node:%d initViewChange fromViewId:%d toViewId:%d lastLogId:%d commitId:%d\n", r.tt.Now(), r.config.NodeId, r.viewId, viewId, r.lastLogId, r.commitId)
	r.status = ReplicaStatusViewChange
	r.viewId = viewId
	r.broadcast(CmdTypeStartViewChange, &StartViewChange{
		ViewId: r.viewId,
		NodeId: r.config.NodeId,
	})
}

func (r *Replica) handleMsgRecv(conn net.Conn, envelope *Envelope) {
	assert.Assert(envelope != nil, "envelope should not be nil")

	switch envelope.Cmd {
	case CmdTypeRequest:
		request, ok := envelope.Payload.(*Request)
		assert.Assert(ok, "should be able to cast envelope payload to *Request type")
		r.clientConns[request.ClientId] = conn // add clientId -> connection mapping
		r.handleRequest(request)
	case CmdTypePrepare:
		prepare, ok := envelope.Payload.(*Prepare)
		assert.Assert(ok, "should be able to cast envelope payload to *Prepare type")
		r.handlePrepare(prepare)
	case CmdTypePrepareOk:
		prepareOk, ok := envelope.Payload.(*PrepareOk)
		assert.Assert(ok, "should be able to cast envelope payload to *PrepareOk type")
		r.handlePrepareOk(prepareOk)
	case CmdTypeReply:
		assert.Assert(false, "reply should not be sent to a server node")
	case CmdTypeCommit:
		commit, ok := envelope.Payload.(*Commit)
		assert.Assert(ok, "should be able to cast envelope payload to *Commit type")
		// fmt.Printf("%v node:%d Commit by_node:%d viewId:%d commitId:%d\n", r.tt.Now(), r.config.NodeId, envelope.FromNodeId, commit.ViewId, commit.CommitId)
		r.handleCommit(commit)
	case CmdTypeStartViewChange:
		startViewChange, ok := envelope.Payload.(*StartViewChange)
		assert.Assert(ok, "should be able to cast envelope payload to *StartViewChange type")
		// fmt.Printf("%v node:%d StartViewChange by_node:%d viewId:%d\n", r.tt.Now(), r.config.NodeId, startViewChange.NodeId, startViewChange.ViewId)
		r.handleStartViewChange(startViewChange)
	case CmdTypeDoViewChange:
		doViewChange, ok := envelope.Payload.(*DoViewChange)
		assert.Assert(ok, "should be able to cast envelope payload to *DoViewChange type")
		// fmt.Printf("%v node:%d DoViewChange by_node:%d viewId:%d lastLogId:%d commitId:%d\n", r.tt.Now(), r.config.NodeId, doViewChange.NodeId, doViewChange.ViewId, doViewChange.LastLogId, doViewChange.CommitId)
		r.handleDoViewChange(doViewChange)
	case CmdTypeStartView:
		startView, ok := envelope.Payload.(*StartView)
		assert.Assert(ok, "should be able to cast envelope payload to *StartView type")
		// fmt.Printf("%v node:%d StartView viewId:%d lastLogId:%d commitId:%d\n", r.tt.Now(), r.config.NodeId, startView.ViewId, startView.LastLogId, startView.CommitId)
		r.handleStartView(startView)
	case CmdTypeRecovery:
		recovery, ok := envelope.Payload.(*Recovery)
		assert.Assert(ok, "should be able to cast envelope payload to *Recovery type")
		r.handleRecovery(recovery)
	case CmdTypeRecoveryResponse:
		recoveryResponse, ok := envelope.Payload.(*RecoveryResponse)
		assert.Assert(ok, "should be able to cast envelope payload to *RecoveryResponse type")
		r.handleRecoveryResponse(recoveryResponse)
	case CmdTypeGetState:
		getState, ok := envelope.Payload.(*GetState)
		assert.Assert(ok, "should be able to cast envelope payload to *GetState type")
		r.handleGetState(envelope.FromNodeId, getState)
	case CmdTypeNewState:
		newState, ok := envelope.Payload.(*NewState)
		assert.Assert(ok, "should be able to cast envelope payload to *NewState type")
		r.handleNewState(newState)
	default:
		assert.Assertf(false, "Unknown envelope cmd %d", envelope.Cmd)
	}
}

func (r *Replica) handleRequest(request *Request) {
	assert.Assert(request != nil, "request should not be nil")

	if r.status != ReplicaStatusNormal {
		// replica is not ready to accept request
		r.sendReply(request.ClientId, &Reply{
			ViewId:    r.viewId,
			RequestId: request.RequestId,
			Error:     ProtocolErrorReplicaNotReady,
		})
		return
	}

	if r.primaryNode() != r.config.NodeId {
		// not the primary node
		r.sendReply(request.ClientId, &Reply{
			ViewId:    r.viewId,
			RequestId: request.RequestId,
			Error:     ProtocolErrorNotPrimaryNode,
		})
		return
	}

	// process from client table
	clientInfo := r.clients[request.ClientId]
	if clientInfo != nil {
		if request.RequestId < clientInfo.requestId {
			r.sendReply(request.ClientId, &Reply{
				ViewId:    r.viewId,
				RequestId: request.RequestId,
				Error:     ProtocolErrorExpiredRequestId,
			})
			return // drop the request
		}
		if request.RequestId == clientInfo.requestId {
			if clientInfo.reply != nil {
				// send reply
				r.sendReply(request.ClientId, clientInfo.reply)
			}
			return
		}

		clientInfo.requestId = request.RequestId
		clientInfo.reply = nil
	}

	// advance log and add to client table
	r.appendLog(request)

	// add to quorum
	toCommitId := r.quorumAddPrepareOk(r.lastLogId, r.config.NodeId)
	assert.Assert(toCommitId <= r.commitId, "Primary quorumAddPrepareOk should not commit anything")

	// broadcast prepare request
	r.broadcast(CmdTypePrepare, &Prepare{
		ViewId:        r.viewId,
		LogId:         r.lastLogId,
		CommitId:      r.commitId,
		ClientRequest: request,
	})
	r.lastCommitAt = r.tt.Now()
}

func (r *Replica) handleResendPrepares() {
	if r.primaryNode() != r.config.NodeId {
		return
	}
	if r.status != ReplicaStatusNormal {
		return
	}

	// TODO: fix this, currently it is hardcoded to the first 100 msgs only
	r.logs.Iterate(r.commitId+1, min(r.commitId+100, r.lastLogId), func(entry *LogEntry) {
		r.broadcast(CmdTypePrepare, &Prepare{
			ViewId:   r.viewId,
			LogId:    entry.LogId,
			CommitId: r.commitId,
			ClientRequest: &Request{
				ClientId:    entry.ClientId,
				RequestId:   entry.RequestId,
				RequestBody: entry.Body,
			},
		})
		r.lastCommitAt = r.tt.Now()
	})
}

func (r *Replica) handlePrepare(prepare *Prepare) {
	assert.Assert(prepare != nil, "prepare should not be nil")

	if r.status != ReplicaStatusNormal {
		return // replica in view change or recovery mode
	}
	if !r.validateViewId(prepare.ViewId, false) {
		r.storeWaitingPrepare(prepare)
		r.initGetState(prepare.ViewId, prepare.LogId, 1, true)
		return
	}
	if r.lastLogId >= prepare.LogId {
		// already existed
		return
	}

	// reset view change timer since there's a message from primary node
	r.viewChangeTimer.Reset(r.config.ViewChangeDelayDuration)

	// backup to apply commit from h.lastLogId + 1 until prepare.CommitId
	r.doCommit(prepare.CommitId, false)

	if r.lastLogId+1 < prepare.LogId {
		r.storeWaitingPrepare(prepare)
		return
	}

	// advance log and add to client table
	r.appendLog(prepare.ClientRequest)

	// send PrepareOk to primary node
	primaryNodeId := r.primaryNode()
	r.sendTo(primaryNodeId, CmdTypePrepareOk, &PrepareOk{
		ViewId: r.viewId,
		LogId:  r.lastLogId,
		NodeId: r.config.NodeId,
	})

	// handle next prepare if present
	if nextPrepare, ok := r.waitingPrepares[r.lastLogId+1]; ok {
		r.handlePrepare(nextPrepare)
	}
}

func (r *Replica) handlePrepareOk(prepareOk *PrepareOk) {
	assert.Assert(prepareOk != nil, "prepareOk should not be nil")

	if r.status != ReplicaStatusNormal {
		return // replica in view change or recovery mode
	}
	if !r.validateViewId(prepareOk.ViewId, true) {
		r.initGetState(prepareOk.ViewId, prepareOk.LogId, 0, false)
		return
	}
	if r.commitId >= prepareOk.LogId {
		return // committed, skip this log id
	}

	toCommitId := r.quorumAddPrepareOk(prepareOk.LogId, prepareOk.NodeId)
	r.doCommit(toCommitId, true)
}

func (r *Replica) handleCommit(commit *Commit) {
	assert.Assert(commit != nil, "commit should not be nil")

	if r.status != ReplicaStatusNormal {
		return // replica in view change or recovery mode
	}
	if !r.validateViewId(commit.ViewId, false) {
		r.initGetState(commit.ViewId, commit.CommitId, 0, true)
		return
	}

	// reset view change timer since there's a message from primary node
	r.viewChangeTimer.Reset(r.config.ViewChangeDelayDuration)
	r.doCommit(commit.CommitId, false)
}

func (r *Replica) handleStartViewChange(startViewChange *StartViewChange) {
	assert.Assert(startViewChange != nil, "startViewChange should not be nil")

	if r.status == ReplicaStatusRecovering {
		return
	}

	if startViewChange.ViewId > r.viewId {
		// trigger view change
		r.initViewChange(startViewChange.ViewId)
		r.quorumAddStartViewChange(startViewChange.ViewId, r.config.NodeId)
	}

	toViewId := r.quorumAddStartViewChange(startViewChange.ViewId, startViewChange.NodeId)
	if toViewId > r.lastNormalViewId {
		nextPrimaryId := int(toViewId % uint64(len(r.config.ServerAddrs)))
		doViewChange := &DoViewChange{
			ViewId:           toViewId,
			LastNormalViewId: r.lastNormalViewId,
			LastLogId:        r.lastLogId,
			CommitId:         r.commitId,
			NodeId:           r.config.NodeId,
		}
		if nextPrimaryId == r.config.NodeId {
			r.handleDoViewChange(doViewChange)
		} else {
			// copy logs
			doViewChange.Logs = r.logs.CopyLogs(1, r.lastLogId)
			r.sendTo(nextPrimaryId, CmdTypeDoViewChange, doViewChange)
		}
	}
}

func (r *Replica) handleDoViewChange(doViewChange *DoViewChange) {
	assert.Assert(doViewChange != nil, "doViewChange should not be nil")
	assert.Assertf(
		int(doViewChange.ViewId%uint64(len(r.config.ServerAddrs))) == r.config.NodeId,
		"DoViewChange sent to a wrong node, doViewChange.ViewId=%d, nodeId=%d",
		doViewChange.ViewId,
		r.config.NodeId,
	)

	if r.status == ReplicaStatusRecovering {
		return
	}
	if doViewChange.ViewId < r.viewId {
		return // skip if incoming view id is outdated
	}
	if doViewChange.ViewId == r.viewId && r.status == ReplicaStatusNormal {
		return // skip if view id matched
	}
	if doViewChange.ViewId > r.viewId {
		// transition state to view change mode
		r.initViewChange(doViewChange.ViewId)
		r.quorumAddStartViewChange(doViewChange.ViewId, r.config.NodeId)
	}

	// add value to the set
	set, ok := r.doViewChangeSet[doViewChange.ViewId]
	if !ok {
		set = make(map[int]*DoViewChange)
		r.doViewChangeSet[doViewChange.ViewId] = set
	}
	set[doViewChange.NodeId] = doViewChange

	// quorum check
	if threshold := (len(r.config.ServerAddrs))/2 + 1; len(set) >= threshold {
		var best *DoViewChange
		bestCommitId := uint64(0)
		for _, current := range set {
			if best == nil ||
				best.LastNormalViewId < current.LastNormalViewId ||
				best.LastNormalViewId == current.LastNormalViewId && best.LastLogId < current.LastLogId {
				best = current
			}
			if bestCommitId < current.CommitId {
				bestCommitId = current.CommitId
			}
		}
		// fmt.Printf("%v node:%d quorum check on view:%d size:%d, lastLogId:%d commitId:%d\n", r.tt.Now(), r.config.NodeId, doViewChange.ViewId, len(set), best.LastLogId, bestCommitId)
		// update state
		r.replaceState(best.ViewId, best.LastLogId, bestCommitId, best.Logs, best.NodeId == r.config.NodeId, false)
		if r.lastLogId > r.commitId {
			r.quorumAddPrepareOk(r.lastLogId, r.config.NodeId)
		}
		// broadcast start view
		logs := best.Logs
		if best.NodeId == r.config.NodeId { // late copy of the logs
			logs = r.logs.CopyLogs(1, r.lastLogId)
		}
		r.broadcast(CmdTypeStartView, &StartView{
			ViewId:    r.viewId,
			Logs:      logs,
			LastLogId: r.lastLogId,
			CommitId:  r.commitId,
		})
	}
}

func (r *Replica) handleStartView(startView *StartView) {
	assert.Assert(startView != nil, "startView should not be nil")
	assert.Assertf(
		int(startView.ViewId%uint64(len(r.config.ServerAddrs))) != r.config.NodeId,
		"StartView sent to a primary node, startView.ViewId=%d, nodeId=%d",
		startView.ViewId,
		r.config.NodeId,
	)

	if r.status == ReplicaStatusRecovering {
		return
	}
	if startView.ViewId < r.viewId || startView.ViewId == r.viewId && r.status == ReplicaStatusNormal {
		return
	}

	// update state
	r.replaceState(startView.ViewId, startView.LastLogId, startView.CommitId, startView.Logs, false, false)
	// send prepareOk
	if r.lastLogId > r.commitId {
		r.sendTo(r.primaryNode(), CmdTypePrepareOk, &PrepareOk{
			ViewId: r.viewId,
			LogId:  r.lastLogId,
			NodeId: r.config.NodeId,
		})
	}
}

func (r *Replica) handleRecovery(recovery *Recovery) {
	assert.Assert(recovery != nil, "recovery should not be nil")
	assert.Assertf(
		recovery.NodeId != r.config.NodeId,
		"Recovery sent to the same node id, recovery.NodeId=%d, nodeId=%d",
		recovery.NodeId,
		r.config.NodeId,
	)

	if r.status != ReplicaStatusNormal {
		return
	}

	response := &RecoveryResponse{
		ViewId:     r.viewId,
		Nonce:      recovery.Nonce,
		FromNodeId: r.config.NodeId,
	}
	if r.primaryNode() == r.config.NodeId {
		response.Logs = r.logs.CopyLogs(1, r.lastLogId)
		response.LastLogId = r.lastLogId
		response.CommitId = r.commitId
	}
	r.sendTo(recovery.NodeId, CmdTypeRecoveryResponse, response)
}

func (r *Replica) handleRecoveryResponse(recoveryResponse *RecoveryResponse) {
	assert.Assert(recoveryResponse != nil, "recoveryResponse should not be nil")
	assert.Assertf(
		recoveryResponse.FromNodeId != r.config.NodeId,
		"RecoveryResponse sent to the same node id, recoveryResponse.FromNodeId=%d, nodeId=%d",
		recoveryResponse.FromNodeId,
		r.config.NodeId,
	)

	if r.status != ReplicaStatusRecovering {
		return
	}
	if recoveryResponse.Nonce != r.recoveryNonce {
		return
	}

	r.recoveryResponses[recoveryResponse.FromNodeId] = recoveryResponse
	maxViewId := uint64(0)
	count := 0
	for _, response := range r.recoveryResponses {
		if response != nil {
			count++
			if response.ViewId > maxViewId {
				maxViewId = response.ViewId
			}
		}
	}
	threshold := (len(r.config.ServerAddrs))/2 + 1
	primaryNodeId := int(maxViewId % uint64(len(r.config.ServerAddrs)))
	primaryNodeResponse := r.recoveryResponses[primaryNodeId]
	if count >= threshold && primaryNodeResponse != nil && primaryNodeResponse.ViewId == maxViewId {
		// update state
		r.replaceState(
			primaryNodeResponse.ViewId,
			primaryNodeResponse.LastLogId,
			primaryNodeResponse.CommitId,
			primaryNodeResponse.Logs,
			false,
			true,
		)
		// cleanup
		for i := range r.recoveryResponses {
			r.recoveryResponses[i] = nil // unset
		}
	}
}

func (r *Replica) handleGetState(fromNodeId int, getState *GetState) {
	assert.Assert(getState != nil, "getState should not be nil")

	if r.status != ReplicaStatusNormal {
		return
	}
	if r.viewId < getState.ViewId {
		return
	}

	r.sendTo(fromNodeId, CmdTypeNewState, &NewState{
		ViewId:    r.viewId,
		Logs:      r.logs.CopyLogs(getState.LastLogId+1, r.lastLogId),
		LastLogId: r.lastLogId,
		CommitId:  r.commitId,
	})
}

func (r *Replica) handleNewState(newState *NewState) {
	assert.Assert(newState != nil, "newState should not be nil")

	if r.viewId > newState.ViewId {
		return
	}

	// remove prefix logs
	//  - if view id is the same, it should be equal until lastLogId
	//  - if view id is bigger, it should be equal until commitId
	newLogs := newState.Logs
	for len(newLogs) > 0 && (r.viewId == newState.ViewId && newLogs[0].LogId <= r.lastLogId || newLogs[0].LogId <= r.commitId) {
		requestLog := &newLogs[0]
		newLogs = newLogs[1:] // pop front
		// should be sequential
		if len(newLogs) > 0 {
			assert.Assert(requestLog.LogId+1 == newLogs[0].LogId, "Should receive logs in order")
		}
		// should be equal, TODO: improve this check by hashing??
		entry := r.logs.At(requestLog.LogId)
		assert.Assert(entry.ClientId == requestLog.ClientId, "ClientId should be equal")
		assert.Assert(entry.RequestId == requestLog.RequestId, "RequestId should be equal")
		assert.Assert(entry.LogId == requestLog.LogId, "logId should be equal")
		assert.Assert(bytes.Equal(entry.Body, requestLog.Body), "bytes should be equal")
	}

	// update viewId & lastLogId
	if r.viewId < newState.ViewId {
		r.viewId = newState.ViewId
		if r.status == ReplicaStatusNormal {
			r.lastNormalViewId = r.viewId
		}
		r.lastLogId = newState.LastLogId
		r.logs.Replace(newLogs)
		r.logs.TruncateFrom(r.lastLogId + 1) // in case old primary have seen a new view
	} else if r.viewId == newState.ViewId && r.lastLogId < newState.LastLogId {
		r.lastLogId = newState.LastLogId
		r.logs.Replace(newLogs)
	}

	// update view
	r.viewId = newState.ViewId
	if r.status == ReplicaStatusNormal {
		r.lastNormalViewId = r.viewId
	}

	// update commit
	r.doCommit(newState.CommitId, false)

	// update internal states
	r.clearInvalidWaitingPrepares()
	r.clearInvalidDoViewChangeSet()
	r.updateClientTableByLogs(r.commitId + 1)

	// handle next prepare logId if present
	if nextPrepare, ok := r.waitingPrepares[r.lastLogId+1]; ok {
		r.handlePrepare(nextPrepare)
	}
}

func (r *Replica) replaceState(viewId, lastLogId, commitId uint64, logs []RequestLog, sameNode, recovery bool) {
	assert.Assertf(viewId > r.lastNormalViewId || recovery, "Should not replaceState to lastNormalViewId, found viewId:%d lastNormalViewId:%d", viewId, r.lastNormalViewId)
	r.viewId = viewId
	r.lastNormalViewId = viewId
	r.lastLogId = lastLogId

	if !sameNode {
		// copy logs
		r.logs.Replace(logs)
	}

	// commit
	r.doCommit(commitId, false)

	// update internal states
	r.clearInvalidWaitingPrepares()
	r.clearInvalidDoViewChangeSet()
	r.updateClientTableByLogs(r.commitId + 1)

	// update node status
	r.status = ReplicaStatusNormal
}

func (r *Replica) validateViewId(viewId uint64, shouldBePrimary bool) bool {
	return viewId == r.viewId && shouldBePrimary == (r.primaryNode() == r.config.NodeId)
}

func (r *Replica) initGetState(viewId, logId, gap uint64, checkEqView bool) {
	if r.viewId < viewId || !checkEqView || checkEqView && r.viewId == viewId && r.lastLogId+gap < logId {
		r.broadcast(CmdTypeGetState, &GetState{
			ViewId:    r.viewId,
			LastLogId: r.commitId,
		})
	}
}

func (r *Replica) primaryNode() int {
	return int(r.viewId) % len(r.config.ServerAddrs)
}

func (r *Replica) appendLog(request *Request) {
	r.lastLogId++
	r.logs.Append(LogEntry{
		LogId:     r.lastLogId,
		ClientId:  request.ClientId,
		RequestId: request.RequestId,
		Body:      bytes.Clone(request.RequestBody), // copy
	})
	r.clients[request.ClientId] = &clientTable{
		requestId: request.RequestId,
		reply:     nil,
	}
	delete(r.waitingPrepares, r.lastLogId)
}

func (r *Replica) quorumAddPrepareOk(logId uint64, nodeId int) uint64 {
	if r.lastPreparedLogId[nodeId] < logId {
		r.lastPreparedLogId[nodeId] = logId
	}
	logIds := make([]uint64, 0, len(r.lastPreparedLogId))
	logIds = append(logIds, r.lastPreparedLogId...)
	sort.Slice(logIds, func(i, j int) bool { return logIds[i] > logIds[j] }) // desc

	threshold := (len(r.config.ServerAddrs))/2 + 1
	return logIds[threshold-1]
}

func (r *Replica) doCommit(toCommitId uint64, sendReply bool) {
	if r.lastLogId < toCommitId {
		toCommitId = r.lastLogId
	}
	r.logs.Iterate(r.commitId+1, toCommitId, func(entry *LogEntry) {
		// upcall & commit
		responseBody := r.upcall(entry.Body)
		r.commitId = entry.LogId
		// update client table
		reply := &Reply{
			ViewId:       r.viewId,
			RequestId:    entry.RequestId,
			ResponseBody: responseBody,
		}
		r.clients[entry.ClientId] = &clientTable{
			requestId: entry.RequestId,
			reply:     reply,
		}
		if sendReply {
			r.sendReply(entry.ClientId, reply)
		}
	})
}

func (r *Replica) quorumAddStartViewChange(viewId uint64, nodeId int) uint64 {
	if r.lastChangedViewId[nodeId] < viewId {
		r.lastChangedViewId[nodeId] = viewId
	}

	viewIds := make([]uint64, 0, len(r.lastChangedViewId))
	viewIds = append(viewIds, r.lastChangedViewId...)
	sort.Slice(viewIds, func(i, j int) bool { return viewIds[i] > viewIds[j] }) // desc

	threshold := (len(r.config.ServerAddrs))/2 + 1
	return viewIds[threshold-1]
}

func (r *Replica) storeWaitingPrepare(prepare *Prepare) {
	// only update mapping if it has more advanced view
	if waitingPrepare, ok := r.waitingPrepares[prepare.LogId]; !ok || waitingPrepare.ViewId < prepare.ViewId {
		r.waitingPrepares[prepare.LogId] = prepare
	}
}

func (r *Replica) clearInvalidWaitingPrepares() {
	toRemove := []uint64{}
	for logId, prepare := range r.waitingPrepares {
		if logId <= r.lastLogId || prepare.ViewId < r.viewId {
			toRemove = append(toRemove, logId)
		}
	}
	for _, logId := range toRemove {
		delete(r.waitingPrepares, logId)
	}
}

func (r *Replica) clearInvalidDoViewChangeSet() {
	for viewId := range r.doViewChangeSet {
		if viewId <= r.viewId {
			delete(r.doViewChangeSet, viewId)
		}
	}
}

func (r *Replica) updateClientTableByLogs(fromLogId uint64) {
	r.logs.Iterate(fromLogId, r.lastLogId, func(entry *LogEntry) {
		r.clients[entry.ClientId] = &clientTable{
			requestId: entry.RequestId,
			reply:     nil,
		}
	})
}
