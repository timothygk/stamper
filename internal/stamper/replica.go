package stamper

import (
	"crypto/sha256"
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
	replicaStatusNormal     replicaStatus = 1
	replicaStatusViewChange replicaStatus = 2
	replicaStatusRecovering replicaStatus = 3
)

type logEntry struct {
	LogId     uint64
	ClientId  uint64
	RequestId uint64
	Body      []byte
}

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
	eventTypeViewChangeTimer eventType = 5
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
	case eventTypeViewChangeTimer:
		return "view_change_timer"
	case eventTypeMsgRecv:
		return "msg_recv"
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
	envelopeIdR *rand.Rand
	events      chan event

	// states
	viewId    uint64
	status    replicaStatus
	lastLogId uint64
	logs      []logEntry
	commitId  uint64
	clients   map[uint64]*clientTable
	upcall    func([]byte) []byte

	// internal states
	closing           chan struct{}
	closed            chan struct{}
	waitAck           map[uint64]struct{} // set of envelopeId
	waitingPrepares   map[uint64]*Prepare // logId -> prepare request
	lastPreparedLogId []uint64            // node -> last prepared log id
	lastCommitAt      time.Time           // last commit time
	lastNormalViewId  uint64
	lastChangedViewId []uint64 // last view change id
	viewChangeTimer   timepkg.Timer
	doViewChangeSet   map[uint64]map[int]*DoViewChange // viewId -> {nodeId -> doViewChange}
}

// NewReplica create a new replica
func NewReplica(
	config ReplicaConfig,
	tt timepkg.Time,
	encdec EncoderDecoder,
	createConn func(string) (net.Conn, error),
	r *rand.Rand,
	upcall func([]byte) []byte,
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
		envelopeIdR:       r,
		events:            make(chan event, 1000),
		viewId:            0,
		status:            replicaStatusNormal, // TODO: should start with recovering??
		lastLogId:         0,
		logs:              make([]logEntry, 0, 1000),
		commitId:          0,
		clients:           make(map[uint64]*clientTable),
		upcall:            upcall,
		closing:           make(chan struct{}),
		closed:            make(chan struct{}),
		waitAck:           make(map[uint64]struct{}),
		waitingPrepares:   make(map[uint64]*Prepare),
		lastPreparedLogId: make([]uint64, len(config.ServerAddrs)),
		lastNormalViewId:  0,
		lastChangedViewId: make([]uint64, len(config.ServerAddrs)),
		doViewChangeSet:   make(map[uint64]map[int]*DoViewChange),
	}
	go replica.heartbeatLoop()
	go replica.loop()
	replica.viewChangeTimer = tt.AfterFunc(config.ViewChangeDelayDuration, func() {
		replica.events <- event{etype: eventTypeViewChangeTimer}
	})
	return replica
}

// GetState get the replica state to compare with another state
func (r *Replica) GetState() string {
	h := sha256.New()
	for i := range r.logs {
		h.Write(r.logs[i].Body)
	}
	return fmt.Sprintf("status:%d, viewId:%d, commitId:%d, lastLogId:%d, loghash:%x",
		r.status,
		r.viewId,
		r.commitId,
		r.lastLogId,
		h.Sum(nil),
	)
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
	for {
		select {
		case <-ticker:
			r.events <- event{etype: eventTypeHeartbeatCommit}
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
		case eventTypeViewChangeTimer:
			r.handleViewChangeTimer()
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
		EnvelopeId: r.envelopeIdR.Uint64(),
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
	r.waitAck[envelope.EnvelopeId] = struct{}{}
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

	// check if it is ack-ed
	if _, ok := r.waitAck[envelope.EnvelopeId]; !ok {
		return
	}

	// defer to retry
	defer r.tt.AfterFunc(r.config.SendRetryDuration, func() {
		r.events <- event{
			etype:        eventTypeSend,
			envelope:     envelope,
			targetNodeId: targetNodeId,
		}
	})

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

	if r.config.NodeId == r.primaryNode() { // skip view change if it is the primary node
		return
	}
	r.initViewChange(r.viewId + 1)
	toViewId := r.quorumAddStartViewChange(r.viewId, r.config.NodeId)
	assert.Assert(toViewId < r.viewId, "First view change timer should not have F replies from other replicas")
}

func (r *Replica) initViewChange(viewId uint64) {
	// fmt.Printf("%v node:%d initViewChange fromViewId:%d toViewId:%d lastLogId:%d commitId:%d\n", r.tt.Now(), r.config.NodeId, r.viewId, viewId, r.lastLogId, r.commitId)
	r.status = replicaStatusViewChange
	r.viewId = viewId
	r.broadcast(CmdTypeStartViewChange, &StartViewChange{
		ViewId: r.viewId,
		NodeId: r.config.NodeId,
	})
}

func (r *Replica) handleMsgRecv(conn net.Conn, envelope *Envelope) {
	assert.Assert(envelope != nil, "envelope should not be nil")

	shouldAck := true
	switch envelope.Cmd {
	case CmdTypeRequest:
		request, ok := envelope.Payload.(*Request)
		assert.Assert(ok, "should be able to cast envelope payload to *Request type")
		r.clientConns[request.ClientId] = conn // add clientId -> connection mapping
		r.handleRequest(request)
		shouldAck = false
	case CmdTypePrepare:
		prepare, ok := envelope.Payload.(*Prepare)
		assert.Assert(ok, "should be able to cast envelope payload to *Prepare type")
		shouldAck = r.handlePrepare(prepare)
	case CmdTypePrepareOk:
		prepareOk, ok := envelope.Payload.(*PrepareOk)
		assert.Assert(ok, "should be able to cast envelope payload to *PrepareOk type")
		shouldAck = r.handlePrepareOk(prepareOk)
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
	case CmdTypeAck:
		ack, ok := envelope.Payload.(*Ack)
		assert.Assert(ok, "should be able to cast envelope payload to *Ack type")
		delete(r.waitAck, ack.EnvelopeId)
		return // continue to skip sending ack
	default:
		assert.Assertf(false, "Unknown envelope cmd %d", envelope.Cmd)
	}

	// fire and forget the ack
	// logging.Logf("Sending an Ack after msg %s, conn: %p\n", envelope.JsonStr(), conn)
	if shouldAck {
		err := r.encdec.Encode(conn, r.newEnvelope(CmdTypeAck, &Ack{envelope.EnvelopeId}))
		if err != nil {
			// TODO: log this error
		}
	}

	// update view change timer
	primaryNodeId := r.primaryNode()
	if primaryNodeId != r.config.NodeId && primaryNodeId == envelope.FromNodeId {
		r.viewChangeTimer.Reset(r.config.ViewChangeDelayDuration)
	}
}

func (r *Replica) handleRequest(request *Request) {
	assert.Assert(request != nil, "request should not be nil")

	if r.status != replicaStatusNormal {
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

func (r *Replica) handlePrepare(prepare *Prepare) (shouldAck bool) {
	assert.Assert(prepare != nil, "prepare should not be nil")

	if r.status != replicaStatusNormal {
		return false // replica in view change or recovery mode
	}
	if !r.validateViewId(prepare.ViewId, false) {
		return true
	}
	if r.lastLogId >= prepare.LogId {
		// already existed
		return true
	}

	// backup to apply commit from h.lastLogId + 1 until prepare.CommitId
	r.doCommit(prepare.CommitId, false)

	if r.lastLogId+1 < prepare.LogId {
		r.waitingPrepares[prepare.LogId] = prepare
		return true
	}

	// advance log and add to client table
	r.appendLog(prepare.ClientRequest)

	nextPrepare, ok := r.waitingPrepares[r.lastLogId+1]
	for ok {
		// advance log and add to client table
		r.appendLog(nextPrepare.ClientRequest)
		delete(r.waitingPrepares, nextPrepare.LogId)
		nextPrepare, ok = r.waitingPrepares[r.lastLogId+1]
	}

	// send PrepareOk to primary node
	primaryNodeId := r.primaryNode()
	r.sendTo(primaryNodeId, CmdTypePrepareOk, &PrepareOk{
		ViewId: r.viewId,
		LogId:  r.lastLogId,
		NodeId: r.config.NodeId,
	})
	return true
}

func (r *Replica) handlePrepareOk(prepareOk *PrepareOk) (shouldAck bool) {
	assert.Assert(prepareOk != nil, "prepareOk should not be nil")

	if r.status != replicaStatusNormal {
		return false // replica in view change or recovery mode
	}
	if !r.validateViewId(prepareOk.ViewId, true) {
		return true
	}
	if r.commitId >= prepareOk.LogId {
		return true // committed, skip this log id
	}

	toCommitId := r.quorumAddPrepareOk(prepareOk.LogId, prepareOk.NodeId)
	r.doCommit(toCommitId, true)
	return true
}

func (r *Replica) handleCommit(commit *Commit) {
	assert.Assert(commit != nil, "commit should not be nil")

	if r.status != replicaStatusNormal {
		return // replica in view change or recovery mode
	}
	if !r.validateViewId(commit.ViewId, false) {
		return
	}

	r.doCommit(commit.CommitId, false)
}

func (r *Replica) handleStartViewChange(startViewChange *StartViewChange) {
	assert.Assert(startViewChange != nil, "startViewChange should not be nil")

	initialViewId := r.viewId
	if startViewChange.ViewId > r.viewId {
		// trigger view change
		r.initViewChange(startViewChange.ViewId)
		r.quorumAddStartViewChange(startViewChange.ViewId, r.config.NodeId)
	}

	toViewId := r.quorumAddStartViewChange(startViewChange.ViewId, startViewChange.NodeId)
	if toViewId > initialViewId {
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
			doViewChange.Logs = make([]RequestLog, len(r.logs))
			for i := range r.logs {
				doViewChange.Logs[i].ClientId = r.logs[i].ClientId
				doViewChange.Logs[i].RequestId = r.logs[i].RequestId
				doViewChange.Logs[i].LogId = r.logs[i].LogId
				doViewChange.Logs[i].Body = r.logs[i].Body
			}
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

	if doViewChange.ViewId < r.viewId {
		return // skip if incoming view id is outdated
	}
	if doViewChange.ViewId == r.viewId && r.status == replicaStatusNormal {
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
		// fmt.Printf("%v node:%d quorum check on view:%d size:%d, lagLogId:%d commitId:%d\n", r.tt.Now(), r.config.NodeId, doViewChange.ViewId, len(set), best.LastLogId, bestCommitId)
		// update state
		r.replaceState(best.ViewId, best.LastLogId, bestCommitId, best.Logs, best.NodeId == r.config.NodeId)
		if r.lastLogId > r.commitId {
			r.quorumAddPrepareOk(r.lastLogId, r.config.NodeId)
		}
		// broadcast start view
		logs := best.Logs
		if best.NodeId == r.config.NodeId { // late copy of the logs
			logs = make([]RequestLog, len(r.logs))
			for i := range len(r.logs) {
				logs[i].ClientId = r.logs[i].ClientId
				logs[i].RequestId = r.logs[i].RequestId
				logs[i].LogId = r.logs[i].LogId
				logs[i].Body = r.logs[i].Body
			}
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

	if startView.ViewId < r.viewId || startView.ViewId == r.viewId && r.status == replicaStatusNormal {
		return
	}

	// update state
	r.replaceState(startView.ViewId, startView.LastLogId, startView.CommitId, startView.Logs, false)
	// send prepareOk
	if r.lastLogId > r.commitId {
		r.sendTo(r.primaryNode(), CmdTypePrepareOk, &PrepareOk{
			ViewId: r.viewId,
			LogId:  r.lastLogId,
			NodeId: r.config.NodeId,
		})
	}
}

func (r *Replica) replaceState(viewId, lastLogId, commitId uint64, logs []RequestLog, sameNode bool) {
	assert.Assertf(viewId > r.lastNormalViewId, "Should not replaceState to lastNormalviewId, found viewId:%d lastNormalViewId:%d", viewId, r.lastNormalViewId)
	r.viewId = viewId
	r.lastNormalViewId = viewId
	r.lastLogId = lastLogId

	if !sameNode {
		// copy logs
		r.logs = make([]logEntry, len(logs))
		for i := range logs {
			r.logs[i].ClientId = logs[i].ClientId
			r.logs[i].RequestId = logs[i].RequestId
			r.logs[i].LogId = logs[i].LogId
			r.logs[i].Body = logs[i].Body
		}
	}

	// commit
	r.doCommit(commitId, false)

	// clear internal states
	clear(r.waitingPrepares)
	for viewId := range r.doViewChangeSet {
		if viewId <= r.viewId {
			delete(r.doViewChangeSet, viewId)
		}
	}

	// update client table
	for logId := r.commitId + 1; logId <= r.lastLogId; logId++ {
		index := int(logId) - 1
		assert.Assertf(r.logs[index].LogId == logId, "Logs array should be ordered correctly, expected %d, found %d", logId, r.logs[index].LogId)
		r.clients[r.logs[index].ClientId] = &clientTable{
			requestId: r.logs[index].RequestId,
			reply:     nil,
		}
	}

	// update node status
	r.status = replicaStatusNormal
}

func (r *Replica) validateViewId(viewId uint64, shouldBePrimary bool) bool {
	return viewId == r.viewId && shouldBePrimary == (r.primaryNode() == r.config.NodeId)
}

func (r *Replica) primaryNode() int {
	return int(r.viewId) % len(r.config.ServerAddrs)
}

func (r *Replica) appendLog(request *Request) {
	assert.Assertf(
		uint64(len(r.logs)) == r.lastLogId,
		"Last log id should match len, found len:%d lastLogId:%d",
		len(r.logs),
		r.lastLogId,
	)
	r.lastLogId++
	r.logs = append(r.logs, logEntry{
		LogId:     r.lastLogId,
		ClientId:  request.ClientId,
		RequestId: request.RequestId,
		Body:      request.RequestBody,
	})
	r.clients[request.ClientId] = &clientTable{
		requestId: request.RequestId,
		reply:     nil,
	}
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
	for logId := r.commitId + 1; logId <= toCommitId; logId++ {
		index := int(logId) - 1
		// upcall & commit
		assert.Assertf(r.logs[index].LogId == logId, "Logs array should be ordered correctly, expected %d, found %d", logId, r.logs[index].LogId)
		responseBody := r.upcall(r.logs[index].Body)
		r.commitId = logId
		// update client table
		reply := &Reply{
			ViewId:       r.viewId,
			RequestId:    r.logs[index].RequestId,
			ResponseBody: responseBody,
		}
		r.clients[r.logs[index].ClientId] = &clientTable{
			requestId: r.logs[index].RequestId,
			reply:     reply,
		}
		if sendReply {
			r.sendReply(r.logs[index].ClientId, reply)
		}
	}
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
