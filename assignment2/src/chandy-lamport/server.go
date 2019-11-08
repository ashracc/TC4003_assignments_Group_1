package chandy_lamport

import "log"

// The main participant of the distributed snapshot protocol.
// Servers exchange token messages and marker messages among each other.
// Token messages represent the transfer of tokens from one server to another.
// Marker messages represent the progress of the snapshot process. The bulk of
// the distributed protocol is implemented in `HandlePacket` and `StartSnapshot`.
type Server struct {
	Id            string
	Tokens        int
	sim           *Simulator
	outboundLinks map[string]*Link // key = link.dest
	inboundLinks  map[string]*Link // key = link.src

	// Map to keep track of channel recording state
	// True means recording, false means recording has stopped
	// Description is: map[snapshotId]map[src]recording_enabled
	chanRecordState map[int]map[string]bool
}

// A unidirectional communication channel between two servers
// Each link contains an event queue (as opposed to a packet queue)
type Link struct {
	src    string
	dest   string
	events *Queue
}

func NewServer(id string, tokens int, sim *Simulator) *Server {
	return &Server{
		id,
		tokens,
		sim,
		make(map[string]*Link),
		make(map[string]*Link),
		make(map[int]map[string]bool),
	}
}

// Add a unidirectional link to the destination server
func (server *Server) AddOutboundLink(dest *Server) {
	if server == dest {
		return
	}
	l := Link{server.Id, dest.Id, NewQueue()}
	server.outboundLinks[dest.Id] = &l
	dest.inboundLinks[server.Id] = &l
}

// Send a message on all of the server's outbound links
func (server *Server) SendToNeighbors(message interface{}) {
	for _, serverId := range getSortedKeys(server.outboundLinks) {
		link := server.outboundLinks[serverId]
		server.sim.logger.RecordEvent(
			server,
			SentMessageEvent{server.Id, link.dest, message})
		link.events.Push(SendMessageEvent{
			server.Id,
			link.dest,
			message,
			server.sim.GetReceiveTime()})
	}
}

// Send a number of tokens to a neighbor attached to this server
func (server *Server) SendTokens(numTokens int, dest string) {
	if server.Tokens < numTokens {
		log.Fatalf("Server %v attempted to send %v tokens when it only has %v\n",
			server.Id, numTokens, server.Tokens)
	}
	message := TokenMessage{numTokens}
	server.sim.logger.RecordEvent(server, SentMessageEvent{server.Id, dest, message})
	// Update local state before sending the tokens
	server.Tokens -= numTokens
	link, ok := server.outboundLinks[dest]
	if !ok {
		log.Fatalf("Unknown dest ID %v from server %v\n", dest, server.Id)
	}
	link.events.Push(SendMessageEvent{
		server.Id,
		dest,
		message,
		server.sim.GetReceiveTime()})
}

// Callback for when a message is received on this server.
// When the snapshot algorithm completes on this server, this function
// should notify the simulator by calling `sim.NotifySnapshotComplete`.
func (server *Server) HandlePacket(src string, message interface{}) {
	switch message := message.(type) {
	case MarkerMessage:
		// If first time received marker, then start snapshot
		if server.chanRecordState[message.snapshotId] == nil {
			server.StartSnapshot(message.snapshotId)
		}

		// Stop recording messages from `src`
		server.chanRecordState[message.snapshotId][src] = false

		// Evaluate if all recording messages have stopped
		completed := true
		for _, state := range server.chanRecordState[message.snapshotId] {
			if state {
				completed = false
				break
			}
		}

		// If no more recordings, then server has finished snapshot
		if completed {
			server.sim.NotifySnapshotComplete(server.Id, message.snapshotId)
		}

	case TokenMessage:
		// If there is a recording enabled for this `src`,
		// then save message in snapshot
		for snapshotId, states := range server.chanRecordState {
			if states[src] {
				value, _ := server.sim.snapshots.Load(snapshotId)
				snap := value.(SnapshotState)
				snap.messages = append(snap.messages, &SnapshotMessage{
					src,
					server.Id,
					message,
				})
				server.sim.snapshots.Store(snapshotId, snap)
			}
		}

		// Receive tokens
		server.Tokens += message.numTokens
	default:
		log.Fatal("Error unknown message: ", message)
	}
}

// Start the chandy-lamport snapshot algorithm on this server.
// This should be called only once per server.
func (server *Server) StartSnapshot(snapshotId int) {
	// Initialize record tracking for this snapshot
	server.chanRecordState[snapshotId] = make(map[string]bool)

	// Enable recordings from each incoming channels
	for link := range server.inboundLinks {
		from := server.inboundLinks[link].src
		server.chanRecordState[snapshotId][from] = true
	}

	// Get the glogal snapshot
	value, _ := server.sim.snapshots.Load(snapshotId)
	snap := value.(SnapshotState)

	// Snapshot server tokens
	snap.tokens[server.Id] = server.Tokens

	// Send marker to all neighbors
	server.SendToNeighbors(MarkerMessage{snapshotId})
}
