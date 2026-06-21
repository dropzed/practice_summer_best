package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"message-broker/pkg/protocol"
)

// OffsetLease represents an outstanding message reservation by a consumer.
type OffsetLease struct {
	SubscriberID string    `json:"subscriber_id"`
	Expiry       time.Time `json:"expiry"`
}

// ConsumerGroupState manages the offsets and leases for a single consumer group.
type ConsumerGroupState struct {
	mu              sync.Mutex
	CommittedOffset uint64          `json:"committed_offset"`
	ResolvedOffsets map[uint64]bool `json:"resolved_offsets"`
	Leases          map[uint64]OffsetLease `json:"-"`
}

func NewConsumerGroupState() *ConsumerGroupState {
	return &ConsumerGroupState{
		CommittedOffset: 1, // Offsets start at 1
		ResolvedOffsets: make(map[uint64]bool),
		Leases:          make(map[uint64]OffsetLease),
	}
}

// TopicState manages routing and group states for a single topic.
type TopicState struct {
	mu       sync.RWMutex
	BrokerID string
	Groups   map[string]*ConsumerGroupState
}

func NewTopicState(brokerID string) *TopicState {
	return &TopicState{
		BrokerID: brokerID,
		Groups:   make(map[string]*ConsumerGroupState),
	}
}

// StateSerialization is used for persisting committed offsets to disk.
type StateSerialization struct {
	Topics map[string]map[string]uint64 `json:"topics"` // topic -> group -> committedOffset
}

// QueueManager is the central coordinator for system routing and metadata.
type QueueManager struct {
	statePath    string
	stateLock    sync.Mutex // Protects file write and serialization of registry
	
	brokersMu    sync.RWMutex
	brokers      map[string]string    // ID -> Address
	brokerHeart  map[string]time.Time // ID -> Last Heartbeat time

	topicsMu     sync.RWMutex
	topics       map[string]*TopicState
	subLastSeen  map[string]map[string]time.Time // Topic -> SubID -> LastSeen (in-memory)
	subSeenMu    sync.RWMutex
}

func NewQueueManager(statePath string) *QueueManager {
	return &QueueManager{
		statePath:   statePath,
		brokers:     make(map[string]string),
		brokerHeart: make(map[string]time.Time),
		topics:      make(map[string]*TopicState),
		subLastSeen: make(map[string]map[string]time.Time),
	}
}

// LoadState restores committed offsets from the JSON registry.
func (qm *QueueManager) LoadState() error {
	qm.stateLock.Lock()
	defer qm.stateLock.Unlock()

	data, err := os.ReadFile(qm.statePath)
	if err != nil {
		if os.IsNotExist(err) {
			log.Printf("[QueueManager] No state file found at %s. Initializing empty registry.", qm.statePath)
			return nil
		}
		return fmt.Errorf("failed to read state file: %w", err)
	}

	var serial StateSerialization
	if err := json.Unmarshal(data, &serial); err != nil {
		return fmt.Errorf("failed to parse state JSON: %w", err)
	}

	qm.topicsMu.Lock()
	defer qm.topicsMu.Unlock()

	for topic, groups := range serial.Topics {
		ts := NewTopicState("")
		for group, commit := range groups {
			gs := NewConsumerGroupState()
			gs.CommittedOffset = commit
			ts.Groups[group] = gs
		}
		qm.topics[topic] = ts
	}

	log.Printf("[QueueManager] Successfully loaded metadata registry from %s", qm.statePath)
	return nil
}

// SaveState persists committed offsets atomically.
func (qm *QueueManager) SaveState() error {
	qm.stateLock.Lock()
	defer qm.stateLock.Unlock()

	serial := StateSerialization{
		Topics: make(map[string]map[string]uint64),
	}

	qm.topicsMu.RLock()
	for topicName, ts := range qm.topics {
		ts.mu.RLock()
		if len(ts.Groups) > 0 {
			serial.Topics[topicName] = make(map[string]uint64)
			for groupName, gs := range ts.Groups {
				gs.mu.Lock()
				serial.Topics[topicName][groupName] = gs.CommittedOffset
				gs.mu.Unlock()
			}
		}
		ts.mu.RUnlock()
	}
	qm.topicsMu.RUnlock()

	data, err := json.MarshalIndent(serial, "", "  ")
	if err != nil {
		return err
	}

	dir := filepath.Dir(qm.statePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	// Atomic write using a temp file and rename (prevent file corruption on crashes)
	tmpPath := qm.statePath + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return err
	}

	if err := os.Rename(tmpPath, qm.statePath); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}

	return nil
}

func (qm *QueueManager) RegisterBroker(w http.ResponseWriter, r *http.Request) {
	var req protocol.BrokerRegisterRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	qm.brokersMu.Lock()
	qm.brokers[req.ID] = req.Address
	qm.brokerHeart[req.ID] = time.Now()
	qm.brokersMu.Unlock()

	log.Printf("[QueueManager] Registered broker %q at %s", req.ID, req.Address)
	w.WriteHeader(http.StatusOK)
}

func (qm *QueueManager) Heartbeat(w http.ResponseWriter, r *http.Request) {
	var req protocol.BrokerRegisterRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	qm.brokersMu.Lock()
	qm.brokerHeart[req.ID] = time.Now()
	if _, exists := qm.brokers[req.ID]; !exists {
		qm.brokers[req.ID] = req.Address
	}
	qm.brokersMu.Unlock()

	w.WriteHeader(http.StatusOK)
}

func (qm *QueueManager) RegisterTopic(w http.ResponseWriter, r *http.Request) {
	var req protocol.TopicRegisterRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	qm.topicsMu.Lock()
	ts, exists := qm.topics[req.Topic]
	if !exists {
		ts = NewTopicState(req.BrokerID)
		qm.topics[req.Topic] = ts
	} else {
		ts.mu.Lock()
		ts.BrokerID = req.BrokerID
		ts.mu.Unlock()
	}
	qm.topicsMu.Unlock()

	log.Printf("[QueueManager] Topic %q mapped to broker %q", req.Topic, req.BrokerID)
	w.WriteHeader(http.StatusOK)
}

func (qm *QueueManager) RouteTopic(w http.ResponseWriter, r *http.Request) {
	topic := r.URL.Query().Get("topic")
	if topic == "" {
		http.Error(w, "missing topic query param", http.StatusBadRequest)
		return
	}

	qm.topicsMu.Lock()
	ts, exists := qm.topics[topic]
	var addr string

	if exists {
		ts.mu.RLock()
		brokerID := ts.BrokerID
		ts.mu.RUnlock()

		qm.brokersMu.RLock()
		lastHeart, active := qm.brokerHeart[brokerID]
		if active && time.Since(lastHeart) < 10*time.Second {
			addr = qm.brokers[brokerID]
		}
		qm.brokersMu.RUnlock()
	}

	// Dynamic routing: if topic has no host or the host broker is offline, re-route to an active broker
	if addr == "" {
		qm.brokersMu.RLock()
		var activeIDs []string
		now := time.Now()
		for id, lastHeart := range qm.brokerHeart {
			if now.Sub(lastHeart) < 10*time.Second {
				activeIDs = append(activeIDs, id)
			}
		}

		if len(activeIDs) == 0 {
			qm.brokersMu.RUnlock()
			qm.topicsMu.Unlock()
			http.Error(w, "No active brokers available", http.StatusServiceUnavailable)
			return
		}

		// Choose the broker with the minimum number of topics assigned (simple load balancing)
		chosenBroker := activeIDs[0]
		minTopics := -1
		for _, bID := range activeIDs {
			count := 0
			for _, t := range qm.topics {
				t.mu.RLock()
				if t.BrokerID == bID {
					count++
				}
				t.mu.RUnlock()
			}
			if minTopics == -1 || count < minTopics {
				minTopics = count
				chosenBroker = bID
			}
		}
		addr = qm.brokers[chosenBroker]
		qm.brokersMu.RUnlock()

		if !exists {
			ts = NewTopicState(chosenBroker)
			qm.topics[topic] = ts
		} else {
			ts.mu.Lock()
			ts.BrokerID = chosenBroker
			ts.mu.Unlock()
		}

		log.Printf("[QueueManager] Auto-routing topic %q to broker %q (%s)", topic, chosenBroker, addr)
		go func() {
			if err := qm.SaveState(); err != nil {
				log.Printf("[QueueManager] Failed to save state on dynamic route: %v", err)
			}
		}()
	}
	qm.topicsMu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{"address": addr})
}

func (qm *QueueManager) LeaseOffsets(w http.ResponseWriter, r *http.Request) {
	var req protocol.QMLeaseRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	qm.topicsMu.Lock()
	ts, exists := qm.topics[req.Topic]
	if !exists {
		ts = NewTopicState("")
		qm.topics[req.Topic] = ts
	}
	qm.topicsMu.Unlock()

	ts.mu.Lock()
	gs, ok := ts.Groups[req.Group]
	if !ok {
		gs = NewConsumerGroupState()
		ts.Groups[req.Group] = gs
	}
	ts.mu.Unlock()

	// Track subscriber activity
	qm.subSeenMu.Lock()
	if qm.subLastSeen[req.Topic] == nil {
		qm.subLastSeen[req.Topic] = make(map[string]time.Time)
	}
	qm.subLastSeen[req.Topic][req.SubscriberID] = time.Now()
	qm.subSeenMu.Unlock()

	gs.mu.Lock()
	defer gs.mu.Unlock()

	var leased []uint64
	now := time.Now()

	for offset := gs.CommittedOffset; offset <= req.BrokerMaxOffset; offset++ {
		if len(leased) >= req.Limit {
			break
		}

		// Skip if offset was already processed and ACKed
		if gs.ResolvedOffsets[offset] {
			continue
		}

		lease, active := gs.Leases[offset]
		if active && now.Before(lease.Expiry) {
			// Offset is currently locked and processing by another subscriber
			continue
		}

		// Lease offset
		gs.Leases[offset] = OffsetLease{
			SubscriberID: req.SubscriberID,
			Expiry:       now.Add(10 * time.Second), // 10s default lease
		}
		leased = append(leased, offset)
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(protocol.QMLeaseResponse{Offsets: leased})
}

func (qm *QueueManager) AckOffsets(w http.ResponseWriter, r *http.Request) {
	var req protocol.QMAckRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	qm.topicsMu.RLock()
	ts, exists := qm.topics[req.Topic]
	qm.topicsMu.RUnlock()

	if !exists {
		http.Error(w, "Topic not found", http.StatusNotFound)
		return
	}

	ts.mu.RLock()
	gs, exists := ts.Groups[req.Group]
	ts.mu.RUnlock()

	if !exists {
		http.Error(w, "Group not found", http.StatusNotFound)
		return
	}

	gs.mu.Lock()
	for _, offset := range req.Offsets {
		if offset >= gs.CommittedOffset {
			gs.ResolvedOffsets[offset] = true
		}
		delete(gs.Leases, offset)
	}

	// Move committed pointer forward past resolved contiguous offsets
	originalOffset := gs.CommittedOffset
	for gs.ResolvedOffsets[gs.CommittedOffset] {
		delete(gs.ResolvedOffsets, gs.CommittedOffset)
		gs.CommittedOffset++
	}
	gs.mu.Unlock()

	// If CommittedOffset moved, persist metadata to disk
	if gs.CommittedOffset != originalOffset {
		go func() {
			if err := qm.SaveState(); err != nil {
				log.Printf("[QueueManager] Failed to save state on ACK: %v", err)
			}
		}()
	}

	w.WriteHeader(http.StatusOK)
}

func (qm *QueueManager) GetStatus(w http.ResponseWriter, r *http.Request) {
	now := time.Now()

	// Gather active brokers
	qm.brokersMu.RLock()
	activeBrokers := make(map[string]string)
	for id, addr := range qm.brokers {
		lastHeart := qm.brokerHeart[id]
		if now.Sub(lastHeart) < 15*time.Second {
			activeBrokers[id] = addr
		}
	}
	qm.brokersMu.RUnlock()

	type ClientGroupStatus struct {
		CommittedOffset   uint64   `json:"committed_offset"`
		PendingAcksCount  int      `json:"pending_acks_count"`
		ActiveSubscribers []string `json:"active_subscribers"`
	}

	type TopicStatus struct {
		BrokerID     string                       `json:"broker_id"`
		ActiveGroups map[string]ClientGroupStatus `json:"active_groups"`
	}

	status := struct {
		ActiveBrokers map[string]string      `json:"active_brokers"`
		Topics        map[string]TopicStatus `json:"topics"`
	}{
		ActiveBrokers: activeBrokers,
		Topics:        make(map[string]TopicStatus),
	}

	qm.topicsMu.RLock()
	defer qm.topicsMu.RUnlock()

	for topicName, ts := range qm.topics {
		ts.mu.RLock()
		brokerID := ts.BrokerID
		groupsStatus := make(map[string]ClientGroupStatus)

		for groupName, gs := range ts.Groups {
			gs.mu.Lock()
			committed := gs.CommittedOffset
			
			pendingCount := 0
			for _, lease := range gs.Leases {
				if now.Before(lease.Expiry) {
					pendingCount++
				}
			}
			gs.mu.Unlock()

			// Gather active subscribers seen in last 30s
			var activeSubs []string
			qm.subSeenMu.RLock()
			if subs, exists := qm.subLastSeen[topicName]; exists {
				for subID, lastSeen := range subs {
					if now.Sub(lastSeen) < 30*time.Second {
						activeSubs = append(activeSubs, subID)
					}
				}
			}
			qm.subSeenMu.RUnlock()

			groupsStatus[groupName] = ClientGroupStatus{
				CommittedOffset:   committed,
				PendingAcksCount:  pendingCount,
				ActiveSubscribers: activeSubs,
			}
		}
		ts.mu.RUnlock()

		status.Topics[topicName] = TopicStatus{
			BrokerID:     brokerID,
			ActiveGroups: groupsStatus,
		}
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(status)
}

// RequestLogger is an HTTP middleware for logging incoming requests.
func RequestLogger(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next(w, r)
		log.Printf("[QueueManager] %s %s took %v", r.Method, r.URL.Path, time.Since(start))
	}
}

func main() {
	port := flag.Int("port", 8080, "Port for Queue Manager")
	statePath := flag.String("state", "data/manager/state.json", "Path to state database file")
	flag.Parse()

	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	qm := NewQueueManager(*statePath)
	if err := qm.LoadState(); err != nil {
		log.Fatalf("[QueueManager] Failed to restore state: %v", err)
	}

	// Background ticker to prune dead brokers
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func(ctx context.Context) {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				qm.brokersMu.Lock()
				now := time.Now()
				for id, last := range qm.brokerHeart {
					if now.Sub(last) > 30*time.Second {
						log.Printf("[QueueManager] Broker %q has timed out. Evicting.", id)
						delete(qm.brokers, id)
						delete(qm.brokerHeart, id)
					}
				}
				qm.brokersMu.Unlock()
			}
		}
	}(ctx)

	mux := http.NewServeMux()
	mux.HandleFunc("/brokers/register", RequestLogger(qm.RegisterBroker))
	mux.HandleFunc("/brokers/heartbeat", RequestLogger(qm.Heartbeat))
	mux.HandleFunc("/topics/register", RequestLogger(qm.RegisterTopic))
	mux.HandleFunc("/topics/route", RequestLogger(qm.RouteTopic))
	mux.HandleFunc("/qm/lease", RequestLogger(qm.LeaseOffsets))
	mux.HandleFunc("/qm/ack", RequestLogger(qm.AckOffsets))
	mux.HandleFunc("/status", RequestLogger(qm.GetStatus))

	srv := &http.Server{
		Addr:    fmt.Sprintf(":%d", *port),
		Handler: mux,
	}

	go func() {
		log.Printf("[QueueManager] Server listening on %s", srv.Addr)
		if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("[QueueManager] Server startup error: %v", err)
		}
	}()

	// Graceful shutdown handling
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	log.Printf("[QueueManager] Shutting down gracefully...")
	cancel()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("[QueueManager] Graceful shutdown failed: %v", err)
	}

	if err := qm.SaveState(); err != nil {
		log.Printf("[QueueManager] Failed to persist final state: %v", err)
	} else {
		log.Printf("[QueueManager] Final state persisted successfully. Bye!")
	}
}
