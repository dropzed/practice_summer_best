package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
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

// IndexEntry holds the file position and length of a message.
type IndexEntry struct {
	Pos int64
	Len int
}

// TopicStore manages durability logs and offset indexing for a single topic.
type TopicStore struct {
	mu        sync.Mutex
	topic     string
	filePath  string
	file      *os.File
	index     map[uint64]IndexEntry
	indexMu   sync.RWMutex
	maxOffset uint64
}

func NewTopicStore(topic, dataDir string) (*TopicStore, error) {
	filePath := filepath.Join(dataDir, topic+".log")
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, err
	}

	ts := &TopicStore{
		topic:    topic,
		filePath: filePath,
		index:    make(map[uint64]IndexEntry),
	}

	if err := ts.Recover(); err != nil {
		return nil, err
	}

	return ts, nil
}

// Recover builds the offset index by reading the file sequentially on startup.
func (ts *TopicStore) Recover() error {
	file, err := os.OpenFile(ts.filePath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	ts.file = file

	var pos int64 = 0
	reader := bufio.NewReader(file)
	for {
		line, err := reader.ReadBytes('\n')
		if len(line) > 0 {
			var msg protocol.Message
			if err2 := json.Unmarshal(line, &msg); err2 == nil {
				ts.index[msg.Offset] = IndexEntry{Pos: pos, Len: len(line)}
				if msg.Offset > ts.maxOffset {
					ts.maxOffset = msg.Offset
				}
			}
			pos += int64(len(line))
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			_ = file.Close()
			return err
		}
	}

	log.Printf("[Topic:%s] Recovered. Messages: %d, MaxOffset: %d", ts.topic, len(ts.index), ts.maxOffset)
	return nil
}

// Publish appends a message to the log, updates the index, and syncs to disk.
func (ts *TopicStore) Publish(payload string) (uint64, error) {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	offset := ts.maxOffset + 1
	msg := protocol.Message{Offset: offset, Payload: payload}
	data, err := json.Marshal(msg)
	if err != nil {
		return 0, err
	}
	data = append(data, '\n')

	stat, err := ts.file.Stat()
	if err != nil {
		return 0, err
	}
	bytePos := stat.Size()

	if _, err := ts.file.Write(data); err != nil {
		return 0, err
	}

	// Guarantee physical persistence
	if err := ts.file.Sync(); err != nil {
		return 0, err
	}

	ts.indexMu.Lock()
	ts.index[offset] = IndexEntry{Pos: bytePos, Len: len(data)}
	ts.maxOffset = offset
	ts.indexMu.Unlock()

	return offset, nil
}

// ReadOffsets retrieves messages for specified offsets concurrently using ReadAt.
func (ts *TopicStore) ReadOffsets(offsets []uint64) []protocol.Message {
	ts.indexMu.RLock()
	defer ts.indexMu.RUnlock()

	var msgs []protocol.Message
	for _, offset := range offsets {
		entry, exists := ts.index[offset]
		if !exists {
			continue
		}

		buf := make([]byte, entry.Len)
		_, err := ts.file.ReadAt(buf, entry.Pos)
		if err != nil {
			log.Printf("[Topic:%s] Error reading offset %d at position %d: %v", ts.topic, offset, entry.Pos, err)
			continue
		}

		var msg protocol.Message
		if err := json.Unmarshal(buf, &msg); err == nil {
			msgs = append(msgs, msg)
		}
	}
	return msgs
}

func (ts *TopicStore) MaxOffset() uint64 {
	ts.indexMu.RLock()
	defer ts.indexMu.RUnlock()
	return ts.maxOffset
}

func (ts *TopicStore) Close() error {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	if ts.file != nil {
		return ts.file.Close()
	}
	return nil
}

// Broker implements the Service Broker server.
type Broker struct {
	id           string
	port         int
	qmAddr       string
	dataDir      string
	externalAddr string
	client       *http.Client

	storesMu     sync.RWMutex
	topicStores  map[string]*TopicStore
}

func NewBroker(id string, port int, qmAddr, dataDir, externalAddr string) *Broker {
	if externalAddr == "" {
		externalAddr = fmt.Sprintf("http://localhost:%d", port)
	}
	return &Broker{
		id:           id,
		port:         port,
		qmAddr:       qmAddr,
		dataDir:      dataDir,
		externalAddr: externalAddr,
		client:       &http.Client{Timeout: 5 * time.Second},
		topicStores:  make(map[string]*TopicStore),
	}
}

func (b *Broker) getOrCreateTopicStore(topic string) (*TopicStore, error) {
	b.storesMu.RLock()
	ts, exists := b.topicStores[topic]
	b.storesMu.RUnlock()
	if exists {
		return ts, nil
	}

	b.storesMu.Lock()
	defer b.storesMu.Unlock()
	
	// Double check
	ts, exists = b.topicStores[topic]
	if exists {
		return ts, nil
	}

	ts, err := NewTopicStore(topic, b.dataDir)
	if err != nil {
		return nil, err
	}
	b.topicStores[topic] = ts

	// Register the topic with the Queue Manager asynchronously
	go b.registerTopicWithQM(topic)

	return ts, nil
}

func (b *Broker) registerWithQM() error {
	reqBody, _ := json.Marshal(protocol.BrokerRegisterRequest{
		ID:      b.id,
		Address: b.externalAddr,
	})

	resp, err := b.client.Post(b.qmAddr+"/brokers/register", "application/json", bytes.NewBuffer(reqBody))
	if err != nil {
		return err
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("QM returned status code %d", resp.StatusCode)
	}
	return nil
}

func (b *Broker) sendHeartbeat() {
	reqBody, _ := json.Marshal(protocol.BrokerRegisterRequest{
		ID:      b.id,
		Address: b.externalAddr,
	})

	resp, err := b.client.Post(b.qmAddr+"/brokers/heartbeat", "application/json", bytes.NewBuffer(reqBody))
	if err == nil {
		_ = resp.Body.Close()
	}
}

func (b *Broker) registerTopicWithQM(topic string) {
	reqBody, _ := json.Marshal(protocol.TopicRegisterRequest{
		Topic:    topic,
		BrokerID: b.id,
	})

	resp, err := b.client.Post(b.qmAddr+"/topics/register", "application/json", bytes.NewBuffer(reqBody))
	if err != nil {
		log.Printf("[Broker:%s] Failed to register topic %q with QM: %v", b.id, topic, err)
		return
	}
	_ = resp.Body.Close()
}

func (b *Broker) handlePublish(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req protocol.PublishRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if req.Topic == "" {
		http.Error(w, "topic name required", http.StatusBadRequest)
		return
	}

	ts, err := b.getOrCreateTopicStore(req.Topic)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to load topic store: %v", err), http.StatusInternalServerError)
		return
	}

	offset, err := ts.Publish(req.Payload)
	if err != nil {
		http.Error(w, fmt.Sprintf("Write error: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(protocol.PublishResponse{Offset: offset})
}

func (b *Broker) handleFetch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req protocol.FetchRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if req.Topic == "" || req.Group == "" || req.SubscriberID == "" {
		http.Error(w, "Topic, Group, and SubscriberID are required", http.StatusBadRequest)
		return
	}

	ts, err := b.getOrCreateTopicStore(req.Topic)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to load topic store: %v", err), http.StatusInternalServerError)
		return
	}

	maxOffset := ts.MaxOffset()

	// Negotiate offset leases with the Queue Manager
	leaseReq, _ := json.Marshal(protocol.QMLeaseRequest{
		Topic:           req.Topic,
		Group:           req.Group,
		SubscriberID:    req.SubscriberID,
		Limit:           req.Limit,
		BrokerMaxOffset: maxOffset,
	})

	resp, err := b.client.Post(b.qmAddr+"/qm/lease", "application/json", bytes.NewBuffer(leaseReq))
	if err != nil {
		http.Error(w, fmt.Sprintf("QM communication failure: %v", err), http.StatusInternalServerError)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		http.Error(w, "QM lease negotiation failed", http.StatusInternalServerError)
		return
	}

	var leaseResp protocol.QMLeaseResponse
	if err := json.NewDecoder(resp.Body).Decode(&leaseResp); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	var msgs []protocol.Message
	if len(leaseResp.Offsets) > 0 {
		msgs = ts.ReadOffsets(leaseResp.Offsets)
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(protocol.FetchResponse{Messages: msgs})
}

func (b *Broker) handleAck(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req protocol.AckRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Forward ACK request directly to Queue Manager
	qmAckBody, _ := json.Marshal(protocol.QMAckRequest{
		Topic:        req.Topic,
		Group:        req.Group,
		SubscriberID: req.SubscriberID,
		Offsets:      req.Offsets,
	})

	resp, err := b.client.Post(b.qmAddr+"/qm/ack", "application/json", bytes.NewBuffer(qmAckBody))
	if err != nil {
		http.Error(w, fmt.Sprintf("QM forward ACK failure: %v", err), http.StatusInternalServerError)
		return
	}
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		http.Error(w, "QM failed to commit offsets", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func main() {
	id := flag.String("id", "broker-1", "Broker unique ID")
	port := flag.Int("port", 8081, "Port for Broker")
	qmAddr := flag.String("qm", "http://localhost:8080", "Queue Manager Address")
	dataDir := flag.String("data", "data/broker", "Data directory for log files")
	externalAddr := flag.String("addr", "", "Broker external address (default http://localhost:<port>)")
	flag.Parse()

	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	instanceDataDir := filepath.Join(*dataDir, *id)
	b := NewBroker(*id, *port, *qmAddr, instanceDataDir, *externalAddr)

	// Discover and reload existing topic files in the dataDir
	files, err := os.ReadDir(instanceDataDir)
	if err == nil {
		for _, f := range files {
			if !f.IsDir() && filepath.Ext(f.Name()) == ".log" {
				topic := f.Name()[:len(f.Name())-4]
				if _, err2 := b.getOrCreateTopicStore(topic); err2 != nil {
					log.Printf("[Broker:%s] Failed to reload topic %q: %v", b.id, topic, err2)
				}
			}
		}
	}

	// Dynamic registration loop on startup (retries until QM is online)
	for {
		if err := b.registerWithQM(); err != nil {
			log.Printf("[Broker:%s] Waiting for Queue Manager to come online: %v", b.id, err)
			time.Sleep(1 * time.Second)
			continue
		}
		break
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Active heartbeat ticker
	go func(ctx context.Context) {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				b.sendHeartbeat()
			}
		}
	}(ctx)

	mux := http.NewServeMux()
	mux.HandleFunc("/publish", b.handlePublish)
	mux.HandleFunc("/fetch", b.handleFetch)
	mux.HandleFunc("/ack", b.handleAck)

	srv := &http.Server{
		Addr:    fmt.Sprintf(":%d", *port),
		Handler: mux,
	}

	go func() {
		log.Printf("[Broker:%s] Listening on %s", b.id, srv.Addr)
		if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("[Broker:%s] Startup error: %v", b.id, err)
		}
	}()

	// Graceful shutdown handling
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	log.Printf("[Broker:%s] Shutting down gracefully...", b.id)
	cancel()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("[Broker:%s] Graceful shutdown failed: %v", err)
	}

	// Safely close all database file handles
	b.storesMu.Lock()
	for _, store := range b.topicStores {
		if err := store.Close(); err != nil {
			log.Printf("[Broker:%s] Error closing store %q: %v", b.id, store.topic, err)
		}
	}
	b.storesMu.Unlock()

	log.Printf("[Broker:%s] Shutdown complete. Bye!", b.id)
}
