// Package sdk provides a high-level API for publishing and subscribing
// to topics in the distributed message broker cluster.
package sdk

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"net/http"
	"sync"
	"time"

	"message-broker/pkg/protocol"
)

// Client handles topic routing and HTTP connections.
type Client struct {
	qmAddr      string
	httpClient  *http.Client
	cacheMu     sync.RWMutex
	brokerCache map[string]string // topic -> broker address
}

func NewClient(qmAddr string) *Client {
	// Custom transport with pooled connections and conservative timeouts
	tr := &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   2 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   2 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}

	return &Client{
		qmAddr: qmAddr,
		httpClient: &http.Client{
			Transport: tr,
			Timeout:   5 * time.Second,
		},
		brokerCache: make(map[string]string),
	}
}

// getBrokerAddress resolves the address of the broker hosting the topic, with caching.
func (c *Client) getBrokerAddress(ctx context.Context, topic string, forceRefresh bool) (string, error) {
	if !forceRefresh {
		c.cacheMu.RLock()
		addr, exists := c.brokerCache[topic]
		c.cacheMu.RUnlock()
		if exists {
			return addr, nil
		}
	}

	url := fmt.Sprintf("%s/topics/route?topic=%s", c.qmAddr, topic)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("failed to contact Queue Manager: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusServiceUnavailable {
		return "", errors.New("Queue Manager reported no brokers online")
	} else if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("Queue Manager returned status code %d", resp.StatusCode)
	}

	var res map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return "", fmt.Errorf("failed to parse routing response: %w", err)
	}

	addr, ok := res["address"]
	if !ok || addr == "" {
		return "", errors.New("invalid route address received")
	}

	c.cacheMu.Lock()
	c.brokerCache[topic] = addr
	c.cacheMu.Unlock()

	return addr, nil
}

func (c *Client) invalidateRoute(topic string) {
	c.cacheMu.Lock()
	delete(c.brokerCache, topic)
	c.cacheMu.Unlock()
}

// Publisher implements the publish client.
type Publisher struct {
	client *Client
}

func NewPublisher(qmAddr string) *Publisher {
	return &Publisher{client: NewClient(qmAddr)}
}

// Publish writes a message payload to a specific topic with retry backoff.
func (p *Publisher) Publish(ctx context.Context, topic string, payload string) (uint64, error) {
	var lastErr error
	backoff := 50 * time.Millisecond

	for attempt := 0; attempt < 5; attempt++ {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		default:
		}

		addr, err := p.client.getBrokerAddress(ctx, topic, attempt > 0)
		if err != nil {
			lastErr = err
			p.sleepWithJitter(ctx, backoff)
			backoff *= 2
			continue
		}

		reqBody, _ := json.Marshal(protocol.PublishRequest{
			Topic:   topic,
			Payload: payload,
		})

		req, err := http.NewRequestWithContext(ctx, http.MethodPost, addr+"/publish", bytes.NewBuffer(reqBody))
		if err != nil {
			return 0, err
		}
		req.Header.Set("Content-Type", "application/json")

		resp, err := p.client.httpClient.Do(req)
		if err != nil {
			p.client.invalidateRoute(topic)
			lastErr = err
			p.sleepWithJitter(ctx, backoff)
			backoff *= 2
			continue
		}

		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			p.client.invalidateRoute(topic)
			lastErr = fmt.Errorf("broker responded with status %d: %s", resp.StatusCode, string(body))
			p.sleepWithJitter(ctx, backoff)
			backoff *= 2
			continue
		}

		var pubResp protocol.PublishResponse
		err = json.NewDecoder(resp.Body).Decode(&pubResp)
		_ = resp.Body.Close()
		if err != nil {
			return 0, fmt.Errorf("failed to decode publish response: %w", err)
		}

		return pubResp.Offset, nil
	}

	return 0, fmt.Errorf("publish failed after 5 attempts: %w", lastErr)
}

func (p *Publisher) sleepWithJitter(ctx context.Context, duration time.Duration) {
	// Add full jitter (0 to duration) to avoid thundering herd
	jitter := time.Duration(rand.Int63n(int64(duration)))
	select {
	case <-ctx.Done():
	case <-time.After(jitter):
	}
}

// Subscriber implements the fetch/ack client.
type Subscriber struct {
	client       *Client
	group        string
	subscriberID string
}

func NewSubscriber(qmAddr, group, subscriberID string) *Subscriber {
	return &Subscriber{
		client:       NewClient(qmAddr),
		group:        group,
		subscriberID: subscriberID,
	}
}

// Fetch retrieves a batch of messages from the broker hosting the topic.
func (s *Subscriber) Fetch(ctx context.Context, topic string, limit int) ([]protocol.Message, error) {
	var lastErr error
	backoff := 50 * time.Millisecond

	for attempt := 0; attempt < 5; attempt++ {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		addr, err := s.client.getBrokerAddress(ctx, topic, attempt > 0)
		if err != nil {
			lastErr = err
			s.sleepWithJitter(ctx, backoff)
			backoff *= 2
			continue
		}

		reqBody, _ := json.Marshal(protocol.FetchRequest{
			Topic:        topic,
			Group:        s.group,
			SubscriberID: s.subscriberID,
			Limit:        limit,
		})

		req, err := http.NewRequestWithContext(ctx, http.MethodPost, addr+"/fetch", bytes.NewBuffer(reqBody))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Content-Type", "application/json")

		resp, err := s.client.httpClient.Do(req)
		if err != nil {
			s.client.invalidateRoute(topic)
			lastErr = err
			s.sleepWithJitter(ctx, backoff)
			backoff *= 2
			continue
		}

		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			s.client.invalidateRoute(topic)
			lastErr = fmt.Errorf("broker responded with status %d: %s", resp.StatusCode, string(body))
			s.sleepWithJitter(ctx, backoff)
			backoff *= 2
			continue
		}

		var fetchResp protocol.FetchResponse
		err = json.NewDecoder(resp.Body).Decode(&fetchResp)
		_ = resp.Body.Close()
		if err != nil {
			return nil, fmt.Errorf("failed to parse fetch response: %w", err)
		}

		return fetchResp.Messages, nil
	}

	return nil, fmt.Errorf("fetch failed after 5 attempts: %w", lastErr)
}

// Ack confirms successful processing of message offsets.
func (s *Subscriber) Ack(ctx context.Context, topic string, offsets []uint64) error {
	if len(offsets) == 0 {
		return nil
	}

	var lastErr error
	backoff := 50 * time.Millisecond

	for attempt := 0; attempt < 5; attempt++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		addr, err := s.client.getBrokerAddress(ctx, topic, attempt > 0)
		if err != nil {
			lastErr = err
			s.sleepWithJitter(ctx, backoff)
			backoff *= 2
			continue
		}

		reqBody, _ := json.Marshal(protocol.AckRequest{
			Topic:        topic,
			Group:        s.group,
			SubscriberID: s.subscriberID,
			Offsets:      offsets,
		})

		req, err := http.NewRequestWithContext(ctx, http.MethodPost, addr+"/ack", bytes.NewBuffer(reqBody))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/json")

		resp, err := s.client.httpClient.Do(req)
		if err != nil {
			s.client.invalidateRoute(topic)
			lastErr = err
			s.sleepWithJitter(ctx, backoff)
			backoff *= 2
			continue
		}

		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			s.client.invalidateRoute(topic)
			lastErr = fmt.Errorf("broker responded with status %d: %s", resp.StatusCode, string(body))
			s.sleepWithJitter(ctx, backoff)
			backoff *= 2
			continue
		}

		_ = resp.Body.Close()
		return nil
	}

	return fmt.Errorf("ack failed after 5 attempts: %w", lastErr)
}

func (s *Subscriber) sleepWithJitter(ctx context.Context, duration time.Duration) {
	jitter := time.Duration(rand.Int63n(int64(duration)))
	select {
	case <-ctx.Done():
	case <-time.After(jitter):
	}
}

// Subscribe listens to a topic and executes the handler for received messages in a loop.
// It executes cleanly and cancels immediately when the context is cancelled.
func (s *Subscriber) Subscribe(ctx context.Context, topic string, limit int, pollInterval time.Duration, handler func(protocol.Message) error) error {
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			msgs, err := s.Fetch(ctx, topic, limit)
			if err != nil {
				// Log and retry on next tick
				log.Printf("[Subscriber:%s] Fetch error: %v", s.subscriberID, err)
				continue
			}

			if len(msgs) == 0 {
				continue
			}

			var ackOffsets []uint64
			for _, msg := range msgs {
				// Process message
				if err := handler(msg); err == nil {
					ackOffsets = append(ackOffsets, msg.Offset)
				} else {
					log.Printf("[Subscriber:%s] Handler returned error on offset %d: %v. Message will be re-leased.", s.subscriberID, msg.Offset, err)
				}
			}

			if len(ackOffsets) > 0 {
				if err := s.Ack(context.Background(), topic, ackOffsets); err != nil {
					log.Printf("[Subscriber:%s] Failed to send ACK for offsets %v: %v", s.subscriberID, ackOffsets, err)
				}
			}
		}
	}
}
