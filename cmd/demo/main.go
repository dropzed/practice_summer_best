package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"message-broker/pkg/protocol"
	"message-broker/pkg/sdk"
)

func main() {
	mode := flag.String("mode", "publish", "Execution mode: publish, subscribe, status")
	topic := flag.String("topic", "test-topic", "Topic name")
	group := flag.String("group", "group-1", "Consumer Group ID (for subscribe)")
	id := flag.String("id", "sub-1", "Subscriber unique ID (for subscribe)")
	count := flag.Int("count", 1, "Number of messages to publish or consume")
	payload := flag.String("payload", "hello", "Message payload prefix (for publish)")
	delay := flag.Int("delay", 0, "Processing delay in milliseconds (for subscribe)")
	rate := flag.Int("rate", 0, "Publish delay in milliseconds between messages")
	qmAddr := flag.String("qm", "http://localhost:8080", "Queue Manager Address")
	limit := flag.Int("limit", 10, "Message fetch limit per request (for subscribe)")
	flag.Parse()

	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	// Context that cancels on SIGINT/SIGTERM
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	switch *mode {
	case "publish":
		pub := sdk.NewPublisher(*qmAddr)
		log.Printf("[Demo] Publishing %d messages to topic %q...", *count, *topic)
		
		start := time.Now()
		for i := 1; i <= *count; i++ {
			select {
			case <-ctx.Done():
				log.Printf("[Demo] Publish cancelled by signal. Exiting.")
				return
			default:
			}

			msgPayload := fmt.Sprintf("%s-%d", *payload, i)
			offset, err := pub.Publish(ctx, *topic, msgPayload)
			if err != nil {
				log.Fatalf("[Demo] Publish failed at msg %d: %v", i, err)
			}

			if *count <= 10 || i%100 == 0 || i == *count {
				log.Printf("[Demo] Published message %q at offset %d", msgPayload, offset)
			}

			if *rate > 0 {
				select {
				case <-ctx.Done():
				case <-time.After(time.Duration(*rate) * time.Millisecond):
				}
			}
		}
		log.Printf("[Demo] Successfully published %d messages in %v", *count, time.Since(start))

	case "subscribe":
		sub := sdk.NewSubscriber(*qmAddr, *group, *id)
		log.Printf("[Demo] Subscriber %q in group %q starting for topic %q to read %d messages...", *id, *group, *topic, *count)

		var (
			consumedCount = 0
			mu            sync.Mutex
			subCtx, cancel = context.WithCancel(ctx)
		)
		defer cancel()

		// Start subscription loop in background
		go func() {
			err := sub.Subscribe(subCtx, *topic, *limit, 200*time.Millisecond, func(msg protocol.Message) error {
				mu.Lock()
				defer mu.Unlock()

				consumedCount++
				log.Printf("[%s] Received: offset=%d, payload=%q (Total consumed: %d/%d)", *id, msg.Offset, msg.Payload, consumedCount, *count)

				if *delay > 0 {
					time.Sleep(time.Duration(*delay) * time.Millisecond)
				}

				if consumedCount >= *count {
					cancel() // Stop the subscription loop cleanly by cancelling context
				}
				return nil
			})
			if err != nil && !errorsIsContextCancelled(err) {
				log.Printf("[Demo] Subscriber loop error: %v", err)
			}
		}()

		// Wait for completion or timeout
		select {
		case <-subCtx.Done():
			mu.Lock()
			if consumedCount >= *count {
				log.Printf("[Demo] Reached target count of %d messages. Gracefully exiting.", *count)
			} else {
				log.Printf("[Demo] Cancelled. Consumed %d/%d messages.", consumedCount, *count)
			}
			mu.Unlock()
			// Give a small moment for final ACKs to complete processing
			time.Sleep(200 * time.Millisecond)
		case <-time.After(65 * time.Second): // Safety timeout
			mu.Lock()
			log.Printf("[Demo] Safety timeout reached. Consumed %d/%d messages. Exiting.", consumedCount, *count)
			mu.Unlock()
		}

	case "status":
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, *qmAddr+"/status", nil)
		if err != nil {
			log.Fatalf("[Demo] Failed to create status request: %v", err)
		}

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			log.Fatalf("[Demo] Failed to fetch status: %v", err)
		}
		defer resp.Body.Close()

		var status map[string]interface{}
		if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
			log.Fatalf("[Demo] Failed to decode status response: %v", err)
		}

		pretty, _ := json.MarshalIndent(status, "", "  ")
		fmt.Println(string(pretty))

	default:
		log.Fatalf("[Demo] Unknown execution mode: %s", *mode)
	}
}

func errorsIsContextCancelled(err error) bool {
	return err != nil && (err == context.Canceled || err.Error() == "context canceled")
}
