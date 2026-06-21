package protocol

// Message represents a single message published to a topic.
type Message struct {
	Offset  uint64 `json:"offset"`
	Payload string `json:"payload"`
}

// PublishRequest is sent by publishers to write a message to a topic.
type PublishRequest struct {
	Topic   string `json:"topic"`
	Payload string `json:"payload"`
}

// PublishResponse is returned upon successful writing.
type PublishResponse struct {
	Offset uint64 `json:"offset"`
}

// FetchRequest is sent by subscribers via the SDK to poll messages.
type FetchRequest struct {
	Topic        string `json:"topic"`
	Group        string `json:"group"`
	SubscriberID string `json:"subscriber_id"`
	Limit        int    `json:"limit"`
}

// FetchResponse is returned to subscribers with the leased messages.
type FetchResponse struct {
	Messages []Message `json:"messages"`
}

// AckRequest is sent by subscribers via the SDK to confirm message processing.
type AckRequest struct {
	Topic        string   `json:"topic"`
	Group        string   `json:"group"`
	SubscriberID string   `json:"subscriber_id"`
	Offsets      []uint64 `json:"offsets"`
}

// BrokerRegisterRequest is sent by brokers on registration and heartbeats.
type BrokerRegisterRequest struct {
	ID      string `json:"id"`
	Address string `json:"address"`
}

// TopicRegisterRequest is sent by brokers to register support for a topic.
type TopicRegisterRequest struct {
	Topic    string `json:"topic"`
	BrokerID string `json:"broker_id"`
}

// QMLeaseRequest is sent by the Broker to the Queue Manager to negotiate offsets.
type QMLeaseRequest struct {
	Topic           string `json:"topic"`
	Group           string `json:"group"`
	SubscriberID    string `json:"subscriber_id"`
	Limit           int    `json:"limit"`
	BrokerMaxOffset uint64 `json:"broker_max_offset"`
}

// QMLeaseResponse contains the list of offsets leased to the subscriber.
type QMLeaseResponse struct {
	Offsets []uint64 `json:"offsets"`
}

// QMAckRequest is sent by the Broker to the Queue Manager to commit offsets.
type QMAckRequest struct {
	Topic        string   `json:"topic"`
	Group        string   `json:"group"`
	SubscriberID string   `json:"subscriber_id"`
	Offsets      []uint64 `json:"offsets"`
}

// GroupStatus defines status representation for a consumer group.
type GroupStatus struct {
	CommittedOffset   uint64   `json:"committed_offset"`
	PendingAcksCount  int      `json:"pending_acks_count"`
	ActiveSubscribers []string `json:"active_subscribers"`
}

// TopicStatus defines status representation for a topic.
type TopicStatus struct {
	BrokerID     string                 `json:"broker_id"`
	ActiveGroups map[string]GroupStatus `json:"active_groups"`
}

// StatusResponse is returned by /status showing the cluster topology.
type StatusResponse struct {
	ActiveBrokers map[string]string      `json:"active_brokers"` // ID -> Address
	Topics        map[string]TopicStatus `json:"topics"`
}
