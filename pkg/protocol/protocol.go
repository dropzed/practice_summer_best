package protocol

// Message представляет одно сообщение, опубликованное в топик.
type Message struct {
	Offset  uint64 `json:"offset"`
	Payload string `json:"payload"`
}

// PublishRequest отправляется издателями для записи сообщения в топик.
type PublishRequest struct {
	Topic   string `json:"topic"`
	Payload string `json:"payload"`
}

// PublishResponse возвращается при успешной записи.
type PublishResponse struct {
	Offset uint64 `json:"offset"`
}

// FetchRequest отправляется подписчиками через SDK для получения сообщений.
type FetchRequest struct {
	Topic        string `json:"topic"`
	Group        string `json:"group"`
	SubscriberID string `json:"subscriber_id"`
	Limit        int    `json:"limit"`
}

// FetchResponse возвращается подписчикам с арендованными сообщениями.
type FetchResponse struct {
	Messages []Message `json:"messages"`
}

// AckRequest отправляется подписчиками через SDK для подтверждения обработки.
type AckRequest struct {
	Topic        string   `json:"topic"`
	Group        string   `json:"group"`
	SubscriberID string   `json:"subscriber_id"`
	Offsets      []uint64 `json:"offsets"`
}

// BrokerRegisterRequest отправляется брокерами при регистрации и хартбитах.
type BrokerRegisterRequest struct {
	ID      string `json:"id"`
	Address string `json:"address"`
}

// TopicRegisterRequest отправляется брокерами для регистрации топика.
type TopicRegisterRequest struct {
	Topic    string `json:"topic"`
	BrokerID string `json:"broker_id"`
}

// QMLeaseRequest отправляется брокером в Queue Manager для согласования офсетов.
type QMLeaseRequest struct {
	Topic           string `json:"topic"`
	Group           string `json:"group"`
	SubscriberID    string `json:"subscriber_id"`
	Limit           int    `json:"limit"`
	BrokerMaxOffset uint64 `json:"broker_max_offset"`
}

// QMLeaseResponse содержит список арендованных офсетов.
type QMLeaseResponse struct {
	Offsets []uint64 `json:"offsets"`
}

// QMAckRequest отправляется брокером в Queue Manager для фиксации офсетов.
type QMAckRequest struct {
	Topic        string   `json:"topic"`
	Group        string   `json:"group"`
	SubscriberID string   `json:"subscriber_id"`
	Offsets      []uint64 `json:"offsets"`
}

// GroupStatus определяет статус группы потребителей.
type GroupStatus struct {
	CommittedOffset   uint64   `json:"committed_offset"`
	PendingAcksCount  int      `json:"pending_acks_count"`
	ActiveSubscribers []string `json:"active_subscribers"`
}

// TopicStatus определяет статус топика.
type TopicStatus struct {
	BrokerID     string                 `json:"broker_id"`
	ActiveGroups map[string]GroupStatus `json:"active_groups"`
}

// StatusResponse возвращается эндпоинтом /status и показывает топологию кластера.
type StatusResponse struct {
	ActiveBrokers map[string]string      `json:"active_brokers"` // ID -> Адрес
	Topics        map[string]TopicStatus `json:"topics"`
}
