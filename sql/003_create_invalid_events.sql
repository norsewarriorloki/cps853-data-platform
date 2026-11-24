CREATE TABLE IF NOT EXISTS invalid_kafka_events (
    topic VARCHAR(100) NOT NULL,
    partition_id INTEGER NOT NULL,
    message_offset BIGINT NOT NULL,
    raw_message BYTEA,
    rejection_reasons TEXT[] NOT NULL,
    rejected_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (topic, partition_id, message_offset)
);