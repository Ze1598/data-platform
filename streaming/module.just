# Composite: sequences kafka -> flink -> producer, the real dependency order
# (the topic must exist before Flink's Kafka source table can read it; Flink's
# sink job should be running before the producer starts writing, so events
# aren't silently missed before the sink job's consumer group is registered).
# Each service also remains independently addressable: `just kafka::start`, etc.

start:
    just kafka::start
    just flink::start
    just producer::start

kill:
    just producer::kill
    just flink::kill
    just kafka::kill
