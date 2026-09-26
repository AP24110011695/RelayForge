# Protocol (v1)

The gRPC/Protobuf API of the RelayForge broker. Schema:
`api/proto/relayforge/v1/relayforge.proto` (package `relayforge.v1`).
Unary RPCs; regeneration must be diff-idempotent (`make proto`).

## RPCs (Phase 5)

- **Produce**(topic, partition, ack_mode, messages[]) → offsets[]
  - Appends messages in order; one offset per message in request order.
  - Ack modes map to the storage ladder (docs/GUARANTEES.md):
    `ACK_MODE_UNSPECIFIED` → `WRITTEN` (default), `NONE`, `WRITTEN`,
    `FLUSHED`.
  - Batch limits: ≤ 1000 messages, ≤ 8 MiB total key+value bytes;
    per-record limits are the engine's (default 4 MiB per record).
  - If an append fails partway, the RPC returns an error and no response:
    records that already landed are visible to consumers but were never
    acknowledged — a producer retry duplicates them. This is the
    at-least-once contract working as designed.
- **Fetch**(topic, partition, offset, max_bytes, max_wait_ms) →
  records[], next_offset, high_watermark
  - Reading exactly at `high_watermark` long-polls up to `max_wait_ms`
    for new data (polling implementation; a push-based wakeup is a Phase 6
    refinement). `max_wait_ms = 0` returns immediately.
  - An offset beyond `high_watermark` fails with `OUT_OF_RANGE`.
  - `max_bytes <= 0` selects the server default (1 MiB); the hard cap is
    8 MiB regardless of request.
  - A batch never crosses a segment boundary; resume from `next_offset`.
  - Records carry `offset`, `key`, `value`, `timestamp_ms` (broker-stamped
    at append when the producer supplied none).

## Status-code mapping

| Condition | gRPC code |
| --- | --- |
| Unknown topic or partition | `NotFound` |
| Fetch offset beyond high watermark | `OutOfRange` |
| Produce batch empty | `InvalidArgument` |
| Batch or record size limits exceeded | `ResourceExhausted` |
| Broker shutting down / log closed | `Unavailable` |
| Client deadline exceeded / canceled | `DeadlineExceeded` / `Canceled` |
| Panic in a handler (recovery interceptor) | `Internal` |
| Anything unexpected | `Internal` |

## Limits

| Limit | Value |
| --- | --- |
| gRPC max receive / send message size | 16 MiB |
| Produce batch messages | 1000 |
| Produce batch bytes | 8 MiB |
| Record size (engine default) | 4 MiB |
| Fetch default / hard cap | 1 MiB / 8 MiB |

## Service surface

- `relayforge.v1.RelayForge` — Produce, Fetch.
- `grpc.health.v1.Health` — serving status (SERVING once the broker is
  recovered and serving).

## Evolution policy

- v1 evolves **additively**: new RPCs and fields are appended; existing
  field numbers never change meaning. Topics/partitions/groups arrive as
  new RPCs and fields in later phases; the topic/partition fields have been
  present since v1.0 so the schema never churns.
- There is deliberately no schema registry, no Kafka compatibility, and no
  streaming RPCs in the MVP (unary only).

## Security

TLS and authentication are explicit non-goals for the MVP; the broker
listens in plain mode for local/lab use.
