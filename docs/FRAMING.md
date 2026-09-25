# Wire Framing

RelayForge frames byte streams with a 4-byte little-endian length prefix
followed by exactly that many payload bytes. `internal/framing` implements
this contract. Phase 3's record format reuses the same length-prefix
discipline on disk (records add CRC/version/timestamp fields on top); this
package stays payload-agnostic and must never grow record fields.

## Layout

```text
+-----------------+--------------------+
| length: 4 bytes | payload: N bytes   |
| little-endian   | opaque bytes       |
+-----------------+--------------------+
```

- The prefix counts payload bytes only (N), not the prefix itself.
- Payloads are opaque: no CRC, no version, no escaping, no reserved bytes.

## Limits

- `ReadFrame(r, max)` allocates only after validating the declared length
  against `max`, so a hostile 4 GiB prefix cannot trigger an allocation bomb.
- `max <= 0` is rejected with `ErrInvalidLimit`.
- Callers choose `max` from their own configuration (the broker will use its
  maximum record size).
- `WriteFrame` rejects payloads that cannot be encoded (the 4-byte prefix
  caps payload size at 2^32-1).

## Error taxonomy

| Error | Meaning | Stream state afterwards |
| --- | --- | --- |
| `io.EOF` | clean end of stream at a frame boundary (zero bytes read) | usable (still at EOF) |
| `io.ErrUnexpectedEOF` | stream ended mid-frame: partial header, or payload shorter than declared | dead — drop the connection |
| `ErrFrameTooLarge` | declared length exceeds `max` | dead — the oversized payload is NOT consumed, so the framing is misaligned |
| any other error | surfaced unchanged from `r` (deadlines, resets, custom readers) | as per the underlying transport |

`WriteFrame` guarantees complete writes even over writers that return short
writes, and surfaces writer errors unchanged. A zero-progress writer (`0,
nil`) is treated as `io.ErrShortWrite` instead of looping forever.

## Rationale

- TCP is a byte stream: without explicit framing, message boundaries do not
  exist.
- Length-prefix framing is the cheapest correct scheme for a log-oriented
  system: one bounded 4-byte read tells the reader the size of the next read.
- Deliberately no escaping, compression, or checksums at this layer:
  integrity and encoding belong to the record format (Phase 3), not to
  transport framing.
- Deadlines and cancellation are not handled here: they belong to the
  underlying `io.Reader`/`io.Writer` (a `net.Conn` with `SetDeadline` shows
  up as `os.ErrDeadlineExceeded` surfaced unchanged, per the taxonomy above).

## Testing

- Round-trip tests over buffers and pipes, including chunked reads
  (`iotest.OneByteReader`) and random-size chunked writes.
- A truncation matrix: a valid frame truncated at every byte offset must
  yield `io.EOF` (cut 0) or `io.ErrUnexpectedEOF` (any other cut), never a
  payload.
- Limit tests: declared length == max passes; max+1 and absurd headers
  (`0xFFFFFFFF`) fail with `ErrFrameTooLarge` without allocating.
- `FuzzReadFrame` asserts the taxonomy invariants over arbitrary input;
  seeds live in `testdata/fuzz/FuzzReadFrame/`.
