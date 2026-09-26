# Record Format v1

The unit of storage in every RelayForge log. This document is the
authoritative reference; `internal/record` implements it and its golden test
pins the layout byte-for-byte against an independently constructed byte
image. The length-prefix framing discipline it builds on is described in
[FRAMING.md](FRAMING.md) (identical prefix rule; this document covers the
record body).

## Layout (little-endian everywhere)

| Offset | Size | Field        | Notes                                                                 |
| ------ | ---- | ------------ | --------------------------------------------------------------------- |
| 0      | 4    | frame length | counts every byte after itself: 22 + len(key) + len(value)            |
| 4      | 4    | CRC32C       | Castagnoli; covers bytes [8, 4 + frameLength): version through value  |
| 8      | 1    | version      | 1 in v1; decoders reject unknown versions                             |
| 9      | 1    | attributes   | reserved; must be 0 in v1                                             |
| 10     | 8    | timestamp    | int64, milliseconds since the Unix epoch, UTC                         |
| 18     | 4    | key length   |                                                                       |
| 22     | var  | key          |                                                                       |
| 22+k   | 4    | value length |                                                                       |
| 26+k   | var  | value        |                                                                       |

- Fixed overhead per record: 26 bytes. Full size = 26 + len(key) + len(value).
- Key and value may be empty; the key is optional.
- The record sits directly on the length-prefix framing contract: the frame
  length counts the CRC field and everything after it.

## Policies

- **Version**: encoders write 1; decoders reject anything else
  (`ErrUnsupportedVersion`). Forward-compat handling (e.g. skipping newer
  records) is deliberately not implemented yet.
- **Attributes**: reserved, must be 0; rejected on both encode and decode.
- **Timestamps**: the log engine stamps records whose timestamp is 0 at
  append time (`time.Now().UnixMilli()`); explicit timestamps are preserved.
- **Size limit**: enforced against the full record size; default 4 MiB
  (`logengine.DefaultMaxRecordSize`), configurable per log.

## Error taxonomy (decoding)

| Error                   | Meaning                                                                |
| ----------------------- | ---------------------------------------------------------------------- |
| `io.EOF`                | clean end at a record boundary (zero bytes read)                       |
| `io.ErrUnexpectedEOF`   | stream/slice ended mid-record                                          |
| `ErrChecksumMismatch`   | stored CRC32C does not match the computed one                          |
| `ErrUnsupportedVersion` | version byte is not 1                                                  |
| `ErrReservedAttributes` | attributes byte is nonzero                                             |
| `ErrMalformed`          | structural violation (frame length below minimum, inconsistent lengths) |
| `ErrRecordTooLarge`     | frame exceeds the configured limit                                     |
| `ErrInvalidLimit`       | a size limit argument is not positive                                  |

Decoded records **alias the input buffer** (zero-copy): callers that retain
records must copy. Allocation for the frame happens only after the declared
length has been validated against the limit, so a hostile length field
cannot trigger an allocation bomb.

## Corruption policy (log engine)

On Open, the log is scanned from position 0. Everything from the first
invalid record onward — a torn tail (crash mid-append) or a corrupt record —
is truncated; the discarded byte count is reported via
`Log.TruncatedOnRecover()`. A transient I/O error during the scan is NOT
treated as corruption: Open fails without modifying the file.

Rationale: after a crash the only safe prefix is the longest fully valid
one, and records after a corrupt record cannot be trusted without a deeper
proof — so they are discarded. The offset sequence continues from the valid
prefix (offsets are never reused).
