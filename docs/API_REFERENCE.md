# API Reference

This document describes the API endpoints and commands supported by EagleDB.

## Connection

EagleDB uses the Redis RESP protocol for client-server communication.

Connect using any Redis-compatible client on the configured port (default: 6379).

## Commands

### Connection Commands

#### PING [message]

- Test server connectivity
- Returns: "PONG" or the message if provided

#### ECHO message

- Returns the message
- Returns: Bulk String Reply

### Key-Value Operations

#### SET key value [EX seconds | PX milliseconds | EXAT timestamp | PXAT timestamp] [NX | XX] [GET] [KEEPTTL]

- Sets the string value of a key
- Options:
  - `EX seconds` - Set expiry in seconds
  - `PX milliseconds` - Set expiry in milliseconds
  - `EXAT timestamp` - Set expiry at Unix timestamp (seconds)
  - `PXAT timestamp` - Set expiry at Unix timestamp (milliseconds)
  - `NX` - Only set if key does not exist
  - `XX` - Only set if key exists
  - `GET` - Return old value
  - `KEEPTTL` - Retain existing TTL
- Returns: Simple String Reply ("OK") or Null if NX/XX condition not met

#### GET key

- Get the value of key
- Returns: Bulk String Reply or Null if key does not exist

#### DEL key [key ...]

- Delete one or more keys
- Returns: Integer Reply (number of keys deleted)

#### MGET key [key ...]

- Get values of multiple keys
- Returns: Array Reply (values or Null for missing keys)

#### MSET key value [key value ...]

- Set multiple key-value pairs
- Returns: Simple String Reply ("OK")

### String Operations

#### INCR key

- Increment integer value by 1
- Returns: Integer Reply (new value)
- Error: If value is not an integer or would overflow

#### DECR key

- Decrement integer value by 1
- Returns: Integer Reply (new value)
- Error: If value is not an integer or would overflow

#### INCRBY key increment

- Increment integer value by specified amount
- Returns: Integer Reply (new value)
- Error: If value is not an integer or would overflow
- Note: Preserves existing TTL

#### DECRBY key decrement

- Decrement integer value by specified amount
- Returns: Integer Reply (new value)
- Error: If value is not an integer or would overflow

#### INCRBYFLOAT key increment

- Increment float value by specified amount
- Returns: Bulk String Reply (new value as string)
- Error: If value is not a valid float, or result is NaN/Infinity

#### APPEND key value

- Append value to existing string
- Creates key if it doesn't exist
- Returns: Integer Reply (length of string after append)
- Note: Preserves existing TTL

#### STRLEN key

- Get length of string value
- Returns: Integer Reply (length, or 0 if key doesn't exist)

#### GETRANGE key start end

- Get substring of string value
- Supports negative indices (-1 = last character)
- Returns: Bulk String Reply

#### SETRANGE key offset value

- Overwrite part of string at offset
- Pads with null bytes if offset exceeds current length
- Returns: Integer Reply (length of string after operation)
- Note: Preserves existing TTL

### Generic Key Commands

#### EXISTS key [key ...]

- Check if keys exist
- Returns: Integer Reply (count of existing keys)
- Note: Duplicate keys are counted multiple times

#### TYPE key

- Get the type of value stored at key
- Returns: Simple String Reply ("string", "hash", or "none")

#### RENAME key newkey

- Rename a key
- Overwrites newkey if it exists
- Returns: Simple String Reply ("OK")
- Error: If key does not exist

#### RENAMENX key newkey

- Rename key only if newkey does not exist
- Returns: Integer Reply (1 if renamed, 0 if newkey exists)
- Error: If key does not exist

### TTL Commands

#### EXPIRE key seconds

- Set key expiry in seconds
- Returns: Integer Reply (1 if set, 0 if key doesn't exist)

#### PEXPIRE key milliseconds

- Set key expiry in milliseconds
- Returns: Integer Reply (1 if set, 0 if key doesn't exist)

#### EXPIREAT key timestamp

- Set key expiry at Unix timestamp (seconds)
- Returns: Integer Reply (1 if set, 0 if key doesn't exist)

#### PEXPIREAT key timestamp

- Set key expiry at Unix timestamp (milliseconds)
- Returns: Integer Reply (1 if set, 0 if key doesn't exist)

#### TTL key

- Get remaining time to live in seconds
- Returns: Integer Reply (seconds, -1 if no expiry, -2 if key doesn't exist)

#### PTTL key

- Get remaining time to live in milliseconds
- Returns: Integer Reply (milliseconds, -1 if no expiry, -2 if key doesn't exist)

#### PERSIST key

- Remove expiry from key
- Returns: Integer Reply (1 if removed, 0 if key doesn't exist or has no expiry)

### Hash Operations

#### HSET key field value [field value ...]

- Set field(s) in hash stored at key
- Returns: Integer Reply (number of new fields added)

#### HGET key field

- Get value of hash field
- Returns: Bulk String Reply or Null if field doesn't exist

#### HDEL key field [field ...]

- Delete hash field(s)
- Returns: Integer Reply (number of fields deleted)

### Cluster Operations

#### CLUSTER INFO

- Provides information about the cluster state
- Returns: Bulk String Reply

#### CLUSTER NODES

- Lists all known cluster nodes
- Returns: Bulk String Reply

## Error Handling

Errors are returned as RESP Error messages with the following format:

```text
-ERR error message
```

Common error types:

- `ERR wrong number of arguments` - Invalid argument count
- `WRONGTYPE` - Operation against wrong value type
- `ERR value is not an integer` - Expected integer value
- `ERR value is not a valid float` - Expected float value
- `ERR increment would produce NaN or Infinity` - Invalid float result
- `ERR no such key` - Key does not exist (for RENAME)
- `ERR string exceeds maximum allowed size` - Value too large

## Performance Considerations

- Use pipelining for better throughput
- Use MGET/MSET for batch operations
- INCR/INCRBY are atomic and lock-free for reads
- Connection reuse is recommended (encoder buffers are reused per connection)
- Command parsing is optimized with zero-allocation byte matching

## Limits

- Maximum bulk string size: 512 MB
- Maximum array elements: 1,000,000
- Maximum connections: 10,000 (configurable)
- Read timeout: 30 seconds (configurable)
