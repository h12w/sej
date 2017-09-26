sej: Segmented Journals
=======================

`h12.me/sej` provides composable components for implementing persisted message queue and allow the devleoper to trade off between reliablilty, latency and throughput with minimal devops overhead.

Package Organization
--------------------

* h12.me/sej: writer, scanner and offset
    * shard: sharding
    * wire: copying across machines
    * cmd/sej: command line tool

SEJ Directory
-------------

```
[sej-dir]/
    jnl.lck
    jnl/
        0000000000000000.jnl
        000000001f9e521e.jnl
        ......
    ofs/
        reader1.ofs
        reader1.lck
        reader2.ofs
        reader2.lck
        ......
```

Journal File format
-------------------

```
segment_file = { message }                          .
message      = offset timestamp type key value size .
offset       = uint64                               .
timestamp    = int64                                .
type         = uint8                                .
key          = key_size { uint8 }                   .
key_size     = int8                                 .
value        = value_size { uint8 }                 .
value_size   = int32                                .
size         = int32                                .
```

All integers are written in the big endian format.

 name      | description
--------   | -----------------------------------------------------------
 offset    | the position of the message in the queue
 timestamp | the timestamp represented in nanoseconds since Unix Epoch
 type      | an int8 value that could be used to indicate the type of the message
 key       | the encoded key
 value     | the encoded value
 size      | the size of the whole message including itself, allowing reading backward

Writer
------

* Append from the last offset in segmented journal files
* File lock to prevent other writers from opening the journal files
* Startup corruption detection & truncation

Scanner
-------

* Read from an offset in segmented journal files
* Change monitoring
    - directory
    - file append
* Handle incomplete last message
* Truncation detection & fail fast
* Timeout

Offset
------

* First/last offset
* Offset persistence

Sharding
--------

```
[root-path]/
    [shard0]/
    [shard1]/
    ......
```

Each shard directory is a SEJ directory with a name in the form of `[prefix].[shard-bit].[shard-index]`.

* prefix must satisfy [a-zA-Z0-9_\-]*
* when prefix is empty, `[prefix].` including the dot is omitted
* shard-bit: 1, 2, ..., 9, a
* shard-index: 000, 001, ..., 3ff


Hub
---

```
[root-dir]/
    [client-id0].[shard0]/
    [client-id1].[shard0]/
    ......
```

client-dir is the SEJ directory name belonging to a client.
