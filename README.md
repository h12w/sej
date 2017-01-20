sej: File-based Segmented Journal (Queue)
=========================================

Directory Structure
-------------------

```
[root-dir]/
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
segment_file = { offset timestamp crc size message size } .
offset       = uint64    .
timestamp    = int64     .
size         = int32     .
message      = { uint8 } .
```

All integers are written in the big endian format.

 name      | description
--------   | -----------------------------------------------------------
 offset    | the position of the message in the queue
 timestamp | the timestamp represented in nanoseconds since Unix Epoch
 size      | the size of the message, allowing reading both forward and backward
 message   | the encoded message
