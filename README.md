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
    reader/
        reader1.ofs
        reader1.lck
        reader2.ofs
        reader2.lck
        ......
```

Journal File format
-------------------

```
segment_file = { offset crc32 size message size } .
offset       = uint64    .
crc          = uint32    .
size         = int32     .
message      = { uint8 } .
```

All integers are written in the big endian format.

 name    | description
-------- | -----------------------------------------------------------
 offset  | the position of the message in the queue
 crc     | the CRC-32 checksum (using the IEEE polynomial) of the message
 size    | the size of the message, allowing reading both forward and backward
 message | the encoded message
