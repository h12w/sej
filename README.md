fq: file-based persistent queue
===============================

File format
-----------

```
segment_file = { offset size crc32 message } .
offset       = uint64    .
size         = int32     .
crc32        = uint32    .
message      = { uint8 } .
```

 name    | description
-------- | -----------------------------------------------------------
 offset  | the position of the message in the queue
 size    | the size of the message
 crc32   | the CRC-32 checksum of the message
 message | the encoded message
