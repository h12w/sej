fq: file-based persistent queue
===============================

File format
-----------

```
segment_file = { offset size crc32 message } .
offset       = uint64    .
size         = int32     .
crc          = uint32    .
message      = { uint8 } .
```

All integers are written in the big endian format.

 name    | description
-------- | -----------------------------------------------------------
 offset  | the position of the message in the queue
 size    | the size of the message
 crc     | the CRC-32 checksum (using the IEEE polynomial) of the message
 message | the encoded message

TODO
----

### Reader

* Search offset (done)
* Tail/file/dir monitoring
* Offset persistence
* Lock to prevent deletion

### Writer

* Write from the last offset
* Segmentation
* Lock to prevent other writer
