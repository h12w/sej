fq: file-based persistent queue
===============================

File format
-----------

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

TODO
----

### Reader

* Search offset (done)
* Tail/file/dir monitoring
* Offset persistence
* Lock to prevent deletion

### Writer

* Write from the last offset (done)
* startup corruption detection
* startup corruption correction
* Segmentation
* Lock to prevent other writer
