sej: File-based Segmented Journal (queue)
=========================================

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
* Read segment files (done)
* monitoring
    - dir (done)
    - append (done)
    - append or dir (done)
* Offset persistence

### Writer

* Write from the last offset (done)
* Segmentation (done)
* Lock to prevent other writer (done)
* startup corruption detection (done)

### Cleaner

* delete files according to cleaning rules

### Benchmark

* Writer
    - Sync
    - Async
* Reader
* Single writer and multiple readers
