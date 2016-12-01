TODO
====

### Reader

* Search offset (done)
* Read segment files (done)
* monitoring
    - dir (done)
    - append (done)
    - append or dir (done)
* handle truncation of the last message (done)
* optional checking CRC (done)
* Offset persistence (done)

### Writer

* Write from the last offset (done)
* Segmentation (done)
* Lock to prevent other writer (done)
* startup corruption detection (done)
* startup corruption correction (done)

### Command line tool

* dump (done)
* last-offset (done)
* tail (done)
* clean (done)
* range
* count
* rollback

### Benchmark

* Writer
    - Sync
    - Async
* Reader
* Single writer and multiple readers
