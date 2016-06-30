TODO
====

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

### Command line tool

* range
* dump
* tail
* count
* rollback
* clean
	- delete files according to cleaning rules

### Benchmark

* Writer
    - Sync
    - Async
* Reader
* Single writer and multiple readers
