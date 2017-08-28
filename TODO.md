TODO
====

### Sharding

* Syncer
    * Shard File structure (shd/00-ff)
    * ShardWriter (shard = fnv32a(id)[:2])
    * ShardScanner
    * ShardConsumer
    * Wire Protocol
    * Routing Rule

### Benchmark

* Writer
* Reader
* Single writer and multiple readers

### Optimization

* Message
    * Read: reuse buffer and prevent make
