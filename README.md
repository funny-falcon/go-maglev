# go-maglev
Implementation (slightly modified) of Google's Maglev balancing algorithm with weights.

https://static.googleusercontent.com/media/research.google.com/ru//pubs/archive/44824.pdf

Documentation:
https://godoc.org/github.com/funny-falcon/go-maglev

It is slightly modified to use power-of-two mapping table and LCG random generator.

Package has no assumption on hash functions to use:
- shards are given as tuple of 64bit hash-sum (of name or whatever) and weight,
- result is just mapping table, you have to lookup by yourself.

```go
	const tableSize = 1<<20 // should be power of two and constant during lifetime of cluster
	shards := []maglev.Shard{
	    {Hash: myhash("shard1", seedShard), Weight: 0.5},
	    {Hash: myhash("shard2", seedShard), Weight: 1},
	    {Hash: myhash("shard3", seedShard), Weight: 2.1},
	}
	table, _ := maglev.BuildTable(shards, tableSize)

	item := myhash("picture1", seedItem)
	shard := table[item%tableSize]
```
