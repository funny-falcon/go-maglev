/*
Package maglev implements Google's Maglev balancing algorithm with weights.
https://static.googleusercontent.com/media/research.google.com/ru//pubs/archive/44824.pdf

It is slightly modified to use power-of-two mapping table and LCG random generator.

Package has no assumption on hash functions to use:
- shards are given as tuple of 64bit hash-sum (of name or whatever) and weight,
- result is just mapping table, you have to lookup by yourself.

	const tableSize = 1<<20 // should be power of two and constant during lifetime of cluster
	shards := []maglev.Shard{
	    {Hash: myhash("shard1", seedShard), Weight: 0.5},
	    {Hash: myhash("shard2", seedShard), Weight: 1},
	    {Hash: myhash("shard3", seedShard), Weight: 2.1},
	}
	table, _ := maglev.BuildTable(shards, tableSize)

	item := myhash("picture1", seedItem)
	shard := table[item%tableSize]
*/
package maglev

import (
	"errors"
	"sort"
)

// Shard is input definition of shard
type Shard struct {
	// Hash is pseudorandom value based on shard's name or whatever
	Hash uint64
	// Weight defines relative weight of shard.
	// All weights are normalized to average, there fore there is no difference will you use 1.0, 0.9, 1.1 or 100, 90, 110.
	Weight float64
}

// Table is result mapping from slot to shard
type Table []uint16

type shardGen struct {
	start int
	pos   int
	add   int
	mult  int
	cnt   int
	ix    int
	w     float64
	sh    *Shard
}

const ga = 0xacd5ad43274593b9
const gb = 0x6956ab76ed268a3d

// BuildTable builds mapping table for set of shards.
// It panics if tableSize is not power of two.
// tableSize have to be constant during whole lifetime of your cluster
func BuildTable(shards []Shard, tableSize int) (Table, error) {
	if tableSize <= 0 || tableSize&(tableSize-1) != 0 {
		panic("tableSize should be positive power of two")
	}
	table := make(Table, tableSize)

	averageWeight := 0.0
	generators := make([]shardGen, 0, len(shards))
	highbits := (^uint64(0))/uint64(tableSize) + 1
	for i, sh := range shards {
		if sh.Weight < 0 {
			return nil, errors.New("weight should be not negative")
		}
		gen := shardGen{pos: 0, w: 0, ix: i + 1, sh: &shards[i]}
		sh.Hash = sh.Hash*ga + gb // note: sh is a copy
		gen.start = int(sh.Hash / highbits)
		sh.Hash = sh.Hash*ga + gb
		gen.add = int(sh.Hash/highbits) | 1
		sh.Hash = sh.Hash*ga + gb
		gen.mult = int(sh.Hash/highbits)&^3 | 5
		generators = append(generators, gen)
		averageWeight += sh.Weight
	}
	if averageWeight == 0 {
		return nil, errors.New("Some weights should be positive")
	}
	averageWeight /= float64(len(shards))

	// Due to algorithm's nature, there is certainly will be slots reassignment
	// if we don't establish stable shards ordering.
	// Hope you've passed different Hash-values for shards.
	sort.Slice(generators, func(i, j int) bool {
		return generators[i].sh.Hash < generators[j].sh.Hash
	})

	rest := tableSize
	// Collect slots counts for all shards.
	// I don't want to manually fix rounding errors,
	// therefore brute-force algorithm used.
	// Well, it is not quite even, but should be stable enough.
	for i := 0; rest > 0; i++ {
		sh := &generators[i%len(generators)]
		sh.w += sh.sh.Weight
		if sh.w >= averageWeight {
			add := int(sh.w / averageWeight)
			if add > rest {
				add = rest
			}
			sh.cnt += add
			rest -= add
			sh.w -= averageWeight * float64(add)
		}
	}

	// Core of Maglev algorithm: slot assignment
	mask := tableSize - 1
	for i := 0; rest < tableSize; i++ {
		sh := &generators[i%len(generators)]
		if sh.cnt == 0 {
			continue
		}
		sh.cnt--
		for {
			cur := (sh.start + sh.pos) & mask
			sh.pos = sh.pos*sh.mult + sh.add
			if table[cur] == 0 {
				table[cur] = uint16(sh.ix)
				rest++
				break
			}
		}
	}

	// now there is stored shardn+1. Change is back to shardn.
	for i := range table {
		table[i]--
	}

	return table, nil
}
