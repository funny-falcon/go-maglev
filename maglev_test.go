package maglev_test

import (
	"sort"
	"testing"

	maglev "github.com/funny-falcon/go-maglev"
)

func TestEvenDistribution(t *testing.T) {
	shards := []maglev.Shard{
		{Hash: h("shard1"), Weight: 1},
		{Hash: h("shard2"), Weight: 1},
		{Hash: h("shard3"), Weight: 1},
		{Hash: h("shard4"), Weight: 1},
	}
	tbl, _ := maglev.BuildTable(shards, 1024)
	cnts := countDist(tbl, len(shards))
	if cnts[0] != 256 || cnts[1] != 256 || cnts[2] != 256 || cnts[3] != 256 {
		t.Error("Distribution is not even: ", cnts)
	}
}

func TestEvenDistribution100(t *testing.T) {
	shards := []maglev.Shard{
		{Hash: h("shard1"), Weight: 100},
		{Hash: h("shard2"), Weight: 100},
		{Hash: h("shard3"), Weight: 100},
		{Hash: h("shard4"), Weight: 100},
	}
	tbl, _ := maglev.BuildTable(shards, 1024)
	cnts := countDist(tbl, len(shards))
	if cnts[0] != 256 || cnts[1] != 256 || cnts[2] != 256 || cnts[3] != 256 {
		t.Error("Distribution is not even: ", cnts)
	}
}

func TestOddDistribution(t *testing.T) {
	shards := []maglev.Shard{
		{Hash: h("shard1"), Weight: 1},
		{Hash: h("shard2"), Weight: 1},
		{Hash: h("shard3"), Weight: 1},
	}
	tbl, _ := maglev.BuildTable(shards, 1024)
	cnts := countDist(tbl, len(shards))
	sort.Ints(cnts)
	if cnts[0] != 341 || cnts[2] != 342 {
		t.Error("Distribution is not balanced: ", cnts)
	}
}

func TestOddDistribution100(t *testing.T) {
	shards := []maglev.Shard{
		{Hash: h("shard1"), Weight: 100},
		{Hash: h("shard2"), Weight: 100},
		{Hash: h("shard3"), Weight: 100},
	}
	tbl, _ := maglev.BuildTable(shards, 1024)
	cnts := countDist(tbl, len(shards))
	sort.Ints(cnts)
	if cnts[0] != 341 || cnts[2] != 342 {
		t.Error("Distribution is not balanced: ", cnts)
	}
}

func TestWeightDistribution_1(t *testing.T) {
	shards := []maglev.Shard{
		{Hash: h("shard1"), Weight: 1},
		{Hash: h("shard2"), Weight: 1},
		{Hash: h("shard3"), Weight: 2},
	}
	tbl, _ := maglev.BuildTable(shards, 1024)
	cnts := countDist(tbl, len(shards))
	sort.Ints(cnts)
	// it is not perfectly even due to our slot-count balancing algorithm.
	// still it is quite close to perfect
	if !inRange(cnts[0], 255, 256) || !inRange(cnts[1], 255, 257) || !inRange(cnts[2], 512, 513) {
		t.Error("Distribution is not balanced: ", cnts)
	}
}

func TestWeightDistribution_1_100(t *testing.T) {
	shards := []maglev.Shard{
		{Hash: h("shard1"), Weight: 100},
		{Hash: h("shard2"), Weight: 100},
		{Hash: h("shard3"), Weight: 200},
	}
	tbl, _ := maglev.BuildTable(shards, 1024)
	cnts := countDist(tbl, len(shards))
	sort.Ints(cnts)
	// it is not perfectly even due to our slot-count balancing algorithm.
	// still it is quite close to perfect
	if !inRange(cnts[0], 255, 256) || !inRange(cnts[1], 255, 257) || !inRange(cnts[2], 512, 513) {
		t.Error("Distribution is not balanced: ", cnts)
	}
}

func TestWeightDistribution_2(t *testing.T) {
	shards := []maglev.Shard{
		{Hash: h("shard1"), Weight: 0.5},
		{Hash: h("shard2"), Weight: 1},
		{Hash: h("shard3"), Weight: 2.1},
	}
	tbl, _ := maglev.BuildTable(shards, 1024)
	cnts := countDist(tbl, len(shards))
	sort.Ints(cnts)
	// it is not perfectly even due to our slot-count balancing algorithm.
	// still it is quite close to perfect
	if !inRange(cnts[0], 141, 142) || !inRange(cnts[1], 283, 285) || !inRange(cnts[2], 597, 599) {
		t.Error("Distribution is not balanced: ", cnts)
	}
}

func TestWeightDistribution_2_100(t *testing.T) {
	shards := []maglev.Shard{
		{Hash: h("shard1"), Weight: 50},
		{Hash: h("shard2"), Weight: 100},
		{Hash: h("shard3"), Weight: 210},
	}
	tbl, _ := maglev.BuildTable(shards, 1024)
	cnts := countDist(tbl, len(shards))
	sort.Ints(cnts)
	// it is not perfectly even due to our slot-count balancing algorithm.
	// still it is quite close to perfect
	if !inRange(cnts[0], 141, 142) || !inRange(cnts[1], 283, 285) || !inRange(cnts[2], 597, 599) {
		t.Error("Distribution is not balanced: ", cnts)
	}
}

func TestMovements(t *testing.T) {
	shards := []maglev.Shard{
		{Hash: h("shard1"), Weight: 100},
		{Hash: h("shard2"), Weight: 100},
		{Hash: h("shard3"), Weight: 100},
	}
	tbl1, _ := maglev.BuildTable(shards, 1024)

	shards = []maglev.Shard{
		{Hash: h("shard1"), Weight: 100},
		{Hash: h("shard2"), Weight: 100},
		{Hash: h("shard3"), Weight: 110},
	}
	tbl2, _ := maglev.BuildTable(shards, 1024)

	shards = []maglev.Shard{
		{Hash: h("shard1"), Weight: 100},
		{Hash: h("shard2"), Weight: 110},
		{Hash: h("shard3"), Weight: 100},
	}
	tbl3, _ := maglev.BuildTable(shards, 1024)

	shards = []maglev.Shard{
		{Hash: h("shard1"), Weight: 100},
		{Hash: h("shard2"), Weight: 110},
		{Hash: h("shard3"), Weight: 110},
	}
	tbl4, _ := maglev.BuildTable(shards, 1024)

	diff12 := countDiff(tbl1, tbl2)
	diff13 := countDiff(tbl1, tbl3)
	diff24 := countDiff(tbl2, tbl4)
	diff34 := countDiff(tbl3, tbl4)

	if !inRange(diff12, 22, 30) || !inRange(diff13, 22, 30) || !inRange(diff24, 22, 30) || !inRange(diff34, 22, 30) {
		t.Error("Too many movements: ", diff12, diff13, diff24, diff34)
	}
}

func countDist(table maglev.Table, shards int) []int {
	cnt := make([]int, shards)
	for _, shard := range table {
		cnt[shard]++
	}
	return cnt
}

func countDiff(tbl1, tbl2 maglev.Table) int {
	cnt := 0
	for i, sh1 := range tbl1 {
		if sh1 != tbl2[i] {
			cnt++
		}
	}
	return cnt
}

func h(s string) uint64 {
	h := uint64(0x53f8e9)
	for _, c := range []byte(s) {
		h = (h ^ uint64(c)) * 0x315785
	}
	return h
}

func inRange(i, k, l int) bool {
	return i >= k && i <= l
}
