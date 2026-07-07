package sim

import (
	"testing"
)

// M2 受け入れテスト: 障害注入下で Log Matching / State Machine Safety 維持。

// 障害なし: 提案がクラスタ全体に複製・commit され、全ノードが同じ prefix を apply する。
func TestReplicationNoFaults(t *testing.T) {
	for seed := int64(1); seed <= 10; seed++ {
		s := New(Options{
			Seed:            seed,
			Nodes:           3,
			Horizon:         3 * Second,
			ProposeInterval: 20 * Millisecond,
			Net:             NetParams{DelayMin: 200 * Microsecond, DelayMean: 2 * Millisecond, DelayMax: 100 * Millisecond},
		})
		if err := s.Run(); err != nil {
			t.Fatalf("seed=%d: %v", seed, err)
		}
		// 全ノードの commitIndex が十分進んでいること
		for id := uint64(1); id <= 3; id++ {
			n := s.Node(id)
			if n.CommitIndex() < 50 {
				t.Fatalf("seed=%d: node %d の commitIndex=%d が少なすぎる", seed, id, n.CommitIndex())
			}
		}
		// 静止状態では全ノードの commitIndex が一致すること
		c1 := s.Node(1).CommitIndex()
		for id := uint64(2); id <= 3; id++ {
			if got := s.Node(id).CommitIndex(); got != c1 {
				t.Fatalf("seed=%d: commitIndex 不一致 node1=%d node%d=%d", seed, c1, id, got)
			}
		}
	}
}

// 全障害 (喪失・重複・遅延・分断・クラッシュ) 注入下での安全性。
// Log Matching / State Machine Safety / Leader Completeness は毎イベント検査される。
func TestLogMatchingUnderFaults(t *testing.T) {
	for _, nodes := range []int{3, 5} {
		for seed := int64(1); seed <= 100; seed++ {
			o := faultyOpts(seed, nodes)
			o.ProposeInterval = 15 * Millisecond
			o.Faults.QuietTail = 1 * Second
			s := New(o)
			if err := s.Run(); err != nil {
				t.Fatalf("nodes=%d seed=%d: %v", nodes, seed, err)
			}
			// 進捗: 障害下でも何かは commit されていること
			var maxCommit uint64
			for id := uint64(1); id <= uint64(nodes); id++ {
				if n := s.Node(id); n != nil && n.CommitIndex() > maxCommit {
					maxCommit = n.CommitIndex()
				}
			}
			if maxCommit == 0 {
				t.Fatalf("nodes=%d seed=%d: 何も commit されていない", nodes, seed)
			}
		}
	}
}

// 決定論の回帰: 提案ワークロード + 全障害でもトレースがバイト一致。
func TestReplicationDeterminism(t *testing.T) {
	run := func() []byte {
		o := faultyOpts(42, 5)
		o.ProposeInterval = 15 * Millisecond
		o.Trace = true
		s := New(o)
		if err := s.Run(); err != nil {
			t.Fatal(err)
		}
		return s.Trace()
	}
	a, b := run(), run()
	if string(a) != string(b) {
		t.Fatal("トレース不一致")
	}
}
