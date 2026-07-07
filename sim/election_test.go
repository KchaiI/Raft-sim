package sim

import (
	"bytes"
	"fmt"
	"testing"

	"raftsim/raft"
)

// M1 受け入れテスト。

// 障害なしで単一リーダーに収束すること。
func TestSingleLeaderConvergesNoFaults(t *testing.T) {
	for _, nodes := range []int{1, 3, 5} {
		for seed := int64(1); seed <= 20; seed++ {
			s := New(Options{
				Seed:    seed,
				Nodes:   nodes,
				Horizon: 3 * Second,
				Net:     NetParams{DelayMin: 200 * Microsecond, DelayMean: 2 * Millisecond, DelayMax: 100 * Millisecond},
			})
			if err := s.Run(); err != nil {
				t.Fatalf("nodes=%d seed=%d: 不変条件違反: %v", nodes, seed, err)
			}
			leaders := s.Leaders()
			if len(leaders) != 1 {
				t.Fatalf("nodes=%d seed=%d: リーダー数 %d (期待 1): %v", nodes, seed, len(leaders), leaders)
			}
			// 全ノードがそのリーダーを認識していること
			for id := uint64(1); id <= uint64(nodes); id++ {
				if got := s.Node(id).LeaderID(); got != leaders[0] {
					t.Fatalf("nodes=%d seed=%d: node %d の leaderID=%d (期待 %d)", nodes, seed, id, got, leaders[0])
				}
			}
		}
	}
}

func faultyOpts(seed int64, nodes int) Options {
	return Options{
		Seed:    seed,
		Nodes:   nodes,
		Horizon: 5 * Second,
		Net: NetParams{
			DropProb:  0.10,
			DupProb:   0.05,
			DelayMin:  200 * Microsecond,
			DelayMean: 5 * Millisecond,
			DelayMax:  400 * Millisecond,
		},
		Faults: FaultOpts{
			PPartition: 0.15,
			PCrash:     0.10,
		},
	}
}

// 分断・クラッシュ・喪失・重複・遅延の下で Election Safety (および term/commit
// 単調性等の全不変条件) が維持されること。
func TestElectionSafetyUnderFaults(t *testing.T) {
	for _, nodes := range []int{3, 5} {
		for seed := int64(1); seed <= 100; seed++ {
			s := New(faultyOpts(seed, nodes))
			if err := s.Run(); err != nil {
				t.Fatalf("nodes=%d seed=%d: %v", nodes, seed, err)
			}
		}
	}
}

// 障害注入を含むフルシミュレーションでも同一シード 2 回のトレースが
// バイト一致すること (M0 の決定論保証の回帰テスト)。
func TestFullSimDeterminism(t *testing.T) {
	for seed := int64(1); seed <= 5; seed++ {
		run := func() []byte {
			o := faultyOpts(seed, 5)
			o.Trace = true
			s := New(o)
			if err := s.Run(); err != nil {
				t.Fatalf("seed=%d: %v", seed, err)
			}
			return s.Trace()
		}
		a, b := run(), run()
		if len(a) == 0 || !bytes.Equal(a, b) {
			t.Fatalf("seed=%d: トレース不一致 (len %d vs %d)", seed, len(a), len(b))
		}
	}
}

// クラッシュした過半数未満のノードが再起動を繰り返しても選出が回復すること。
func TestLeaderReelectionAfterCrash(t *testing.T) {
	for seed := int64(1); seed <= 20; seed++ {
		o := Options{
			Seed:    seed,
			Nodes:   5,
			Horizon: 8 * Second,
			Net:     NetParams{DelayMin: 200 * Microsecond, DelayMean: 2 * Millisecond, DelayMax: 100 * Millisecond},
			Faults:  FaultOpts{PCrash: 0.20, RestartMean: 300 * Millisecond, QuietTail: 2 * Second},
		}
		s := New(o)
		if err := s.Run(); err != nil {
			t.Fatalf("seed=%d: %v", seed, err)
		}
		// horizon 終了時点で生存ノードが quorum 以上なら、リーダーがいるはず
		alive := s.aliveIDs()
		if len(alive) >= 3 {
			if len(s.Leaders()) == 0 {
				t.Fatalf("seed=%d: 生存 %d ノードだがリーダー不在", seed, len(alive))
			}
		}
	}
}

// PreVote: 分断から復帰したノードが term を荒らさないこと (間接検証)。
// 分断中に孤立したノードの term が、PreVote により肥大しないこと。
func TestPreVotePreventsTermInflation(t *testing.T) {
	seed := int64(7)
	run := func(disablePreVote bool) uint64 {
		s := New(Options{
			Seed:           seed,
			Nodes:          5,
			Horizon:        10 * Second,
			DisablePreVote: disablePreVote,
			Net:            NetParams{DelayMin: 200 * Microsecond, DelayMean: 2 * Millisecond, DelayMax: 100 * Millisecond},
			Faults:         FaultOpts{PPartition: 0.5, PartitionMean: 1 * Second},
		})
		if err := s.Run(); err != nil {
			t.Fatalf("%v", err)
		}
		var maxTerm uint64
		for id := uint64(1); id <= 5; id++ {
			if n := s.Node(id); n != nil && n.Term() > maxTerm {
				maxTerm = n.Term()
			}
		}
		return maxTerm
	}
	withPreVote := run(false)
	if withPreVote > 30 {
		t.Fatalf("PreVote 有効でも term が %d まで肥大", withPreVote)
	}
}

func ExampleNew() {
	s := New(Options{Seed: 1, Nodes: 3, Horizon: 2 * Second,
		Net: NetParams{DelayMin: 200 * Microsecond, DelayMean: 2 * Millisecond, DelayMax: 100 * Millisecond}})
	err := s.Run()
	fmt.Println(err, len(s.Leaders()))
	// Output: <nil> 1
}

var _ = raft.StateLeader // 参照維持
