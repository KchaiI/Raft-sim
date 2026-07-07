package sim

import (
	"testing"

	"raftsim/raft"
)

// M3 受け入れテスト: 永続化 + クラッシュリカバリ。
// fsync 境界クラッシュを含む再起動テストで Leader Completeness (含む全不変条件) 維持。

// fsync 境界クラッシュ + 通常クラッシュ + 全ネットワーク障害の混合。
func TestCrashRecoveryUnderFaults(t *testing.T) {
	for _, nodes := range []int{3, 5} {
		for seed := int64(1); seed <= 100; seed++ {
			o := faultyOpts(seed, nodes)
			o.ProposeInterval = 15 * Millisecond
			o.Horizon = 6 * Second
			o.Faults.PCrash = 0.15
			o.Faults.PCrashPersist = 0.02 // Persist の 2% が fsync 前に喪失
			o.Faults.QuietTail = 1 * Second
			s := New(o)
			if err := s.Run(); err != nil {
				t.Fatalf("nodes=%d seed=%d: %v", nodes, seed, err)
			}
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

// 極端ケース: すべての Persist が fsync 前にクラッシュしても、
// 外部作用 (メッセージ) が漏れず安全性違反ゼロ。リーダーは決して生まれない
// (投票の永続化が完了しないため)。
func TestAllPersistsCrash(t *testing.T) {
	for seed := int64(1); seed <= 10; seed++ {
		s := New(Options{
			Seed:    seed,
			Nodes:   3,
			Horizon: 3 * Second,
			Net:     NetParams{DelayMin: 200 * Microsecond, DelayMean: 2 * Millisecond, DelayMax: 100 * Millisecond},
			Faults:  FaultOpts{PCrashPersist: 1.0, RestartMean: 100 * Millisecond, MaxDown: 3},
		})
		if err := s.Run(); err != nil {
			t.Fatalf("seed=%d: %v", seed, err)
		}
		if ls := s.Leaders(); len(ls) != 0 {
			t.Fatalf("seed=%d: votedFor を永続化できないのにリーダーが誕生: %v", seed, ls)
		}
	}
}

// 決定的シナリオ: リーダーをクラッシュ → 新リーダーの下で commit が進む →
// 旧リーダーが耐久状態から復帰して追いつく。
func TestLeaderCrashRecoveryScenario(t *testing.T) {
	for seed := int64(1); seed <= 20; seed++ {
		s := New(Options{
			Seed:            seed,
			Nodes:           5,
			Horizon:         20 * Second,
			ProposeInterval: 20 * Millisecond,
			Net:             NetParams{DelayMin: 200 * Microsecond, DelayMean: 2 * Millisecond, DelayMax: 100 * Millisecond},
		})
		if err := s.RunUntil(3 * Second); err != nil {
			t.Fatalf("seed=%d: %v", seed, err)
		}
		leaders := s.Leaders()
		if len(leaders) != 1 {
			t.Fatalf("seed=%d: リーダー不在", seed)
		}
		old := leaders[0]
		oldTerm := s.Node(old).Term()
		commitBefore := s.Node(old).CommitIndex()
		if commitBefore < 50 {
			t.Fatalf("seed=%d: 前提の commit が進んでいない: %d", seed, commitBefore)
		}
		s.Crash(old, false)

		if err := s.RunUntil(8 * Second); err != nil {
			t.Fatalf("seed=%d: %v", seed, err)
		}
		leaders = s.Leaders()
		if len(leaders) != 1 {
			t.Fatalf("seed=%d: 新リーダーが選出されない", seed)
		}
		newLead := s.Node(leaders[0])
		if newLead.Term() <= oldTerm {
			t.Fatalf("seed=%d: 新リーダーの term %d が古い", seed, newLead.Term())
		}
		// 新リーダーは旧リーダーの commit 済みエントリをすべて持つ
		// (Leader Completeness はチェッカーが毎イベント検査済み)
		if newLead.CommitIndex() <= commitBefore {
			t.Fatalf("seed=%d: 新リーダーの下で commit が進まない: %d <= %d", seed, newLead.CommitIndex(), commitBefore)
		}

		commitMid := newLead.CommitIndex()
		s.Restart(old)
		restarted := s.Node(old)
		// 再起動直後: 揮発状態は失われ、fsync 済みの term / ログのみ生存
		if restarted.Term() < oldTerm {
			t.Fatalf("seed=%d: 永続化された term が巻き戻った: %d < %d", seed, restarted.Term(), oldTerm)
		}
		if restarted.State() != raft.StateFollower {
			t.Fatalf("seed=%d: 再起動ノードが Follower でない", seed)
		}
		if err := s.RunUntil(12 * Second); err != nil {
			t.Fatalf("seed=%d: %v", seed, err)
		}
		// 復帰ノードが追いつく
		if got := s.Node(old).CommitIndex(); got < commitMid {
			t.Fatalf("seed=%d: 復帰ノードが追いつかない: commit=%d < %d", seed, got, commitMid)
		}
	}
}

// 過半数を同時に失って回復するシナリオ: 全ノード停止 → 全ノード再起動 →
// commit 済みデータが失われずリーダーが再選出される。
func TestFullClusterRestart(t *testing.T) {
	for seed := int64(1); seed <= 10; seed++ {
		s := New(Options{
			Seed:            seed,
			Nodes:           3,
			Horizon:         20 * Second,
			ProposeInterval: 20 * Millisecond,
			Net:             NetParams{DelayMin: 200 * Microsecond, DelayMean: 2 * Millisecond, DelayMax: 100 * Millisecond},
		})
		if err := s.RunUntil(3 * Second); err != nil {
			t.Fatalf("seed=%d: %v", seed, err)
		}
		var commitBefore uint64
		for id := uint64(1); id <= 3; id++ {
			if c := s.Node(id).CommitIndex(); c > commitBefore {
				commitBefore = c
			}
		}
		for id := uint64(1); id <= 3; id++ {
			s.Crash(id, false)
		}
		if err := s.RunUntil(4 * Second); err != nil {
			t.Fatalf("seed=%d: %v", seed, err)
		}
		for id := uint64(1); id <= 3; id++ {
			s.Restart(id)
		}
		if err := s.RunUntil(8 * Second); err != nil {
			t.Fatalf("seed=%d: %v", seed, err)
		}
		if len(s.Leaders()) != 1 {
			t.Fatalf("seed=%d: 全再起動後にリーダー不在", seed)
		}
		// commit 済みエントリは失われない (チェッカーの committed 記録とも整合)
		lead := s.Node(s.Leaders()[0])
		if lead.CommitIndex() < commitBefore {
			t.Fatalf("seed=%d: commit 済み %d が再起動後 %d に縮んだ", seed, commitBefore, lead.CommitIndex())
		}
	}
}
