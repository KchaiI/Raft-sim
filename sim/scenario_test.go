package sim

import "testing"

// SPEC §4.3 狙い撃ちシナリオ: 可用性の境界。

// 5 ノード中 2 ノードの永久停止では可用性が継続する (過半数 3 が生存)。
func TestFiveNodesTolerateTwoPermanentFailures(t *testing.T) {
	for seed := int64(1); seed <= 20; seed++ {
		s := New(Options{
			Seed:    seed,
			Nodes:   5,
			Clients: 3,
			Horizon: 12 * Second,
			Net:     NetParams{DelayMin: 200 * Microsecond, DelayMean: 2 * Millisecond, DelayMax: 100 * Millisecond},
		})
		if err := s.RunUntil(2 * Second); err != nil {
			t.Fatalf("seed=%d: %v", seed, err)
		}
		// リーダーを含む 2 ノードを永久停止 (最も過酷な組)
		lead := s.LeaderID()
		if lead == 0 {
			t.Fatalf("seed=%d: リーダー不在", seed)
		}
		other := lead%5 + 1
		s.Crash(lead, false)
		s.Crash(other, false)
		crashT := s.Now()
		if err := s.RunUntil(12 * Second); err != nil {
			t.Fatalf("seed=%d: %v", seed, err)
		}
		if err := s.CheckLinearizable(); err != nil {
			t.Fatalf("seed=%d: %v", seed, err)
		}
		// 停止後に invoke された操作が完了していること (可用性の継続)
		completedAfter := 0
		for _, op := range s.History() {
			if op.Complete && op.Invoke > crashT {
				completedAfter++
			}
		}
		if completedAfter < 20 {
			t.Fatalf("seed=%d: 2 ノード停止後の完了操作 %d 件 (可用性が失われた)", seed, completedAfter)
		}
	}
}

// 5 ノード中 3 ノードの停止では安全に停止する: 新規操作は一切完了しないが
// 安全性違反も起こさない (split-brain しない)。
func TestFiveNodesHaltSafelyWithThreeFailures(t *testing.T) {
	for seed := int64(1); seed <= 20; seed++ {
		s := New(Options{
			Seed:    seed,
			Nodes:   5,
			Clients: 3,
			Horizon: 10 * Second,
			Net:     NetParams{DelayMin: 200 * Microsecond, DelayMean: 2 * Millisecond, DelayMax: 100 * Millisecond},
		})
		if err := s.RunUntil(2 * Second); err != nil {
			t.Fatalf("seed=%d: %v", seed, err)
		}
		lead := s.LeaderID()
		if lead == 0 {
			t.Fatalf("seed=%d: リーダー不在", seed)
		}
		// リーダーを含む 3 ノードを永久停止
		s.Crash(lead, false)
		down := 1
		for id := uint64(1); id <= 5 && down < 3; id++ {
			if id != lead {
				s.Crash(id, false)
				down++
			}
		}
		crashT := s.Now()
		if err := s.RunUntil(10 * Second); err != nil {
			t.Fatalf("seed=%d: %v", seed, err)
		}
		if err := s.CheckLinearizable(); err != nil {
			t.Fatalf("seed=%d: %v", seed, err)
		}
		// 停止後に invoke された操作は 1 つも完了しない (安全な停止)。
		// 停止前に commit 済みの操作の応答 (セッションキャッシュ) は許される。
		for _, op := range s.History() {
			if op.Complete && op.Invoke > crashT {
				t.Fatalf("seed=%d: 過半数喪失後に操作が完了した (split-brain の疑い): %v", seed, op)
			}
		}
		// リーダーも存在しない (CheckQuorum で退位済み)
		if got := s.LeaderID(); got != 0 {
			t.Fatalf("seed=%d: 過半数喪失後もリーダーが存在: %d", seed, got)
		}
	}
}
