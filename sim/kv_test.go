package sim

import (
	"testing"
)

// M4 受け入れテスト: ランダム履歴で線形化違反ゼロ、重複リクエストが exactly-once。

func countComplete(s *Simulator) (complete, total int) {
	for _, op := range s.History() {
		total++
		if op.Complete {
			complete++
		}
	}
	return
}

// 障害なし: クライアント操作がすべて線形化可能。
func TestLinearizableNoFaults(t *testing.T) {
	for seed := int64(1); seed <= 10; seed++ {
		s := New(Options{
			Seed:    seed,
			Nodes:   3,
			Clients: 5,
			Horizon: 5 * Second,
			Net:     NetParams{DelayMin: 200 * Microsecond, DelayMean: 2 * Millisecond, DelayMax: 100 * Millisecond},
		})
		if err := s.Run(); err != nil {
			t.Fatalf("seed=%d: %v", seed, err)
		}
		if err := s.CheckLinearizable(); err != nil {
			t.Fatalf("seed=%d: %v", seed, err)
		}
		complete, total := countComplete(s)
		if complete < 100 {
			t.Fatalf("seed=%d: 完了操作が少なすぎる: %d/%d", seed, complete, total)
		}
	}
}

// 全障害注入下 (喪失/重複/遅延/分断/クラッシュ/fsync 境界クラッシュ):
// 応答を得た全操作が線形化可能で、再送による重複は exactly-once。
func TestLinearizableUnderFullFaults(t *testing.T) {
	for _, nodes := range []int{3, 5} {
		for seed := int64(1); seed <= 50; seed++ {
			o := faultyOpts(seed, nodes)
			o.Clients = 4
			o.Horizon = 6 * Second
			o.Faults.PCrash = 0.10
			o.Faults.PCrashPersist = 0.01
			s := New(o)
			if err := s.Run(); err != nil {
				t.Fatalf("nodes=%d seed=%d: %v", nodes, seed, err)
			}
			if err := s.CheckLinearizable(); err != nil {
				t.Fatalf("nodes=%d seed=%d: %v", nodes, seed, err)
			}
			complete, _ := countComplete(s)
			if complete == 0 {
				t.Fatalf("nodes=%d seed=%d: 1 つも操作が完了していない", nodes, seed)
			}
		}
	}
}

// 高頻度のメッセージ喪失でクライアント再送を強制し、exactly-once を検証する
// (二重適用は appApply 内の effectAt 検査が violation として検出する)。
func TestExactlyOnceUnderHeavyRetries(t *testing.T) {
	for seed := int64(1); seed <= 20; seed++ {
		s := New(Options{
			Seed:    seed,
			Nodes:   3,
			Clients: 4,
			Horizon: 6 * Second,
			Net: NetParams{
				DropProb:  0.30, // クライアント要求・応答も 30% 喪失 → 再送多発
				DupProb:   0.10,
				DelayMin:  200 * Microsecond,
				DelayMean: 5 * Millisecond,
				DelayMax:  300 * Millisecond,
			},
			ClientTimeout: 300 * Millisecond,
		})
		if err := s.Run(); err != nil {
			t.Fatalf("seed=%d: %v", seed, err)
		}
		if err := s.CheckLinearizable(); err != nil {
			t.Fatalf("seed=%d: %v", seed, err)
		}
		complete, _ := countComplete(s)
		if complete < 20 {
			t.Fatalf("seed=%d: 完了操作が少なすぎる: %d", seed, complete)
		}
	}
}

// SPEC §4.3: 分断されたリーダーがクライアントに応答できないこと (stale read 防止)。
// 旧リーダーは commit できないため Get にも応答できず、クライアントは新リーダー側で
// 完了する。全履歴の線形化可能性がそれを機械的に証明する。
func TestPartitionedLeaderCannotServeClients(t *testing.T) {
	for seed := int64(1); seed <= 20; seed++ {
		s := New(Options{
			Seed:    seed,
			Nodes:   5,
			Clients: 4,
			Horizon: 12 * Second,
			Net:     NetParams{DelayMin: 200 * Microsecond, DelayMean: 2 * Millisecond, DelayMax: 100 * Millisecond},
		})
		if err := s.RunUntil(3 * Second); err != nil {
			t.Fatalf("seed=%d: %v", seed, err)
		}
		leaders := s.Leaders()
		if len(leaders) != 1 {
			t.Fatalf("seed=%d: リーダー不在", seed)
		}
		old := leaders[0]
		// 旧リーダーを少数側に隔離 (クライアントは全ノードに到達できるまま)
		groups := map[uint64]int{}
		for id := uint64(1); id <= 5; id++ {
			if id != old {
				groups[id] = 1
			}
		}
		s.SetPartition(groups)
		if err := s.RunUntil(8 * Second); err != nil {
			t.Fatalf("seed=%d: %v", seed, err)
		}
		// 多数派側に新リーダーがいて、操作が進行していること
		var newLead uint64
		for _, l := range s.Leaders() {
			if l != old {
				newLead = l
			}
		}
		if newLead == 0 {
			t.Fatalf("seed=%d: 多数派側に新リーダーがいない", seed)
		}
		s.SetPartition(nil)
		if err := s.RunUntil(12 * Second); err != nil {
			t.Fatalf("seed=%d: %v", seed, err)
		}
		if err := s.CheckLinearizable(); err != nil {
			t.Fatalf("seed=%d: 分断シナリオで線形化違反 (stale read の疑い): %v", seed, err)
		}
	}
}

// 決定論の回帰: クライアント層込みでもトレースがバイト一致。
func TestKVWorkloadDeterminism(t *testing.T) {
	run := func() []byte {
		o := faultyOpts(11, 5)
		o.Clients = 4
		o.Trace = true
		o.Faults.PCrashPersist = 0.01
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
