package sim

import "testing"

// M5 受け入れテスト: ログ圧縮後の遅延フォロワー復帰、転送中クラッシュ耐性。

// 長時間ダウンしていたフォロワーが、圧縮済みリーダーから InstallSnapshot で
// 復帰し追いつくこと。
func TestLaggingFollowerCatchesUpViaSnapshot(t *testing.T) {
	for seed := int64(1); seed <= 20; seed++ {
		s := New(Options{
			Seed:              seed,
			Nodes:             3,
			Clients:           4,
			Horizon:           15 * Second,
			SnapshotThreshold: 50,
			Net:               NetParams{DelayMin: 200 * Microsecond, DelayMean: 2 * Millisecond, DelayMax: 100 * Millisecond},
		})
		if err := s.RunUntil(1 * Second); err != nil {
			t.Fatalf("seed=%d: %v", seed, err)
		}
		// リーダーでないノードを 1 つ落とす
		var victim uint64
		for id := uint64(1); id <= 3; id++ {
			if s.Node(id) != nil && len(s.Leaders()) > 0 && id != s.Leaders()[0] {
				victim = id
				break
			}
		}
		if victim == 0 {
			t.Fatalf("seed=%d: 対象ノードが選べない", seed)
		}
		s.Crash(victim, false)
		// 残り 2 ノードで大量に commit → リーダーが複数回圧縮する
		if err := s.RunUntil(8 * Second); err != nil {
			t.Fatalf("seed=%d: %v", seed, err)
		}
		var leadSnap uint64
		for _, l := range s.Leaders() {
			leadSnap = s.Node(l).SnapIndex()
		}
		if leadSnap == 0 {
			t.Fatalf("seed=%d: リーダーが圧縮していない (threshold 調整が必要)", seed)
		}
		s.Restart(victim)
		if err := s.RunUntil(12 * Second); err != nil {
			t.Fatalf("seed=%d: %v", seed, err)
		}
		if s.SnapshotsSent() == 0 {
			t.Fatalf("seed=%d: InstallSnapshot が送られていない", seed)
		}
		// 復帰ノードが追いついたこと
		if len(s.Leaders()) == 0 {
			t.Fatalf("seed=%d: リーダー不在", seed)
		}
		lead := s.Node(s.Leaders()[0])
		vic := s.Node(victim)
		if vic == nil || vic.CommitIndex() < leadSnap {
			t.Fatalf("seed=%d: 復帰ノードが追いつかない", seed)
		}
		_ = lead
		if err := s.CheckLinearizable(); err != nil {
			t.Fatalf("seed=%d: %v", seed, err)
		}
	}
}

// スナップショット転送中のクラッシュ (SPEC §4.3): 転送先・転送元の双方を
// 転送タイミング周辺で繰り返し落としても、安全性を保ったまま最終的に復帰する。
func TestSnapshotTransferCrashResilience(t *testing.T) {
	for seed := int64(1); seed <= 20; seed++ {
		s := New(Options{
			Seed:              seed,
			Nodes:             3,
			Clients:           3,
			Horizon:           20 * Second,
			SnapshotThreshold: 40,
			Net: NetParams{ // 喪失 20%: 転送自体もよく失敗する
				DropProb: 0.20, DupProb: 0.05,
				DelayMin: 200 * Microsecond, DelayMean: 3 * Millisecond, DelayMax: 200 * Millisecond,
			},
		})
		if err := s.RunUntil(1 * Second); err != nil {
			t.Fatalf("seed=%d: %v", seed, err)
		}
		if len(s.Leaders()) == 0 {
			continue // このシードでは前提が組めない (喪失率が高いため稀にある)
		}
		lead0 := s.Leaders()[0]
		var victim uint64
		for id := uint64(1); id <= 3; id++ {
			if id != lead0 {
				victim = id
				break
			}
		}
		s.Crash(victim, false)
		if err := s.RunUntil(6 * Second); err != nil {
			t.Fatalf("seed=%d: %v", seed, err)
		}
		// 復帰直後に何度か落とす (転送の途中でクラッシュする状況を作る)
		for k := 0; k < 3; k++ {
			s.Restart(victim)
			if err := s.RunUntil(s.Now() + 400*Millisecond); err != nil {
				t.Fatalf("seed=%d: %v", seed, err)
			}
			s.Crash(victim, false)
			if err := s.RunUntil(s.Now() + 200*Millisecond); err != nil {
				t.Fatalf("seed=%d: %v", seed, err)
			}
		}
		// リーダー側も一度落とす (転送元クラッシュ)
		if ls := s.Leaders(); len(ls) > 0 {
			s.Crash(ls[0], true)
		}
		s.Restart(victim)
		if err := s.RunUntil(20 * Second); err != nil {
			t.Fatalf("seed=%d: %v", seed, err)
		}
		if err := s.CheckLinearizable(); err != nil {
			t.Fatalf("seed=%d: %v", seed, err)
		}
		// 最終的に復帰していること
		vic := s.Node(victim)
		if vic == nil {
			t.Fatalf("seed=%d: victim が復帰していない", seed)
		}
		var maxCommit uint64
		for id := uint64(1); id <= 3; id++ {
			if n := s.Node(id); n != nil && n.CommitIndex() > maxCommit {
				maxCommit = n.CommitIndex()
			}
		}
		if vic.CommitIndex()+100 < maxCommit {
			t.Fatalf("seed=%d: victim が大きく遅れたまま: %d << %d", seed, vic.CommitIndex(), maxCommit)
		}
	}
}

// 全障害 + スナップショット有効の混合ソーク (M5 回帰)。
func TestSnapshotUnderFullFaults(t *testing.T) {
	for seed := int64(1); seed <= 50; seed++ {
		o := faultyOpts(seed, 5)
		o.Clients = 4
		o.Horizon = 6 * Second
		o.SnapshotThreshold = 30
		o.Faults.PCrash = 0.10
		o.Faults.PCrashPersist = 0.01
		s := New(o)
		if err := s.Run(); err != nil {
			t.Fatalf("seed=%d: %v", seed, err)
		}
		if err := s.CheckLinearizable(); err != nil {
			t.Fatalf("seed=%d: %v", seed, err)
		}
	}
}

// 決定論の回帰: スナップショット有効でもトレースがバイト一致。
func TestSnapshotDeterminism(t *testing.T) {
	run := func() []byte {
		o := faultyOpts(21, 5)
		o.Clients = 3
		o.SnapshotThreshold = 30
		o.Trace = true
		o.Faults.PCrash = 0.10
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
