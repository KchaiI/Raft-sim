package sim

import (
	"testing"

	"raftsim/raft"
)

// M6 受け入れテスト: single-server メンバーシップ変更 + リーダーシップ移譲。

// 新サーバーの追加: catch-up フェーズ (D-010) を経て投票メンバーに昇格し、
// 全ノードの構成が一致すること。
func TestAddServerCatchUpAndJoin(t *testing.T) {
	for seed := int64(1); seed <= 20; seed++ {
		s := New(Options{
			Seed:              seed,
			Nodes:             3,
			SpareNodes:        1, // id 4
			Clients:           3,
			Horizon:           15 * Second,
			SnapshotThreshold: 50,
			Net:               NetParams{DelayMin: 200 * Microsecond, DelayMean: 2 * Millisecond, DelayMax: 100 * Millisecond},
		})
		// まずログを育てる (スナップショット経由の catch-up も踏ませる)
		if err := s.RunUntil(4 * Second); err != nil {
			t.Fatalf("seed=%d: %v", seed, err)
		}
		if !s.ProposeAddServer(4) {
			t.Fatalf("seed=%d: AddServer が受理されない", seed)
		}
		if err := s.RunUntil(10 * Second); err != nil {
			t.Fatalf("seed=%d: %v", seed, err)
		}
		// 全生存ノードの構成に 4 が含まれること
		for id := uint64(1); id <= 4; id++ {
			n := s.Node(id)
			if n == nil {
				continue
			}
			voters := n.ConfigVoters()
			if len(voters) != 4 {
				t.Fatalf("seed=%d: node %d の構成が %v (期待 4 ノード)", seed, id, voters)
			}
		}
		// 新メンバーが commit に追随していること
		lead := s.leaderServer()
		if lead == nil {
			t.Fatalf("seed=%d: リーダー不在", seed)
		}
		if s.Node(4).CommitIndex()+50 < lead.node.CommitIndex() {
			t.Fatalf("seed=%d: 新メンバーが追いついていない", seed)
		}
		if err := s.CheckLinearizable(); err != nil {
			t.Fatalf("seed=%d: %v", seed, err)
		}
	}
}

// リーダー自身の除去 (博士論文 §4.2.2): 構成 commit 後に退位し、
// 残りのクラスタが新リーダーを選出して継続すること。
func TestRemoveLeaderItself(t *testing.T) {
	for seed := int64(1); seed <= 20; seed++ {
		s := New(Options{
			Seed:    seed,
			Nodes:   5,
			Clients: 3,
			Horizon: 15 * Second,
			Net:     NetParams{DelayMin: 200 * Microsecond, DelayMean: 2 * Millisecond, DelayMax: 100 * Millisecond},
		})
		if err := s.RunUntil(3 * Second); err != nil {
			t.Fatalf("seed=%d: %v", seed, err)
		}
		old := s.LeaderID()
		if old == 0 {
			t.Fatalf("seed=%d: リーダー不在", seed)
		}
		if !s.ProposeRemoveServer(old) {
			t.Fatalf("seed=%d: RemoveServer(リーダー自身) が受理されない", seed)
		}
		if err := s.RunUntil(10 * Second); err != nil {
			t.Fatalf("seed=%d: %v", seed, err)
		}
		newLead := s.LeaderID()
		if newLead == 0 || newLead == old {
			t.Fatalf("seed=%d: 新リーダーが選出されない (lead=%d old=%d)", seed, newLead, old)
		}
		// 新リーダーの構成から旧リーダーが除かれている
		voters := s.Node(newLead).ConfigVoters()
		for _, v := range voters {
			if v == old {
				t.Fatalf("seed=%d: 除去したはずの %d が構成に残っている: %v", seed, old, voters)
			}
		}
		if len(voters) != 4 {
			t.Fatalf("seed=%d: 構成サイズ %d (期待 4)", seed, len(voters))
		}
		// 除去されたノードがクラスタを妨害しないこと (term の肥大なし)
		if s.Node(old) != nil && s.Node(old).Term() > s.Node(newLead).Term()+2 {
			t.Fatalf("seed=%d: 除去ノードが term を荒らしている: %d vs %d", seed, s.Node(old).Term(), s.Node(newLead).Term())
		}
		if err := s.CheckLinearizable(); err != nil {
			t.Fatalf("seed=%d: %v", seed, err)
		}
	}
}

// リーダーシップ移譲 (TimeoutNow): 対象が速やかにリーダーになること。
func TestLeadershipTransfer(t *testing.T) {
	for seed := int64(1); seed <= 20; seed++ {
		s := New(Options{
			Seed:    seed,
			Nodes:   5,
			Clients: 3,
			Horizon: 10 * Second,
			Net:     NetParams{DelayMin: 200 * Microsecond, DelayMean: 2 * Millisecond, DelayMax: 100 * Millisecond},
		})
		if err := s.RunUntil(3 * Second); err != nil {
			t.Fatalf("seed=%d: %v", seed, err)
		}
		old := s.LeaderID()
		if old == 0 {
			t.Fatalf("seed=%d: リーダー不在", seed)
		}
		target := old%5 + 1
		if !s.ProposeTransfer(target) {
			t.Fatalf("seed=%d: 移譲が受理されない", seed)
		}
		if err := s.RunUntil(5 * Second); err != nil {
			t.Fatalf("seed=%d: %v", seed, err)
		}
		if got := s.LeaderID(); got != target {
			t.Fatalf("seed=%d: 移譲後のリーダー=%d (期待 %d)", seed, got, target)
		}
		if err := s.CheckLinearizable(); err != nil {
			t.Fatalf("seed=%d: %v", seed, err)
		}
	}
}

// メンバーシップ変更中のリーダークラッシュ (SPEC §4.3): 変更 (catch-up 含む) の
// 直後にリーダーを落としても、安全性を保って回復すること。
func TestLeaderCrashDuringConfChange(t *testing.T) {
	for seed := int64(1); seed <= 30; seed++ {
		s := New(Options{
			Seed:       seed,
			Nodes:      3,
			SpareNodes: 1,
			Clients:    3,
			Horizon:    20 * Second,
			Net:        NetParams{DelayMin: 200 * Microsecond, DelayMean: 2 * Millisecond, DelayMax: 100 * Millisecond},
		})
		if err := s.RunUntil(2 * Second); err != nil {
			t.Fatalf("seed=%d: %v", seed, err)
		}
		lead := s.LeaderID()
		if lead == 0 || !s.ProposeAddServer(4) {
			continue // 前提が組めないシードはスキップ (安全性はチェッカーが常時検査)
		}
		// catch-up 中〜構成エントリ複製中のどこかでリーダーをクラッシュさせる
		dt := int64(seed%5) * 30 * Millisecond
		if err := s.RunUntil(s.Now() + 50*Millisecond + dt); err != nil {
			t.Fatalf("seed=%d: %v", seed, err)
		}
		s.Crash(lead, true)
		if err := s.RunUntil(15 * Second); err != nil {
			t.Fatalf("seed=%d: %v", seed, err)
		}
		// 生存ノードの過半数が合意できる状態に回復していること
		if s.LeaderID() == 0 {
			t.Fatalf("seed=%d: 回復しない", seed)
		}
		// 構成は「4 ノードに拡大」か「3 ノードのまま」のどちらかで全ノード一致に収束
		if err := s.RunUntil(18 * Second); err != nil {
			t.Fatalf("seed=%d: %v", seed, err)
		}
		leadN := s.Node(s.LeaderID())
		want := leadN.ConfigVoters()
		if len(want) != 3 && len(want) != 4 {
			t.Fatalf("seed=%d: 構成サイズ %v", seed, want)
		}
		if err := s.CheckLinearizable(); err != nil {
			t.Fatalf("seed=%d: %v", seed, err)
		}
	}
}

// ランダム構成変更チャーン + 移譲 + 全障害の混合 (変更中の全障害シナリオ)。
// 「新旧構成の同時多数決」の不正 (disjoint quorum) はチェッカーの
// Election Safety / single-server change 検査が毎イベント検出する。
func TestMembershipChurnUnderFullFaults(t *testing.T) {
	for seed := int64(1); seed <= 60; seed++ {
		o := faultyOpts(seed, 5)
		o.SpareNodes = 2
		o.Clients = 3
		o.Horizon = 8 * Second
		o.SnapshotThreshold = 40
		o.Faults.PCrash = 0.08
		o.Faults.PCrashPersist = 0.01
		o.Faults.PConfChange = 0.20
		o.Faults.PTransfer = 0.10
		s := New(o)
		if err := s.Run(); err != nil {
			t.Fatalf("seed=%d: %v", seed, err)
		}
		if err := s.CheckLinearizable(); err != nil {
			t.Fatalf("seed=%d: %v", seed, err)
		}
	}
}

// 決定論の回帰: チャーン込みでもトレースがバイト一致。
func TestMembershipDeterminism(t *testing.T) {
	run := func() []byte {
		o := faultyOpts(33, 5)
		o.SpareNodes = 2
		o.Clients = 3
		o.SnapshotThreshold = 40
		o.Trace = true
		o.Faults.PConfChange = 0.25
		o.Faults.PTransfer = 0.10
		o.Faults.PCrash = 0.08
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

var _ = raft.ConfAddServer
