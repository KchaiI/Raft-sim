package sim

import (
	"fmt"
	"math/rand"
	"sort"

	"raftsim/checker"
	"raftsim/kv"
	"raftsim/raft"
	"raftsim/storage"
)

// FaultOpts は障害注入のパラメータ (SPEC §3.3)。
type FaultOpts struct {
	CtrlInterval  int64   // 障害コントローラの周期 (default 100ms)
	PPartition    float64 // 周期ごとの分断発生確率 (アクティブな分断がない場合)
	PartitionMean int64   // 分断継続時間の平均 (指数分布)
	PCrash        float64 // 周期ごとのランダムクラッシュ確率
	RestartMean   int64   // 再起動までの平均時間 (指数分布)
	MaxDown       int     // 同時ダウン上限 (0 = quorum を保つ (n-1)/2)
	PCrashPersist float64 // Persist ごとの fsync 境界クラッシュ確率
	// QuietTail > 0 なら horizon 直前のその期間、新規の障害注入を止め
	// 分断も強制回復する (収束アサーションを持つテスト用)。
	QuietTail int64
}

// Options はシミュレーション 1 回分の設定。1 シード = 1 宇宙。
type Options struct {
	Seed           int64
	Nodes          int
	Horizon        int64 // 仮想時間の実行上限
	Trace          bool
	TickInterval   int64 // default 10ms
	ElectionTicks  int   // default 15 (150ms 基準, [ET,2ET) にランダム化)
	HeartbeatTicks int   // default 5 (50ms)
	DisablePreVote bool
	Net            NetParams
	Faults         FaultOpts
	MaxEvents      int // 暴走防止 (default 5,000,000)

	// ProposeInterval > 0 なら、その周期でリーダーに不透明ペイロードを提案する
	// (M2: ログ複製の駆動用。KV クライアントは M4 で別途実装)。
	ProposeInterval int64

	// KV クライアントワークロード (M4)。Clients > 0 で有効。
	Clients         int
	ClientThinkMean int64 // 操作間の平均思考時間 (default 20ms)
	ClientTimeout   int64 // 再送タイムアウト (default 800ms)
	Keys            int   // キー空間サイズ (default 5)

	// SnapshotThreshold > 0 なら、apply 済みエントリが前回スナップショットから
	// この数を超えたときにスナップショットを取りログを圧縮する (M5)。
	SnapshotThreshold uint64

	// SpareNodes は初期構成に含まれない予備サーバー数 (M6)。
	// ID は Nodes+1 .. Nodes+SpareNodes。AddServer で投票メンバーに昇格できる。
	SpareNodes int
	// MinVoters はチャーン注入が構成を縮小する下限 (default 3)。
	MinVoters int
}

func (o *Options) defaults() {
	if o.TickInterval == 0 {
		o.TickInterval = 10 * Millisecond
	}
	if o.ElectionTicks == 0 {
		o.ElectionTicks = 15
	}
	if o.HeartbeatTicks == 0 {
		o.HeartbeatTicks = 5
	}
	if o.Horizon == 0 {
		o.Horizon = 10 * Second
	}
	if o.MaxEvents == 0 {
		o.MaxEvents = 5_000_000
	}
	if o.Net.DelayMean == 0 {
		o.Net = NetParams{DelayMin: 200 * Microsecond, DelayMean: 2 * Millisecond, DelayMax: 500 * Millisecond}
	}
	if o.Faults.CtrlInterval == 0 {
		o.Faults.CtrlInterval = 100 * Millisecond
	}
	if o.Faults.PartitionMean == 0 {
		o.Faults.PartitionMean = 500 * Millisecond
	}
	if o.Faults.RestartMean == 0 {
		o.Faults.RestartMean = 500 * Millisecond
	}
	if o.Faults.MaxDown == 0 {
		o.Faults.MaxDown = (o.Nodes - 1) / 2
	}
	if o.ClientThinkMean == 0 {
		o.ClientThinkMean = 20 * Millisecond
	}
	if o.ClientTimeout == 0 {
		o.ClientTimeout = 800 * Millisecond
	}
	if o.Keys == 0 {
		o.Keys = 5
	}
}

// Simulator は決定論的シミュレータ本体。単一 goroutine で駆動する。
type Simulator struct {
	opt Options
	rng *rand.Rand
	q   *Queue
	net *Network
	tr  *Trace
	now int64

	servers []*Server // [1..Nodes] (index 0 は未使用)
	inv     *checker.Invariants

	partitionActive bool
	downCount       int
	events          int
	proposeSeq      int
	snapSent        int // 送信された InstallSnapshot の数 (テスト観測用)

	// KV クライアント層 (M4)
	clients []*Client
	history []checker.LinOp
	// exactly-once 検査: (clientID, seq) が状態を変化させたログ index。
	// 異なる index で 2 度 fresh apply されたら重複排除の失敗。
	effectAt map[effectKey]uint64

	violation error
}

type effectKey struct {
	client uint64
	seq    uint64
}

// New はシミュレータを構築する。
func New(opt Options) *Simulator {
	opt.defaults()
	s := &Simulator{
		opt:      opt,
		rng:      rand.New(rand.NewSource(opt.Seed)),
		q:        &Queue{},
		tr:       NewTrace(opt.Trace),
		inv:      checker.NewInvariants(),
		effectAt: map[effectKey]uint64{},
	}
	s.net = NewNetwork(opt.Net, s.rng, s.q, s.tr)
	s.servers = make([]*Server, opt.Nodes+1)
	for id := uint64(1); id <= uint64(opt.Nodes); id++ {
		sv := &Server{id: id, sim: s, store: storage.New(), app: kv.NewStore(), pending: map[uint64]uint64{}}
		sv.node = raft.NewNode(s.raftParams(id))
		sv.tickInterval = s.skewedTick()
		s.servers[id] = sv
		s.scheduleTick(sv)
	}
	s.scheduleFaultCtrl()
	if opt.ProposeInterval > 0 {
		s.scheduleProposer()
	}
	if opt.Clients > 0 {
		s.setupClients()
	}
	return s
}

// scheduleProposer は周期的にリーダーへ一意なコマンドを提案する (M2 ワークロード)。
// ペイロードは予約 ClientID=0 の KV Put (アプリ層でも正しくデコードできる形式)。
func (s *Simulator) scheduleProposer() {
	s.q.At(s.now+s.opt.ProposeInterval, func() {
		for _, id := range s.aliveIDs() {
			sv := s.servers[id]
			if sv.node.State() == raft.StateLeader {
				s.proposeSeq++
				cmd := kv.Command{ClientID: 0, Seq: uint64(s.proposeSeq), Op: kv.OpPut,
					Key: "raw", Value: fmt.Sprintf("cmd-%d", s.proposeSeq)}
				reply := sv.step(raft.Propose{Data: cmd.Encode()})
				if reply != nil && reply.OK {
					s.tr.Logf(s.now, "propose to %d: index=%d term=%d", id, reply.Index, reply.Term)
				}
				break
			}
		}
		s.scheduleProposer()
	})
}

func (s *Simulator) skewedTick() int64 {
	// クロックスキュー: ±20% (D-004)
	f := 0.8 + 0.4*s.rng.Float64()
	return int64(float64(s.opt.TickInterval) * f)
}

func (s *Simulator) scheduleTick(sv *Server) {
	s.q.At(s.now+sv.tickInterval, func() {
		if sv.alive() {
			sv.step(raft.Tick{})
		}
		s.scheduleTick(sv)
	})
}

// Run は horizon まで実行し、最初の不変条件違反を error として返す。
func (s *Simulator) Run() error { return s.RunUntil(s.opt.Horizon) }

// RunUntil は仮想時刻 until (horizon が上限) まで実行する。狙い撃ちシナリオが
// 「実行 → 介入 (Crash/Restart/分断) → 実行」と刻むための部分実行 API。
func (s *Simulator) RunUntil(until int64) error {
	if until > s.opt.Horizon {
		until = s.opt.Horizon
	}
	for {
		if s.violation != nil {
			return s.violation
		}
		t, ok := s.q.PeekTime()
		if !ok || t > until {
			return s.violation
		}
		_, fn, _ := s.q.Pop()
		s.events++
		if s.events > s.opt.MaxEvents {
			return fmt.Errorf("イベント数が上限 %d を超過 (暴走の疑い)", s.opt.MaxEvents)
		}
		s.now = t
		fn()
		s.checkInvariants()
	}
}

// Crash はノードを即座にクラッシュさせる (狙い撃ちシナリオ用)。
// autoRestart=true なら RestartMean 後の再起動を予約する。
func (s *Simulator) Crash(id uint64, autoRestart bool) { s.crash(id, autoRestart) }

// Restart はクラッシュ中のノードを耐久状態から再起動する。
func (s *Simulator) Restart(id uint64) { s.restartServer(id) }

// SetPartition はノード→グループ割当で分断を設定する (nil で回復)。
func (s *Simulator) SetPartition(groups map[uint64]int) {
	s.net.SetPartition(groups)
	s.partitionActive = groups != nil
}

// fail はチェッカー以外の検査 (exactly-once 等) の違反を記録する。
func (s *Simulator) fail(msg string) {
	if s.violation == nil {
		s.tr.Logf(s.now, "VIOLATION: %s", msg)
		s.violation = fmt.Errorf("t=%d: %s", s.now, msg)
	}
}

func (s *Simulator) checkInvariants() {
	nodes := make([]*raft.Node, 0, len(s.servers))
	for id := 1; id < len(s.servers); id++ { // 決定論: ID 順に検査
		if sv := s.servers[id]; sv.alive() {
			nodes = append(nodes, sv.node)
		}
	}
	s.inv.Check(nodes)
	if !s.inv.OK() && s.violation == nil {
		vio := s.inv.Violations()[0]
		s.tr.Logf(s.now, "VIOLATION: %s", vio)
		s.violation = fmt.Errorf("t=%d: %s", s.now, vio)
	}
}

// ---- メッセージ配送 ----

func (s *Simulator) sendRaftMsg(m raft.Message) {
	if m.Type == raft.MsgSnap {
		s.snapSent++
	}
	desc := ""
	if s.tr.Enabled() {
		desc = fmt.Sprintf("%s term=%d", m.Type, m.Term)
	}
	s.net.Send(s.now, m.From, m.To, false, desc, func() {
		sv := s.servers[m.To]
		if !sv.alive() {
			s.tr.Logf(s.now, "drop (down): %s %d->%d", m.Type, m.From, m.To)
			return
		}
		if s.tr.Enabled() {
			s.tr.Logf(s.now, "deliver %s %d->%d term=%d prev=%d/%d n=%d commit=%d granted=%v",
				m.Type, m.From, m.To, m.Term, m.PrevLogIndex, m.PrevLogTerm, len(m.Entries), m.Commit, m.Granted)
		}
		sv.step(raft.Receive{Msg: m})
	})
}

// ---- apply ----

func (s *Simulator) applyEntry(sv *Server, e raft.Entry) {
	s.inv.ObserveApply(sv.id, e)
	s.appApply(sv, e)
}

func (s *Simulator) applySnapshot(sv *Server, snap *raft.Snapshot) {
	s.inv.ObserveSnapshotRestore(sv.id, snap)
	s.appRestore(sv, snap)
	s.tr.Logf(s.now, "node %d: snapshot 復元 index=%d term=%d", sv.id, snap.Index, snap.Term)
}

// ---- クラッシュ・再起動 ----

// crash はノードを落とす。restart=true なら平均 RestartMean 後に再起動を予約する。
func (s *Simulator) crash(id uint64, restart bool) {
	sv := s.servers[id]
	if !sv.alive() {
		return
	}
	sv.node = nil
	s.downCount++
	s.onCrash(sv)
	s.tr.Logf(s.now, "fault: node %d crash", id)
	if restart {
		d := int64(s.rng.ExpFloat64() * float64(s.opt.Faults.RestartMean))
		s.q.At(s.now+d+1, func() { s.restartServer(id) })
	}
}

func (s *Simulator) restartServer(id uint64) {
	sv := s.servers[id]
	if sv.alive() {
		return
	}
	s.downCount--
	sv.tickInterval = s.skewedTick() // 再起動でスキューを引き直す
	sv.node = raft.RestartNode(s.raftParams(id), sv.store.Load())
	s.inv.ObserveRestart(id)
	s.onRestart(sv)
	sv.lastState, sv.lastTerm = sv.node.State(), sv.node.Term()
	s.tr.Logf(s.now, "fault: node %d restart (term=%d last=%d commit=%d)", id, sv.node.Term(), sv.node.LastIndex(), sv.node.CommitIndex())
}

// ---- 障害コントローラ ----

func (s *Simulator) scheduleFaultCtrl() {
	f := s.opt.Faults
	s.q.At(s.now+f.CtrlInterval, func() {
		s.faultCtrlTick()
		s.scheduleFaultCtrl()
	})
}

// quiet は静穏期間 (新規障害注入の停止) 中かを返す。
func (s *Simulator) quiet() bool {
	return s.opt.Faults.QuietTail > 0 && s.now > s.opt.Horizon-s.opt.Faults.QuietTail
}

func (s *Simulator) faultCtrlTick() {
	f := s.opt.Faults
	if s.quiet() {
		if s.partitionActive {
			s.net.SetPartition(nil)
			s.partitionActive = false
			s.tr.Logf(s.now, "fault: partition healed (quiet tail)")
		}
		return
	}
	// ネットワーク分断: ランダムな 2 分割 / 3 分割 → 指数分布時間後に回復
	if f.PPartition > 0 && !s.partitionActive && s.rng.Float64() < f.PPartition {
		k := 2 + s.rng.Intn(2)
		groups := map[uint64]int{}
		descr := make([]int, 0, s.opt.Nodes)
		for id := 1; id <= s.opt.Nodes; id++ {
			g := s.rng.Intn(k)
			groups[uint64(id)] = g
			descr = append(descr, g)
		}
		s.net.SetPartition(groups)
		s.partitionActive = true
		dur := int64(s.rng.ExpFloat64()*float64(f.PartitionMean)) + 50*Millisecond
		s.tr.Logf(s.now, "fault: partition k=%d groups=%v dur=%d", k, descr, dur)
		s.q.At(s.now+dur, func() {
			s.net.SetPartition(nil)
			s.partitionActive = false
			s.tr.Logf(s.now, "fault: partition healed")
		})
	}
	// ノードクラッシュ (揮発状態全喪失) → 再起動
	if f.PCrash > 0 && s.downCount < f.MaxDown && s.rng.Float64() < f.PCrash {
		alive := s.aliveIDs()
		if len(alive) > 0 {
			id := alive[s.rng.Intn(len(alive))]
			s.crash(id, true)
		}
	}
}

func (s *Simulator) aliveIDs() []uint64 {
	ids := make([]uint64, 0, s.opt.Nodes)
	for id := 1; id < len(s.servers); id++ {
		if s.servers[id].alive() {
			ids = append(ids, uint64(id))
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

// ---- 検査・テスト用アクセサ ----

// Trace はトレースのバイト列。
func (s *Simulator) Trace() []byte { return s.tr.Bytes() }

// Invariants はチェッカーへの参照。
func (s *Simulator) Invariants() *checker.Invariants { return s.inv }

// Now は現在の仮想時刻。
func (s *Simulator) Now() int64 { return s.now }

// Events は処理済みイベント数。
func (s *Simulator) Events() int { return s.events }

// Leaders は生存ノード中のリーダー ID 一覧。
func (s *Simulator) Leaders() []uint64 {
	var ls []uint64
	for id := 1; id < len(s.servers); id++ {
		sv := s.servers[id]
		if sv.alive() && sv.node.State() == raft.StateLeader {
			ls = append(ls, uint64(id))
		}
	}
	return ls
}

// Node は検査用にノードを返す (クラッシュ中は nil)。
func (s *Simulator) Node(id uint64) *raft.Node { return s.servers[id].node }

// SnapshotsSent は送信された InstallSnapshot メッセージ数。
func (s *Simulator) SnapshotsSent() int { return s.snapSent }

// History はクライアント操作履歴 (線形化可能性検査の入力)。
func (s *Simulator) History() []checker.LinOp { return s.history }

// CheckLinearizable は記録された履歴を検査する (Run 後に呼ぶ)。
func (s *Simulator) CheckLinearizable() error {
	return checker.CheckLinearizable(s.history)
}

// M4 以降のフック (KV アプリ) は sim/app.go に実装する。
func (s *Simulator) onCrash(sv *Server)   { s.appCrash(sv) }
func (s *Simulator) onRestart(sv *Server) { s.appRestart(sv) }
