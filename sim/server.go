package sim

import (
	"math/rand"

	"raftsim/kv"
	"raftsim/raft"
	"raftsim/storage"
)

// Server は 1 ノード分の実行環境: Raft ノード + 耐久ストレージ + アプリ。
// node == nil はクラッシュ中を表す。
type Server struct {
	id    uint64
	sim   *Simulator
	node  *raft.Node
	store *storage.Durable

	tickInterval int64 // クロックスキュー適用済み (D-004)

	// KV アプリ (揮発。再起動時はスナップショット + ログ再生で復元)
	app *kv.Store
	// apply 時に応答すべきクライアント要求: clientID → seq (揮発)
	pending map[uint64]uint64

	// トレース用の直前状態
	lastState raft.StateType
	lastTerm  uint64
}

func (sv *Server) alive() bool { return sv.node != nil }

// step はノードへ入力を渡し、出力を処理する。
func (sv *Server) step(in raft.Input) *raft.ProposeReply {
	if sv.node == nil {
		return nil
	}
	out := sv.node.Step(in)
	sv.handleOutput(out)
	return out.Reply
}

// handleOutput は Step の出力を処理する。順序が重要:
// Persist を fsync してからメッセージを送る。fsync 境界クラッシュが注入されると
// この Step の外部作用 (メッセージ・apply) はすべて消滅する (D-003)。
func (sv *Server) handleOutput(out raft.Output) {
	s := sv.sim
	if out.Persist != nil {
		if s.opt.Faults.PCrashPersist > 0 && !s.quiet() && s.rng.Float64() < s.opt.Faults.PCrashPersist {
			s.tr.Logf(s.now, "fault: node %d fsync 境界クラッシュ (persist 喪失)", sv.id)
			s.crash(sv.id, true)
			return
		}
		sv.store.Apply(out.Persist)
	}
	for _, m := range out.Messages {
		s.sendRaftMsg(m)
	}
	if out.ApplySnapshot != nil {
		s.applySnapshot(sv, out.ApplySnapshot)
	}
	for _, e := range out.Applied {
		s.applyEntry(sv, e)
	}
	sv.traceStateChange()
	if len(out.Applied) > 0 {
		sv.maybeSnapshot()
	}
}

// maybeSnapshot は apply がしきい値を超えたらスナップショットを取りログを圧縮する。
// CreateSnapshot の Step は新たな apply を生まないため再帰は 1 段で止まる。
func (sv *Server) maybeSnapshot() {
	s := sv.sim
	th := s.opt.SnapshotThreshold
	if th == 0 || sv.node == nil {
		return
	}
	if sv.node.LastApplied()-sv.node.SnapIndex() < th {
		return
	}
	idx := sv.node.LastApplied()
	data := sv.app.Snapshot()
	s.tr.Logf(s.now, "node %d: snapshot 作成 index=%d", sv.id, idx)
	out := sv.node.Step(raft.CreateSnapshot{Index: idx, Data: data})
	sv.handleOutput(out)
}

func (sv *Server) traceStateChange() {
	if sv.node == nil {
		return
	}
	st, tm := sv.node.State(), sv.node.Term()
	if st != sv.lastState || tm != sv.lastTerm {
		sv.sim.tr.Logf(sv.sim.now, "node %d: %s term=%d (was %s term=%d)", sv.id, st, tm, sv.lastState, sv.lastTerm)
		sv.lastState, sv.lastTerm = st, tm
	}
}

// raftParams はノード構築パラメータ。RNG は master RNG から系列を派生させる。
func (s *Simulator) raftParams(id uint64) raft.Params {
	return raft.Params{
		ID:             id,
		Voters:         s.initialVoters(),
		ElectionTicks:  s.opt.ElectionTicks,
		HeartbeatTicks: s.opt.HeartbeatTicks,
		PreVote:        !s.opt.DisablePreVote,
		RNG:            rand.New(rand.NewSource(s.rng.Int63())),
	}
}

func (s *Simulator) initialVoters() []uint64 {
	v := make([]uint64, s.opt.Nodes)
	for i := range v {
		v[i] = uint64(i + 1)
	}
	return v
}
