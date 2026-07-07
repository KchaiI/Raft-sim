// Package checker は Raft の安全性不変条件 (論文 Figure 3) と
// 線形化可能性の機械検査を提供する。
package checker

import (
	"fmt"
	"hash/fnv"

	"raftsim/raft"
)

// entryHash はエントリ内容の指紋 (Type + Data)。
func entryHash(typ raft.EntryType, data []byte) uint64 {
	h := fnv.New64a()
	h.Write([]byte{byte(typ)})
	h.Write(data)
	return h.Sum64()
}

type entryKey struct {
	index uint64
	term  uint64
}

type chainVal struct {
	hash     uint64
	prevTerm uint64
}

type committedVal struct {
	term uint64
	hash uint64
	// commitTermUB は「このエントリを commit したリーダーの term」の健全な上界。
	// commit を観測したノードの currentTerm (観測のたびに min で引き締める)。
	// Leader Completeness は commit term より後の term のリーダーにのみ
	// 要求されるため (論文 §5.4.3)、stale リーダー (分断中でまだ退位して
	// いない古い term のリーダー) への偽陽性を避けるのに必要。
	commitTermUB uint64
}

func (c committedVal) sameEntry(o committedVal) bool {
	return c.term == o.term && c.hash == o.hash
}

type nodeTrack struct {
	restarted bool
	term      uint64
	commit    uint64

	// Leader Append-Only 検査
	leaderTerm     uint64 // 0 = 直近観測で非リーダー
	leaderLast     uint64
	leaderLastTerm uint64

	// ログ連鎖検査の増分化: 前回観測時のログ terms コピー
	logFirst uint64 // terms[0] の index
	terms    []uint64

	// Leader Completeness の増分検査位置 (commitOrder のオフセット)
	verified int

	// State Machine Safety: apply 順序の連続性
	appliedTo    uint64
	appliedReset bool // 再起動直後: 次の apply から連続性を再開

	voters []uint64 // 前回観測時の構成 (単一サーバー変更の検査)
}

// Invariants は SPEC §4.1 の安全性不変条件チェッカー。
// 毎イベント後に Check を呼ぶ。検査は増分的で、等価な全量検査と同じ違反を検出する。
type Invariants struct {
	violations []string

	leaders map[uint64]uint64 // term → leader (Election Safety)

	// Log Matching: (index,term) → (内容ハッシュ, 直前エントリの term)。
	// 同一 (index,term) の内容と親が全ログで一致するなら、帰納的に
	// 「同 index 同 term なら以前が完全一致」が成り立つ。
	chain map[entryKey]chainVal

	// commit 済みエントリの記録 (Leader Completeness / SM Safety 用)
	committed   map[uint64]committedVal
	commitOrder []uint64 // committed に追加された index の順序列

	// State Machine Safety: apply された (index → 内容)
	applied map[uint64]committedVal

	tracks map[uint64]*nodeTrack
}

func NewInvariants() *Invariants {
	return &Invariants{
		leaders:   map[uint64]uint64{},
		chain:     map[entryKey]chainVal{},
		committed: map[uint64]committedVal{},
		applied:   map[uint64]committedVal{},
		tracks:    map[uint64]*nodeTrack{},
	}
}

func (v *Invariants) violate(format string, args ...interface{}) {
	v.violations = append(v.violations, fmt.Sprintf(format, args...))
}

// Violations は検出済み違反の一覧。
func (v *Invariants) Violations() []string { return v.violations }

// OK は違反ゼロかを返す。
func (v *Invariants) OK() bool { return len(v.violations) == 0 }

func (v *Invariants) track(id uint64) *nodeTrack {
	t := v.tracks[id]
	if t == nil {
		t = &nodeTrack{}
		v.tracks[id] = t
	}
	return t
}

// ObserveRestart はノードのクラッシュ再起動を通知する。揮発状態
// (commitIndex, lastApplied) の巻き戻しと、fsync 境界での term 巻き戻りを許す。
func (v *Invariants) ObserveRestart(id uint64) {
	t := v.track(id)
	t.restarted = true
	t.appliedReset = true
	t.leaderTerm = 0
	t.terms = nil
	t.logFirst = 0
	t.verified = 0
	t.voters = nil
}

// ObserveApply はノードがエントリを apply したことを通知する
// (State Machine Safety: 同 index に異なるコマンドを apply したら違反)。
func (v *Invariants) ObserveApply(id uint64, e raft.Entry) {
	t := v.track(id)
	if t.appliedReset {
		t.appliedReset = false
	} else if e.Index != t.appliedTo+1 {
		v.violate("apply 順序違反: node %d が %d の次に %d を apply", id, t.appliedTo, e.Index)
	}
	t.appliedTo = e.Index
	h := entryHash(e.Type, e.Data)
	if prev, ok := v.applied[e.Index]; ok {
		if prev.term != e.Term || prev.hash != h {
			v.violate("State Machine Safety 違反: index %d に異なるコマンド (term %d vs %d)", e.Index, prev.term, e.Term)
		}
	} else {
		v.applied[e.Index] = committedVal{term: e.Term, hash: h}
	}
	if c, ok := v.committed[e.Index]; ok && (c.term != e.Term || c.hash != h) {
		v.violate("apply されたエントリが committed 記録と不一致: index %d", e.Index)
	}
}

// ObserveSnapshotRestore はスナップショットからのアプリ状態復元を通知する。
func (v *Invariants) ObserveSnapshotRestore(id uint64, s *raft.Snapshot) {
	t := v.track(id)
	t.appliedReset = true
	t.appliedTo = s.Index
	if c, ok := v.committed[s.Index]; ok && c.term != s.Term {
		v.violate("スナップショット (index %d, term %d) が committed 記録 term %d と不一致", s.Index, s.Term, c.term)
	}
}

// Check は全生存ノードの状態を検査する。毎イベント後に呼ぶこと。
func (v *Invariants) Check(nodes []*raft.Node) {
	for _, n := range nodes {
		v.checkNode(n)
	}
}

func (v *Invariants) checkNode(n *raft.Node) {
	id := n.ID()
	t := v.track(id)

	// term / commitIndex の単調性 (再起動直後は巻き戻りを許す: D-014)
	if !t.restarted {
		if n.Term() < t.term {
			v.violate("term 単調性違反: node %d が %d → %d", id, t.term, n.Term())
		}
		if n.CommitIndex() < t.commit {
			v.violate("commitIndex 単調性違反: node %d が %d → %d", id, t.commit, n.CommitIndex())
		}
	}
	t.term = n.Term()
	t.commit = n.CommitIndex()

	// 構成の正当性: voters は非空・昇順・重複なし。
	// single-server change (一度に高々1ノードの増減) はリーダーの連続在任中に
	// のみ検査する。フォロワーは catch-up 時に 1 バッチで複数の構成エントリを
	// 受け取る (構成が一気に進む) し、truncate で巻き戻りもするため、
	// 観測ごとの差分検査は成り立たない。構成エントリの内容自体の一意性は
	// ログ連鎖マップが保証する。
	voters := n.ConfigVoters()
	if len(voters) == 0 {
		v.violate("構成が空: node %d", id)
	}
	for i := 1; i < len(voters); i++ {
		if voters[i] <= voters[i-1] {
			v.violate("構成が昇順でない/重複: node %d %v", id, voters)
		}
	}
	if n.State() == raft.StateLeader && t.leaderTerm == n.Term() && t.voters != nil && !t.restarted {
		if d := symmetricDiff(t.voters, voters); d > 1 {
			v.violate("single-server change 違反: leader %d の構成が一度に %d ノード変化 %v → %v", id, d, t.voters, voters)
		}
	}
	t.voters = voters

	// Election Safety: 同一 term に 2 リーダーがいない
	if n.State() == raft.StateLeader {
		if prev, ok := v.leaders[n.Term()]; ok && prev != id {
			v.violate("Election Safety 違反: term %d に leader %d と %d", n.Term(), prev, id)
		} else {
			v.leaders[n.Term()] = id
		}
	}

	// Leader Append-Only: 同一 term のリーダーであり続ける間、
	// lastIndex は減らず、以前の末尾エントリの term も変わらない
	if n.State() == raft.StateLeader {
		if t.leaderTerm == n.Term() && t.leaderLast > 0 {
			if n.LastIndex() < t.leaderLast {
				v.violate("Leader Append-Only 違反: leader %d (term %d) の lastIndex %d → %d", id, n.Term(), t.leaderLast, n.LastIndex())
			} else if t.leaderLast >= n.FirstIndex() {
				if tt, ok := n.TermAt(t.leaderLast); ok && tt != t.leaderLastTerm {
					v.violate("Leader Append-Only 違反: leader %d が index %d を term %d → %d に上書き", id, t.leaderLast, t.leaderLastTerm, tt)
				}
			}
		}
		t.leaderTerm = n.Term()
		t.leaderLast = n.LastIndex()
		t.leaderLastTerm, _ = n.TermAt(n.LastIndex())
	} else {
		t.leaderTerm = 0
	}

	v.checkLogChain(n, t)
	v.recordCommitted(n)
	v.checkLeaderCompleteness(n, t)

	t.restarted = false
}

// checkLogChain はログの変化分を連鎖マップと照合する (Log Matching)。
func (v *Invariants) checkLogChain(n *raft.Node, t *nodeTrack) {
	first, last := n.FirstIndex(), n.LastIndex()

	// 前回観測からの差分開始点を求める
	scanFrom := first
	if t.terms != nil {
		prevLastIdx := t.logFirst + uint64(len(t.terms)) - 1
		scanFrom = prevLastIdx + 1
		lo := first
		if t.logFirst > lo {
			lo = t.logFirst
		}
		for i := lo; i <= prevLastIdx && i <= last; i++ {
			nt, _ := n.TermAt(i)
			if nt != t.terms[i-t.logFirst] {
				scanFrom = i
				break
			}
		}
		if last < scanFrom-1 {
			scanFrom = last + 1 // 純粋な truncate (再スキャン不要)
		}
	}
	if scanFrom < first {
		// スナップショットで first が前回観測の末尾を超えて進んだ場合
		// (InstallSnapshot によるログ置換)。圧縮済み領域は検査対象外。
		scanFrom = first
	}

	for i := scanFrom; i <= last; i++ {
		e := n.EntryAt(i)
		if e == nil {
			v.violate("ログ穴: node %d index %d", n.ID(), i)
			return
		}
		prevTerm, ok := n.TermAt(i - 1)
		if !ok {
			prevTerm = 0 // i-1 == 0 (ログ先頭) の場合
		}
		key := entryKey{index: i, term: e.Term}
		val := chainVal{hash: entryHash(e.Type, e.Data), prevTerm: prevTerm}
		if exist, ok := v.chain[key]; ok {
			if exist != val {
				v.violate("Log Matching 違反: (index %d, term %d) の内容/親が不一致 (node %d)", i, e.Term, n.ID())
			}
		} else {
			v.chain[key] = val
		}
	}

	// terms コピーを更新
	sz := int(last - first + 1)
	if cap(t.terms) < sz {
		t.terms = make([]uint64, sz)
	}
	t.terms = t.terms[:sz]
	for i := first; i <= last; i++ {
		tt, _ := n.TermAt(i)
		t.terms[i-first] = tt
	}
	t.logFirst = first
}

// recordCommitted はノードの commitIndex までのエントリを大域 committed 集合へ記録する。
func (v *Invariants) recordCommitted(n *raft.Node) {
	c := n.CommitIndex()
	if c > n.LastIndex() {
		v.violate("commitIndex %d > lastIndex %d: node %d", c, n.LastIndex(), n.ID())
		return
	}
	for i := n.FirstIndex(); i <= c; i++ {
		e := n.EntryAt(i)
		if e == nil {
			continue
		}
		val := committedVal{term: e.Term, hash: entryHash(e.Type, e.Data), commitTermUB: n.Term()}
		if exist, ok := v.committed[i]; ok {
			if !exist.sameEntry(val) {
				v.violate("committed エントリの不一致: index %d term %d vs %d (node %d)", i, exist.term, e.Term, n.ID())
			}
			if val.commitTermUB < exist.commitTermUB {
				exist.commitTermUB = val.commitTermUB
				v.committed[i] = exist
			}
		} else {
			v.committed[i] = val
			v.commitOrder = append(v.commitOrder, i)
		}
	}
	// スナップショット地点も commit 済み
	si, st := n.SnapIndex(), n.SnapTerm()
	if si > 0 {
		if exist, ok := v.committed[si]; ok && exist.term != st {
			v.violate("スナップショット地点 (index %d, term %d) が committed 記録 term %d と不一致 (node %d)", si, st, exist.term, n.ID())
		}
	}
}

// checkLeaderCompleteness はリーダーのログが全 commit 済みエントリを含むことを検査する。
func (v *Invariants) checkLeaderCompleteness(n *raft.Node, t *nodeTrack) {
	if n.State() != raft.StateLeader {
		t.verified = 0 // 次にリーダーになったら全量再検証
		return
	}
	for ; t.verified < len(v.commitOrder); t.verified++ {
		i := v.commitOrder[t.verified]
		c := v.committed[i]
		switch {
		case n.Term() <= c.commitTermUB:
			// commit したリーダーの term 以前のリーダーには要求されない
			// (stale リーダーが後続 term の commit を持たないのは正当)
		case i <= n.SnapIndex():
			// スナップショットに含まれる (recordCommitted で lineage を検査済み)
		case i > n.LastIndex():
			v.violate("Leader Completeness 違反: leader %d (term %d) のログに committed index %d がない", n.ID(), n.Term(), i)
		default:
			if tt, _ := n.TermAt(i); tt != c.term {
				v.violate("Leader Completeness 違反: leader %d の index %d が term %d (committed は term %d)", n.ID(), i, tt, c.term)
			}
		}
	}
}

func symmetricDiff(a, b []uint64) int {
	d := 0
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		switch {
		case a[i] == b[j]:
			i++
			j++
		case a[i] < b[j]:
			d++
			i++
		default:
			d++
			j++
		}
	}
	return d + (len(a) - i) + (len(b) - j)
}
