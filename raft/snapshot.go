package raft

import "fmt"

// スナップショット (ログ圧縮 + InstallSnapshot RPC, 論文 §7)。

func cloneSnap(s *Snapshot) *Snapshot {
	return &Snapshot{Index: s.Index, Term: s.Term, Config: s.Config.Clone(), Data: s.Data}
}

// createSnapshot はアプリからの「index までの状態を data として保存した」通知。
// ログを圧縮しスナップショットを耐久化する。
func (n *Node) createSnapshot(index uint64, data []byte) {
	if index <= n.log.snapIndex {
		return // 既に圧縮済み (古い通知)
	}
	if index > n.lastApplied {
		panic(fmt.Sprintf("raft: snapshot index %d > lastApplied %d", index, n.lastApplied))
	}
	t, ok := n.log.term(index)
	if !ok {
		panic(fmt.Sprintf("raft: snapshot index %d のエントリがない", index))
	}
	snap := &Snapshot{Index: index, Term: t, Config: n.configAt(index), Data: data}
	n.log.compact(index, t)
	n.snapshot = snap
	n.pruneConfHistory(index)
	n.snapDirty = &Persist{Snapshot: snap}
}

// sendSnapshot は nextIndex が圧縮済み領域まで下がったフォロワーへ
// スナップショット全体を 1 メッセージで送る (D-011)。
func (n *Node) sendSnapshot(to uint64, pr *progress) {
	if pr.pendingSnap {
		return // 応答待ち (CheckQuorum 周期でリセットして再送する)
	}
	if n.snapshot == nil {
		panic(fmt.Sprintf("raft: leader %d に snapshot がないのに next=%d <= snapIndex=%d", n.id, pr.next, n.log.snapIndex))
	}
	pr.pendingSnap = true
	n.send(Message{Type: MsgSnap, To: to, Term: n.term, Snapshot: cloneSnap(n.snapshot)})
}

// installSnapshot はフォロワー側の InstallSnapshot 処理。
func (n *Node) installSnapshot(m Message) {
	if n.state != StateFollower {
		n.becomeFollower(m.Term, m.From)
	}
	n.leaderID = m.From
	n.electionElapsed = 0
	s := m.Snapshot
	if s == nil {
		return
	}
	if s.Index <= n.commitIndex {
		// 手元の commit 済み範囲に含まれる: 進捗のみ返す
		n.send(Message{Type: MsgSnapResp, To: m.From, Term: n.term, MatchIndex: n.commitIndex})
		return
	}
	if t, ok := n.log.term(s.Index); ok && t == s.Term {
		// ログが snapshot 地点を含む: そこまで apply してから圧縮 (suffix 保持)
		n.commitIndex = s.Index
		n.applyCommitted()
		n.log.compact(s.Index, s.Term)
		n.snapshot = cloneSnap(s)
		n.pruneConfHistory(s.Index)
		n.snapDirty = &Persist{Snapshot: n.snapshot}
		n.send(Message{Type: MsgSnapResp, To: m.From, Term: n.term, MatchIndex: s.Index})
		return
	}
	// ログと競合: 丸ごと置き換え、アプリ状態も復元させる
	snap := cloneSnap(s)
	n.log.reset(s.Index, s.Term)
	n.commitIndex = s.Index
	n.lastApplied = s.Index
	n.snapshot = snap
	n.initConfHistory(s.Index, snap.Config)
	n.snapDirty = &Persist{Snapshot: snap, ReplaceLog: true}
	n.out.ApplySnapshot = snap
	n.send(Message{Type: MsgSnapResp, To: m.From, Term: n.term, MatchIndex: s.Index})
}

// handleSnapshotResp はリーダー側の InstallSnapshot 応答処理。
func (n *Node) handleSnapshotResp(m Message) {
	if n.state != StateLeader || m.Term != n.term {
		return
	}
	pr := n.prs[m.From]
	if pr == nil {
		return
	}
	pr.recentActive = true
	pr.pendingSnap = false
	if m.MatchIndex > pr.match {
		pr.match = m.MatchIndex
	}
	if pr.match+1 > pr.next {
		pr.next = pr.match + 1
	}
	n.maybeCommit()
	if n.state != StateLeader {
		return
	}
	n.maybePromoteLearner(m.From)
	if pr.next <= n.log.lastIndex() {
		n.sendAppend(m.From)
	}
}
