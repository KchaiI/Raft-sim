package raft

// メンバーシップ変更 (博士論文 §4, single-server change) と
// リーダーシップ移譲 (§3.10)。

func (n *Node) rejectPropose() {
	n.out.Reply = &ProposeReply{OK: false, LeaderHint: n.leaderID}
}

// proposeConfChange は single-server 構成変更を開始する。
// 直前の変更が commit されるまで次の変更は受け付けない (§4.1)。
func (n *Node) proposeConfChange(ch ConfChange) {
	if n.state != StateLeader || n.transferee != None {
		n.rejectPropose()
		return
	}
	if n.configIndex > n.commitIndex || n.learner != None {
		n.rejectPropose() // 変更が in-flight
		return
	}
	switch ch.Type {
	case ConfAddServer:
		if n.config.Contains(ch.ID) || ch.ID == n.id || ch.ID == None {
			n.rejectPropose()
			return
		}
		// catch-up フェーズ開始 (D-010): 非投票メンバーとして複製し、
		// 追いついた時点で構成エントリを append する
		n.learner = ch.ID
		n.prs[ch.ID] = &progress{next: n.log.lastIndex() + 1}
		n.out.Reply = &ProposeReply{OK: true, Term: n.term}
		n.sendAppend(ch.ID)
	case ConfRemoveServer:
		if !n.config.Contains(ch.ID) || len(n.config.Voters) == 1 {
			n.rejectPropose()
			return
		}
		cfg := Config{}
		for _, v := range n.config.Voters {
			if v != ch.ID {
				cfg.Voters = append(cfg.Voters, v)
			}
		}
		n.appendEntry(Entry{Type: EntryConfig, Data: encodeConfig(cfg)})
		if ch.ID != n.id {
			delete(n.prs, ch.ID)
		}
		n.out.Reply = &ProposeReply{OK: true, Index: n.log.lastIndex(), Term: n.term}
		n.maybeCommit()
		if n.state == StateLeader {
			n.broadcastAppend()
		}
	default:
		n.rejectPropose()
	}
}

// maybePromoteLearner は catch-up 完了した新サーバーを投票メンバーへ昇格する。
func (n *Node) maybePromoteLearner(from uint64) {
	if n.learner == None || n.learner != from || n.state != StateLeader {
		return
	}
	pr := n.prs[from]
	if pr == nil || pr.match < n.log.lastIndex() {
		return
	}
	cfg := n.config.Clone()
	cfg.Voters = insertSorted(cfg.Voters, from)
	n.learner = None
	n.appendEntry(Entry{Type: EntryConfig, Data: encodeConfig(cfg)})
	n.maybeCommit()
	if n.state == StateLeader {
		n.broadcastAppend()
	}
}

// afterCommit は commit 前進後のフック。自分を除いた構成が commit されたら
// リーダーは退位する (博士論文 §4.2.2)。
func (n *Node) afterCommit() {
	if n.state == StateLeader && n.configIndex <= n.commitIndex && !n.config.Contains(n.id) {
		n.becomeFollower(n.term, None)
	}
}

// transferLeadership はリーダーシップ移譲を開始する (博士論文 §3.10)。
// 対象のログが追いつくのを待って TimeoutNow を送る。移譲中は新規提案を拒否。
func (n *Node) transferLeadership(target uint64) {
	if n.state != StateLeader || target == n.id || !n.config.Contains(target) {
		n.rejectPropose()
		return
	}
	n.transferee = target
	n.transferElapsed = 0
	n.out.Reply = &ProposeReply{OK: true, Term: n.term}
	if pr := n.prs[target]; pr != nil && pr.match == n.log.lastIndex() {
		n.send(Message{Type: MsgTimeoutNow, To: target, Term: n.term})
		n.transferee = None
	} else {
		n.sendAppend(target)
	}
}

// timeoutNow は TimeoutNow を受けた側: Pre-Vote を飛ばし stickiness を
// 無視させる強制選挙を直ちに開始する。
func (n *Node) timeoutNow(_ Message) {
	if n.state == StateLeader || !n.promotable() {
		return
	}
	n.campaign(false, true)
}

func insertSorted(s []uint64, v uint64) []uint64 {
	out := make([]uint64, 0, len(s)+1)
	inserted := false
	for _, x := range s {
		if !inserted && v < x {
			out = append(out, v)
			inserted = true
		}
		if x == v {
			inserted = true
		}
		out = append(out, x)
	}
	if !inserted {
		out = append(out, v)
	}
	return out
}
