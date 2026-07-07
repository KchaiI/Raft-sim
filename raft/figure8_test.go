package raft

import "testing"

// 論文 Figure 8 のシナリオの忠実な再現。
//
// 問題の核心: 旧 term のエントリが過半数に複製されていても、それだけでは
// commit してはならない (§5.4.2)。リーダーは自分の term のエントリが過半数に
// 達したときにのみ、それ以前のエントリも間接的に commit できる。
//
// このテストは 5 ノードの全メッセージを手動で配送し、論文の (a)〜(e) の
// 状態遷移を正確に組み立てる。選挙は TimeoutNow (リーダーシップ移譲と同じ
// 強制選挙経路) で決定論的に駆動する。commit 規則の検証には影響しない。

func f8Params(id uint64) Params {
	return Params{ID: id, Voters: []uint64{1, 2, 3, 4, 5},
		ElectionTicks: 10, HeartbeatTicks: 2, PreVote: false, RNG: fixedRNG{}}
}

// tryElect は cand に強制選挙をさせ、指定 voters の票だけを配送する。
// 当選したら (true, 就任時の出力)。votedFor の衝突があるため最大 4 round 試す。
func tryElect(cand *Node, voters ...*Node) (bool, Output) {
	for round := 0; round < 4; round++ {
		out := cand.Step(Receive{Msg: Message{Type: MsgTimeoutNow, From: cand.ID(), To: cand.ID(), Term: cand.Term()}})
		if cand.State() == StateLeader {
			return true, out
		}
		for _, m := range out.Messages {
			if m.Type != MsgVote {
				continue
			}
			for _, v := range voters {
				if v.ID() != m.To {
					continue
				}
				vout := v.Step(Receive{Msg: m})
				for _, r := range vout.Messages {
					if r.Type == MsgVoteResp && r.To == cand.ID() {
						res := cand.Step(Receive{Msg: r})
						if cand.State() == StateLeader {
							return true, res
						}
					}
				}
			}
		}
	}
	return false, Output{}
}

// crashRestart はノードのクラッシュ+再起動を模す: 永続状態 (term/votedFor/log)
// のみから作り直す (揮発状態・リーダーシップは失われる)。
func crashRestart(n *Node) *Node {
	var es []Entry
	for i := n.FirstIndex(); i <= n.LastIndex(); i++ {
		es = append(es, *n.EntryAt(i))
	}
	return RestartNode(f8Params(n.ID()), Restore{
		HardState: HardState{Term: n.Term(), VotedFor: n.VotedFor()},
		Entries:   es,
	})
}

func electManually(t *testing.T, cand *Node, voters ...*Node) Output {
	t.Helper()
	ok, out := tryElect(cand, voters...)
	if !ok {
		t.Fatalf("node %d がリーダーになれない (term=%d)", cand.ID(), cand.Term())
	}
	return out
}

// pump は alive に含まれるノード間でメッセージを収束するまで配送する
// (完全なネットワークの模擬)。
func pump(t *testing.T, alive map[uint64]*Node, msgs []Message) {
	t.Helper()
	queue := append([]Message(nil), msgs...)
	for guard := 0; len(queue) > 0; guard++ {
		if guard > 10000 {
			t.Fatal("pump が収束しない")
		}
		m := queue[0]
		queue = queue[1:]
		dst := alive[m.To]
		if dst == nil {
			continue
		}
		out := dst.Step(Receive{Msg: m})
		queue = append(queue, out.Messages...)
	}
}

// setupFigure8 は (a)(b) を構築する:
//   - S1 がリーダー (term1)、no-op@1 を全員に commit
//   - S1 が X@2(term1) を append、S2 にのみ複製 (未 commit)
//   - S1 停止。S5 が S3,S4 の票で term2 リーダーになり index2 に no-op(term2) を
//     積むが複製しない。S5 停止。
func setupFigure8(t *testing.T) map[uint64]*Node {
	t.Helper()
	nodes := map[uint64]*Node{}
	for id := uint64(1); id <= 5; id++ {
		nodes[id] = NewNode(f8Params(id))
	}
	s1, s2, s5 := nodes[1], nodes[2], nodes[5]

	out := electManually(t, s1, nodes[2], nodes[3])
	pump(t, nodes, out.Messages)
	if s1.CommitIndex() != 1 || s1.Term() != 1 {
		t.Fatalf("前提: S1 term1 commit=1 (got term=%d commit=%d)", s1.Term(), s1.CommitIndex())
	}

	outX := s1.Step(Propose{Data: []byte("X")})
	for _, m := range outX.Messages {
		if m.To == 2 && m.Type == MsgApp {
			pump(t, map[uint64]*Node{1: s1, 2: s2}, []Message{m})
		}
	}
	if s1.CommitIndex() != 1 {
		t.Fatalf("X が過半数未満で commit された: %d", s1.CommitIndex())
	}
	if tm, _ := s2.TermAt(2); tm != 1 {
		t.Fatalf("前提: S2 が X@2(t1) を持つ (got term=%d)", tm)
	}

	// S1 停止 (以後メッセージを配送しない)。S5 が term2 のリーダーに。
	electManually(t, s5, nodes[3], nodes[4]) // 就任 no-op@2(t2) は複製しない
	if s5.Term() != 2 || s5.LastIndex() != 2 {
		t.Fatalf("前提: S5 term2 last=2 (got term=%d last=%d)", s5.Term(), s5.LastIndex())
	}
	return nodes
}

func TestFigure8UncommittedOldTermEntryIsNotCommittedByReplication(t *testing.T) {
	nodes := setupFigure8(t)
	s1, s2, s3, s4, s5 := nodes[1], nodes[2], nodes[3], nodes[4], nodes[5]

	// (c) S5 停止。S1 がクラッシュから復帰 → term3 のリーダー (票: S2, S3)。
	// ログ: [noop@1(t1), X@2(t1), 就任 no-op@3(t3)]。
	s1 = crashRestart(s1)
	nodes[1] = s1
	electManually(t, s1, s2, s3)
	if s1.Term() != 3 {
		t.Fatalf("前提: S1 term3 (got %d)", s1.Term())
	}
	// X@2(t1) だけを S3 へ複製する (手製メッセージ: 就任 no-op@3 は含めない)
	xEntry := *s1.EntryAt(2)
	fout := s3.Step(Receive{Msg: Message{Type: MsgApp, From: 1, To: 3, Term: 3,
		PrevLogIndex: 1, PrevLogTerm: 1, Entries: []Entry{xEntry}, Commit: 1}})
	for _, r := range fout.Messages {
		if r.To == 1 {
			s1.Step(Receive{Msg: r})
		}
	}
	if tm, _ := s3.TermAt(2); tm != 1 {
		t.Fatalf("前提: S3 が X@2(t1) を受理 (got term=%d)", tm)
	}
	// ★ 核心: X@2(t1) は過半数 {S1,S2,S3} にあるが、term1 のエントリなので
	// term3 のリーダーはこれを commit してはならない
	if s1.CommitIndex() >= 2 {
		t.Fatalf("Figure 8 違反: 旧 term のエントリが複製数だけで commit された (commit=%d)", s1.CommitIndex())
	}

	// (d) S1 停止。S5 がクラッシュから復帰 → S3,S4 の票で当選
	// (S5 の末尾 (2,t2) は S3 の (2,t1) より新しい)。
	s5 = crashRestart(s5)
	nodes[5] = s5
	ok, out := tryElect(s5, s3, s4)
	if !ok {
		t.Fatalf("S5 が再選できない (term=%d)", s5.Term())
	}
	// S5 が自ログを全員 (S1 以外) に複製 → X@2(t1) は正当に上書きされて消える
	alive := map[uint64]*Node{2: s2, 3: s3, 4: s4, 5: s5}
	pump(t, alive, out.Messages)
	for i := 0; i < 4; i++ { // ハートビートで全員へ行き渡らせる
		o := s5.Step(Tick{})
		pump(t, alive, o.Messages)
	}
	for _, id := range []uint64{2, 3, 4} {
		if tm, _ := nodes[id].TermAt(2); tm != 2 {
			t.Fatalf("S%d の index2 が上書きされていない: term=%d", id, tm)
		}
	}
	// S5 の自 term エントリ (就任 no-op) が過半数に達したので commit が進む
	if s5.CommitIndex() < 3 {
		t.Fatalf("S5 の commit が進まない: %d", s5.CommitIndex())
	}
	// X は一度もどのノードでも apply されていない (commit 済みは no-op のみ) ため
	// 上書きは State Machine Safety を破らない。
}

// Figure 8 の対 (正しい commit 経路): S1 (term3) が自 term の no-op を過半数に
// 複製すれば X@2(t1) は間接 commit され、以後 S5 は当選できない。
func TestFigure8CurrentTermReplicationCommitsIndirectly(t *testing.T) {
	nodes := setupFigure8(t)
	s1, s2, s3, s5 := nodes[1], nodes[2], nodes[3], nodes[5]

	// (c') S1 がクラッシュから復帰 → term3。今回は S2, S3 へ全部 (X@2 と no-op@3) を複製する。
	s1 = crashRestart(s1)
	nodes[1] = s1
	out := electManually(t, s1, s2, s3)
	alive := map[uint64]*Node{1: s1, 2: s2, 3: s3}
	pump(t, alive, out.Messages)
	for i := 0; i < 4; i++ {
		o := s1.Step(Tick{})
		pump(t, alive, o.Messages)
	}
	// term3 の no-op@3 が {S1,S2,S3} に → index3 まで commit (X@2 も間接 commit)
	if s1.CommitIndex() < 3 {
		t.Fatalf("自 term 複製で commit が進まない: %d", s1.CommitIndex())
	}

	// (d') S5 がクラッシュから復帰し S2,S3 から当選を試みても、
	// up-to-date 検査で拒否される (S2,S3 は commit 済みエントリを含む term3 の末尾を持つ)。
	s5 = crashRestart(s5)
	nodes[5] = s5
	ok, _ := tryElect(s5, s2, s3)
	if ok {
		t.Fatal("commit 済みエントリを持たない S5 が当選した (Leader Completeness の穴)")
	}
	if s5.State() == StateLeader {
		t.Fatal("S5 がリーダーになっている")
	}
}
