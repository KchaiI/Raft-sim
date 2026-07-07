package raft

import "testing"

// 決定論的な固定 RNG (raft パッケージのテストも math/rand を import しない)。
type fixedRNG struct{ v int }

func (r fixedRNG) Intn(n int) int {
	if r.v >= n {
		return n - 1
	}
	return r.v
}

func testParams(id uint64, voters []uint64) Params {
	return Params{ID: id, Voters: voters, ElectionTicks: 10, HeartbeatTicks: 2, PreVote: true, RNG: fixedRNG{}}
}

// tickUntilCampaign はタイムアウトまで tick を入れ、最後の Output を返す。
func tickUntilCampaign(n *Node) Output {
	var out Output
	for i := 0; i < 25; i++ {
		out = n.Step(Tick{})
		if n.State() != StateFollower {
			return out
		}
	}
	return out
}

func TestSingleNodeBecomesLeaderAndCommits(t *testing.T) {
	n := NewNode(testParams(1, []uint64{1}))
	out := tickUntilCampaign(n)
	if n.State() != StateLeader {
		t.Fatalf("単一ノードがリーダーにならない: %s", n.State())
	}
	if n.Term() != 1 {
		t.Fatalf("term = %d (期待 1)", n.Term())
	}
	// 就任 no-op が即 commit & apply される
	if len(out.Applied) != 1 || out.Applied[0].Type != EntryNoop {
		t.Fatalf("no-op が apply されない: %+v", out.Applied)
	}
	if out.Persist == nil || out.Persist.HardState == nil || len(out.Persist.Entries) != 1 {
		t.Fatalf("HardState/エントリの永続化指示がない: %+v", out.Persist)
	}
	// 提案は即 commit
	out = n.Step(Propose{Data: []byte("x")})
	if out.Reply == nil || !out.Reply.OK || out.Reply.Index != 2 {
		t.Fatalf("propose 失敗: %+v", out.Reply)
	}
	if len(out.Applied) != 1 || string(out.Applied[0].Data) != "x" {
		t.Fatalf("提案が apply されない: %+v", out.Applied)
	}
}

func TestPreVoteDoesNotBumpTerm(t *testing.T) {
	n := NewNode(testParams(1, []uint64{1, 2, 3}))
	out := tickUntilCampaign(n)
	if n.State() != StatePreCandidate {
		t.Fatalf("PreCandidate にならない: %s", n.State())
	}
	if n.Term() != 0 {
		t.Fatalf("Pre-Vote で term が変わった: %d", n.Term())
	}
	if len(out.Messages) != 2 {
		t.Fatalf("PreVote 要求が 2 通でない: %d", len(out.Messages))
	}
	for _, m := range out.Messages {
		if m.Type != MsgVote || !m.PreVote || m.Term != 1 {
			t.Fatalf("不正な PreVote 要求: %+v", m)
		}
	}
	if out.Persist != nil {
		t.Fatalf("Pre-Vote で永続化が発生した: %+v", out.Persist)
	}
}

func TestPreVoteMajorityStartsRealElection(t *testing.T) {
	n := NewNode(testParams(1, []uint64{1, 2, 3}))
	tickUntilCampaign(n)
	out := n.Step(Receive{Msg: Message{Type: MsgVoteResp, From: 2, To: 1, Term: 1, PreVote: true, Granted: true}})
	if n.State() != StateCandidate || n.Term() != 1 {
		t.Fatalf("実選挙へ移行しない: %s term=%d", n.State(), n.Term())
	}
	if out.Persist == nil || out.Persist.HardState == nil || out.Persist.HardState.VotedFor != 1 {
		t.Fatalf("自票の永続化がない: %+v", out.Persist)
	}
	// 実投票の過半数でリーダーへ
	out = n.Step(Receive{Msg: Message{Type: MsgVoteResp, From: 3, To: 1, Term: 1, Granted: true}})
	if n.State() != StateLeader {
		t.Fatalf("リーダーにならない: %s", n.State())
	}
	// no-op が全ピアへブロードキャストされる
	apps := 0
	for _, m := range out.Messages {
		if m.Type == MsgApp && len(m.Entries) == 1 && m.Entries[0].Type == EntryNoop {
			apps++
		}
	}
	if apps != 2 {
		t.Fatalf("no-op のブロードキャストが %d 通 (期待 2)", apps)
	}
}

func TestVoteGrantRules(t *testing.T) {
	n := NewNode(testParams(2, []uint64{1, 2, 3}))
	// term 1 での投票要求 → 授与
	out := n.Step(Receive{Msg: Message{Type: MsgVote, From: 1, To: 2, Term: 1, LastLogIndex: 0, LastLogTerm: 0}})
	if len(out.Messages) != 1 || !out.Messages[0].Granted {
		t.Fatalf("投票が授与されない: %+v", out.Messages)
	}
	if out.Persist == nil || out.Persist.HardState == nil || out.Persist.HardState.VotedFor != 1 {
		t.Fatalf("votedFor の永続化がない")
	}
	// 同 term で別候補への投票は拒否
	out = n.Step(Receive{Msg: Message{Type: MsgVote, From: 3, To: 2, Term: 1, LastLogIndex: 0, LastLogTerm: 0}})
	if out.Messages[0].Granted {
		t.Fatal("二重投票が発生")
	}
	// 同候補への再要求 (重複メッセージ) は再授与 (冪等)
	out = n.Step(Receive{Msg: Message{Type: MsgVote, From: 1, To: 2, Term: 1}})
	if !out.Messages[0].Granted {
		t.Fatal("同一候補への再授与が拒否された")
	}
}

func TestVoteRejectsStaleLog(t *testing.T) {
	n := NewNode(testParams(2, []uint64{1, 2, 3}))
	// ログにエントリを持たせる (term 1 のリーダーから複製)
	n.Step(Receive{Msg: Message{Type: MsgApp, From: 1, To: 2, Term: 1,
		Entries: []Entry{{Index: 1, Term: 1, Type: EntryNoop}}}})
	// 空ログの候補 (term 2) は拒否される
	out := n.Step(Receive{Msg: Message{Type: MsgVote, From: 3, To: 2, Term: 2, Force: true,
		LastLogIndex: 0, LastLogTerm: 0}})
	var resp *Message
	for i := range out.Messages {
		if out.Messages[i].Type == MsgVoteResp {
			resp = &out.Messages[i]
		}
	}
	if resp == nil || resp.Granted {
		t.Fatalf("古いログの候補に投票した: %+v", out.Messages)
	}
	// term は進んでいる (§5.1)
	if n.Term() != 2 {
		t.Fatalf("term が進まない: %d", n.Term())
	}
}

func TestLeaderStickinessRejectsVote(t *testing.T) {
	n := NewNode(testParams(2, []uint64{1, 2, 3}))
	// リーダー 1 からハートビートを受けた直後
	n.Step(Receive{Msg: Message{Type: MsgApp, From: 1, To: 2, Term: 1}})
	if n.LeaderID() != 1 {
		t.Fatalf("リーダーを認識しない")
	}
	// 除去されたはずのノード 3 が高い term で選挙を仕掛けても無視 (term も進めない)
	out := n.Step(Receive{Msg: Message{Type: MsgVote, From: 3, To: 2, Term: 5,
		LastLogIndex: 9, LastLogTerm: 4}})
	if n.Term() != 1 {
		t.Fatalf("stickiness が効かず term が %d に", n.Term())
	}
	if len(out.Messages) != 1 || out.Messages[0].Granted {
		t.Fatalf("拒否応答がない: %+v", out.Messages)
	}
	// Force (リーダーシップ移譲) なら受け付ける
	n.Step(Receive{Msg: Message{Type: MsgVote, From: 3, To: 2, Term: 5, Force: true,
		LastLogIndex: 9, LastLogTerm: 4}})
	if n.Term() != 5 {
		t.Fatalf("Force 投票で term が進まない: %d", n.Term())
	}
}

func TestHigherTermAppendMakesFollower(t *testing.T) {
	n := NewNode(testParams(1, []uint64{1}))
	tickUntilCampaign(n)
	if n.State() != StateLeader {
		t.Fatal("前提: リーダー")
	}
	n.Step(Receive{Msg: Message{Type: MsgApp, From: 2, To: 1, Term: 99}})
	if n.State() != StateFollower || n.Term() != 99 || n.LeaderID() != 2 {
		t.Fatalf("退位しない: %s term=%d leader=%d", n.State(), n.Term(), n.LeaderID())
	}
}

func TestCheckQuorumStepsDown(t *testing.T) {
	// 3 ノード中 1 ノードだけのリーダーは ET 経過で退位する
	n := NewNode(testParams(1, []uint64{1, 2, 3}))
	tickUntilCampaign(n) // PreCandidate
	n.Step(Receive{Msg: Message{Type: MsgVoteResp, From: 2, To: 1, Term: 1, PreVote: true, Granted: true}})
	n.Step(Receive{Msg: Message{Type: MsgVoteResp, From: 2, To: 1, Term: 1, Granted: true}})
	if n.State() != StateLeader {
		t.Fatal("前提: リーダー")
	}
	for i := 0; i < 25 && n.State() == StateLeader; i++ {
		n.Step(Tick{})
	}
	if n.State() != StateFollower {
		t.Fatalf("CheckQuorum で退位しない: %s", n.State())
	}
}

func TestProposeRejectedByNonLeader(t *testing.T) {
	n := NewNode(testParams(2, []uint64{1, 2, 3}))
	n.Step(Receive{Msg: Message{Type: MsgApp, From: 1, To: 2, Term: 1}})
	out := n.Step(Propose{Data: []byte("x")})
	if out.Reply == nil || out.Reply.OK {
		t.Fatalf("非リーダーが提案を受理: %+v", out.Reply)
	}
	if out.Reply.LeaderHint != 1 {
		t.Fatalf("リーダーヒントがない: %+v", out.Reply)
	}
}
