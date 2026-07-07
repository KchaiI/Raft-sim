package raft

import "testing"

// AppendEntries の一貫性チェックと conflict 高速バックアップの単体テスト。

// フォロワーに指定 term 列のログを作る。
func followerWithLog(t *testing.T, terms []uint64) *Node {
	t.Helper()
	n := NewNode(testParams(2, []uint64{1, 2, 3}))
	var ents []Entry
	for i, tm := range terms {
		ents = append(ents, Entry{Index: uint64(i + 1), Term: tm, Type: EntryNormal, Data: []byte{byte(i)}})
	}
	maxTerm := terms[len(terms)-1]
	out := n.Step(Receive{Msg: Message{Type: MsgApp, From: 1, To: 2, Term: maxTerm,
		PrevLogIndex: 0, PrevLogTerm: 0, Entries: ents}})
	resp := lastAppResp(out)
	if resp == nil || !resp.Success {
		t.Fatalf("前提ログの構築に失敗: %+v", out.Messages)
	}
	return n
}

func lastAppResp(out Output) *Message {
	for i := len(out.Messages) - 1; i >= 0; i-- {
		if out.Messages[i].Type == MsgAppResp {
			return &out.Messages[i]
		}
	}
	return nil
}

func TestAppendConsistencyCheckMissing(t *testing.T) {
	n := followerWithLog(t, []uint64{1, 1})
	// prev=5 は存在しない → ConflictIndex = lastIndex+1, ConflictTerm = 0
	out := n.Step(Receive{Msg: Message{Type: MsgApp, From: 1, To: 2, Term: 1,
		PrevLogIndex: 5, PrevLogTerm: 1}})
	resp := lastAppResp(out)
	if resp.Success || resp.ConflictIndex != 3 || resp.ConflictTerm != 0 {
		t.Fatalf("欠落時の conflict ヒントが不正: %+v", resp)
	}
}

func TestAppendConsistencyCheckTermMismatch(t *testing.T) {
	n := followerWithLog(t, []uint64{1, 2, 2, 2, 3})
	// prev=4 の term は 2 だがリーダーは 5 と主張 → ConflictTerm=2, その term の最初 index=2
	out := n.Step(Receive{Msg: Message{Type: MsgApp, From: 1, To: 2, Term: 5,
		PrevLogIndex: 4, PrevLogTerm: 5}})
	resp := lastAppResp(out)
	if resp.Success || resp.ConflictTerm != 2 || resp.ConflictIndex != 2 {
		t.Fatalf("term 不一致時の conflict ヒントが不正: %+v", resp)
	}
}

func TestAppendTruncatesConflictingSuffix(t *testing.T) {
	n := followerWithLog(t, []uint64{1, 1, 2, 2})
	// リーダー (term 3) が index 3 から異なるエントリを送る → suffix を上書き
	out := n.Step(Receive{Msg: Message{Type: MsgApp, From: 3, To: 2, Term: 3,
		PrevLogIndex: 2, PrevLogTerm: 1,
		Entries: []Entry{{Index: 3, Term: 3, Type: EntryNormal, Data: []byte("new")}}}})
	resp := lastAppResp(out)
	if !resp.Success || resp.MatchIndex != 3 {
		t.Fatalf("上書き追記に失敗: %+v", resp)
	}
	if n.LastIndex() != 3 {
		t.Fatalf("truncate されていない: lastIndex=%d", n.LastIndex())
	}
	if tm, _ := n.TermAt(3); tm != 3 {
		t.Fatalf("index 3 の term=%d (期待 3)", tm)
	}
	// 永続化指示に truncate 後の追記が含まれる
	if resp := out.Persist; resp == nil || len(resp.Entries) != 1 || resp.Entries[0].Index != 3 {
		t.Fatalf("Persist.Entries が不正: %+v", out.Persist)
	}
}

func TestAppendIdempotentDuplicate(t *testing.T) {
	n := followerWithLog(t, []uint64{1, 1})
	// 同じエントリの重複配送は truncate せず成功応答のみ
	out := n.Step(Receive{Msg: Message{Type: MsgApp, From: 1, To: 2, Term: 1,
		PrevLogIndex: 0, PrevLogTerm: 0,
		Entries: []Entry{{Index: 1, Term: 1, Data: []byte{0}}, {Index: 2, Term: 1, Data: []byte{1}}}}})
	resp := lastAppResp(out)
	if !resp.Success || resp.MatchIndex != 2 {
		t.Fatalf("重複 append の応答が不正: %+v", resp)
	}
	if out.Persist != nil {
		t.Fatalf("重複 append で永続化が発生: %+v", out.Persist)
	}
}

func TestFollowerCommitAdvancesWithLeaderCommit(t *testing.T) {
	n := followerWithLog(t, []uint64{1, 1})
	out := n.Step(Receive{Msg: Message{Type: MsgApp, From: 1, To: 2, Term: 1,
		PrevLogIndex: 2, PrevLogTerm: 1, Commit: 2}})
	if n.CommitIndex() != 2 {
		t.Fatalf("commitIndex=%d (期待 2)", n.CommitIndex())
	}
	if len(out.Applied) != 2 {
		t.Fatalf("apply されない: %+v", out.Applied)
	}
	// leaderCommit が自ログの一致範囲を超えても、新規エントリの末尾までしか進めない
	n2 := followerWithLog(t, []uint64{1})
	n2.Step(Receive{Msg: Message{Type: MsgApp, From: 1, To: 2, Term: 1,
		PrevLogIndex: 1, PrevLogTerm: 1, Commit: 100}})
	if n2.CommitIndex() != 1 {
		t.Fatalf("commitIndex=%d (期待 1: min(leaderCommit, lastNew))", n2.CommitIndex())
	}
}

// リーダー側: conflict ヒントによる nextIndex の高速バックアップ。
func TestLeaderFastBackup(t *testing.T) {
	// リーダー (term 2) を作る: ログ [1,2]
	n := NewNode(testParams(1, []uint64{1, 2, 3}))
	// term1 のログを持たせてからリーダー化
	n.Step(Receive{Msg: Message{Type: MsgApp, From: 3, To: 1, Term: 1,
		Entries: []Entry{{Index: 1, Term: 1, Data: []byte("a")}}}})
	tickUntilCampaign(n) // PreCandidate (term は 1 のまま)
	n.Step(Receive{Msg: Message{Type: MsgVoteResp, From: 2, To: 1, Term: 2, PreVote: true, Granted: true}})
	out := n.Step(Receive{Msg: Message{Type: MsgVoteResp, From: 2, To: 1, Term: 2, Granted: true}})
	if n.State() != StateLeader || n.Term() != 2 {
		t.Fatalf("前提: term2 のリーダー (got %s term=%d)", n.State(), n.Term())
	}
	_ = out
	// フォロワー 2 が「term1 は index1 から」と拒否 → next=2 に合わせて再送
	out = n.Step(Receive{Msg: Message{Type: MsgAppResp, From: 2, To: 1, Term: 2,
		Success: false, ConflictIndex: 1, ConflictTerm: 1}})
	var resent *Message
	for i := range out.Messages {
		if out.Messages[i].Type == MsgApp && out.Messages[i].To == 2 {
			resent = &out.Messages[i]
		}
	}
	if resent == nil {
		t.Fatalf("再送がない: %+v", out.Messages)
	}
	// リーダーは term1 を index1 に持つ → next = lastIndexOfTerm(1)+1 = 2 → prev=1
	if resent.PrevLogIndex != 1 || resent.PrevLogTerm != 1 {
		t.Fatalf("バックアップ位置が不正: prev=%d/%d", resent.PrevLogIndex, resent.PrevLogTerm)
	}
}

// 過半数の match で commit が進み、Applied が出ること。
func TestLeaderCommitsOnMajorityMatch(t *testing.T) {
	n := NewNode(testParams(1, []uint64{1, 2, 3}))
	tickUntilCampaign(n)
	n.Step(Receive{Msg: Message{Type: MsgVoteResp, From: 2, To: 1, Term: 1, PreVote: true, Granted: true}})
	n.Step(Receive{Msg: Message{Type: MsgVoteResp, From: 2, To: 1, Term: 1, Granted: true}})
	out := n.Step(Propose{Data: []byte("x")})
	if !out.Reply.OK || out.Reply.Index != 2 {
		t.Fatalf("propose: %+v", out.Reply)
	}
	// フォロワー 2 が index2 まで複製済みと応答 → 過半数 (self+2) で commit
	out = n.Step(Receive{Msg: Message{Type: MsgAppResp, From: 2, To: 1, Term: 1,
		Success: true, MatchIndex: 2}})
	if n.CommitIndex() != 2 {
		t.Fatalf("commitIndex=%d (期待 2)", n.CommitIndex())
	}
	if len(out.Applied) != 2 { // no-op + x
		t.Fatalf("applied=%d 件 (期待 2)", len(out.Applied))
	}
}
