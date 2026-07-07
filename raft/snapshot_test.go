package raft

import "testing"

// スナップショット (ログ圧縮 + InstallSnapshot) の単体テスト。

// 単一ノードのリーダーに n 件 propose して commit させる。
func leaderWithEntries(t *testing.T, n int) *Node {
	t.Helper()
	nd := NewNode(testParams(1, []uint64{1}))
	tickUntilCampaign(nd)
	if nd.State() != StateLeader {
		t.Fatal("前提: リーダー")
	}
	for i := 0; i < n; i++ {
		out := nd.Step(Propose{Data: []byte{byte(i)}})
		if !out.Reply.OK {
			t.Fatal("propose 失敗")
		}
	}
	return nd
}

func TestCreateSnapshotCompactsLog(t *testing.T) {
	n := leaderWithEntries(t, 5) // log: noop + 5 = index 6
	out := n.Step(CreateSnapshot{Index: 4, Data: []byte("app-state")})
	if n.SnapIndex() != 4 || n.FirstIndex() != 5 {
		t.Fatalf("圧縮されない: snap=%d first=%d", n.SnapIndex(), n.FirstIndex())
	}
	if n.LastIndex() != 6 {
		t.Fatalf("suffix が消えた: last=%d", n.LastIndex())
	}
	p := out.Persist
	if p == nil || p.Snapshot == nil || p.Snapshot.Index != 4 || p.ReplaceLog {
		t.Fatalf("Persist が不正: %+v", p)
	}
	if string(p.Snapshot.Data) != "app-state" {
		t.Fatal("アプリ状態が入っていない")
	}
	// 古い通知は無視
	out = n.Step(CreateSnapshot{Index: 3, Data: nil})
	if out.Persist != nil || n.SnapIndex() != 4 {
		t.Fatalf("古いスナップショット通知が適用された")
	}
}

// 圧縮済み領域を要求する遅延フォロワーには MsgSnap が送られる。
func TestLeaderSendsSnapshotToLaggingFollower(t *testing.T) {
	// 3 ノード構成: フォロワー 2 は追従、フォロワー 3 は一度も応答しない (ダウン中)
	n := NewNode(testParams(1, []uint64{1, 2, 3}))
	tickUntilCampaign(n)
	n.Step(Receive{Msg: Message{Type: MsgVoteResp, From: 2, To: 1, Term: 1, PreVote: true, Granted: true}})
	n.Step(Receive{Msg: Message{Type: MsgVoteResp, From: 2, To: 1, Term: 1, Granted: true}})
	if n.State() != StateLeader {
		t.Fatal("前提: リーダー")
	}
	for i := 0; i < 5; i++ {
		n.Step(Propose{Data: []byte{byte(i)}})
	}
	// フォロワー 2 が全複製 → 過半数 (1,2) で commit=6
	n.Step(Receive{Msg: Message{Type: MsgAppResp, From: 2, To: 1, Term: 1, Success: true, MatchIndex: 6}})
	if n.CommitIndex() != 6 {
		t.Fatalf("前提: commit=6 (got %d)", n.CommitIndex())
	}
	n.Step(CreateSnapshot{Index: 6, Data: []byte("S")})
	// ダウンしていたフォロワー 3 が復帰し、ハートビートを「何も持っていない」と拒否
	out := n.Step(Receive{Msg: Message{Type: MsgAppResp, From: 3, To: 1, Term: 1,
		Success: false, ConflictIndex: 1}})
	var snap *Message
	for i := range out.Messages {
		if out.Messages[i].Type == MsgSnap {
			snap = &out.Messages[i]
		}
	}
	if snap == nil || snap.Snapshot == nil || snap.Snapshot.Index != 6 {
		t.Fatalf("MsgSnap が送られない: %+v", out.Messages)
	}
	if string(snap.Snapshot.Data) != "S" {
		t.Fatal("スナップショットデータ不一致")
	}
	// 応答が来るまで再送しない (pendingSnap)
	out = n.Step(Receive{Msg: Message{Type: MsgAppResp, From: 3, To: 1, Term: 1,
		Success: false, ConflictIndex: 1}})
	for _, m := range out.Messages {
		if m.Type == MsgSnap {
			t.Fatal("pendingSnap 中に再送された")
		}
	}
	// 応答で progress が進む
	n.Step(Receive{Msg: Message{Type: MsgSnapResp, From: 3, To: 1, Term: 1, MatchIndex: 6}})
	out = n.Step(Propose{Data: []byte("after")})
	found := false
	for _, m := range out.Messages {
		if m.Type == MsgApp && m.To == 3 && m.PrevLogIndex == 6 {
			found = true
		}
	}
	if !found {
		t.Fatalf("スナップショット後の複製が再開しない: %+v", out.Messages)
	}
}

func TestInstallSnapshotReplacesConflictingLog(t *testing.T) {
	n := followerWithLog(t, []uint64{1, 1, 1})
	snap := &Snapshot{Index: 5, Term: 3, Config: Config{Voters: []uint64{1, 2, 3}}, Data: []byte("S")}
	out := n.Step(Receive{Msg: Message{Type: MsgSnap, From: 1, To: 2, Term: 3, Snapshot: snap}})
	if n.SnapIndex() != 5 || n.LastIndex() != 5 || n.CommitIndex() != 5 {
		t.Fatalf("置き換えられない: snap=%d last=%d commit=%d", n.SnapIndex(), n.LastIndex(), n.CommitIndex())
	}
	if out.ApplySnapshot == nil || string(out.ApplySnapshot.Data) != "S" {
		t.Fatalf("アプリ復元指示がない: %+v", out.ApplySnapshot)
	}
	if out.Persist == nil || out.Persist.Snapshot == nil || !out.Persist.ReplaceLog {
		t.Fatalf("Persist が不正: %+v", out.Persist)
	}
	var resp *Message
	for i := range out.Messages {
		if out.Messages[i].Type == MsgSnapResp {
			resp = &out.Messages[i]
		}
	}
	if resp == nil || resp.MatchIndex != 5 {
		t.Fatalf("SnapResp が不正: %+v", resp)
	}
}

// ログがスナップショット地点を含む場合は suffix を保持して圧縮のみ行う。
func TestInstallSnapshotKeepsMatchingSuffix(t *testing.T) {
	n := followerWithLog(t, []uint64{1, 1, 1, 1})
	snap := &Snapshot{Index: 2, Term: 1, Config: Config{Voters: []uint64{1, 2, 3}}, Data: []byte("S")}
	out := n.Step(Receive{Msg: Message{Type: MsgSnap, From: 1, To: 2, Term: 1, Snapshot: snap}})
	if n.SnapIndex() != 2 || n.LastIndex() != 4 {
		t.Fatalf("suffix が保持されない: snap=%d last=%d", n.SnapIndex(), n.LastIndex())
	}
	if out.ApplySnapshot != nil {
		t.Fatal("suffix 保持パスでアプリ復元が指示された")
	}
	// 地点までのエントリは apply される
	if n.CommitIndex() != 2 || len(out.Applied) != 2 {
		t.Fatalf("commit/apply が不正: commit=%d applied=%d", n.CommitIndex(), len(out.Applied))
	}
	if out.Persist == nil || out.Persist.Snapshot == nil || out.Persist.ReplaceLog {
		t.Fatalf("Persist が不正: %+v", out.Persist)
	}
	// 古いスナップショット (commit 済み範囲) は無視して進捗のみ返す
	old := &Snapshot{Index: 1, Term: 1, Config: Config{Voters: []uint64{1, 2, 3}}}
	out = n.Step(Receive{Msg: Message{Type: MsgSnap, From: 1, To: 2, Term: 1, Snapshot: old}})
	if n.SnapIndex() != 2 {
		t.Fatal("古いスナップショットで巻き戻った")
	}
}
