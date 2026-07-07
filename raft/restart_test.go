package raft

import "testing"

// 再起動 (RestartNode) の単体テスト: fsync 済み状態のみから正しく復元されること。

func TestRestartRestoresPersistentState(t *testing.T) {
	n := NewNode(testParams(1, []uint64{1}))
	tickUntilCampaign(n)
	out := n.Step(Propose{Data: []byte("x")})
	if !out.Reply.OK {
		t.Fatal("前提: propose 成功")
	}

	// 耐久状態を模擬 (シミュレータでは storage.Durable が担う)
	r := Restore{
		HardState: HardState{Term: n.Term(), VotedFor: n.VotedFor()},
		Entries: func() []Entry {
			var es []Entry
			for i := uint64(1); i <= n.LastIndex(); i++ {
				es = append(es, *n.EntryAt(i))
			}
			return es
		}(),
	}
	n2 := RestartNode(testParams(1, []uint64{1}), r)

	if n2.State() != StateFollower {
		t.Fatalf("再起動直後は Follower であるべき: %s", n2.State())
	}
	if n2.Term() != n.Term() || n2.VotedFor() != n.VotedFor() {
		t.Fatalf("HardState が復元されない: term=%d votedFor=%d", n2.Term(), n2.VotedFor())
	}
	if n2.LastIndex() != n.LastIndex() {
		t.Fatalf("ログが復元されない: %d != %d", n2.LastIndex(), n.LastIndex())
	}
	// 揮発状態 (commitIndex) は 0 に戻る (D-014)
	if n2.CommitIndex() != 0 {
		t.Fatalf("commitIndex が揮発でない: %d", n2.CommitIndex())
	}
	// 再選出後、エントリは再 commit・再 apply される
	tickUntilCampaign(n2)
	if n2.State() != StateLeader {
		t.Fatalf("再起動後に再選出されない: %s", n2.State())
	}
	if n2.CommitIndex() != n2.LastIndex() {
		t.Fatalf("再起動後に commit が回復しない: %d", n2.CommitIndex())
	}
}

func TestRestartWithSnapshot(t *testing.T) {
	snap := &Snapshot{Index: 10, Term: 3, Config: Config{Voters: []uint64{1, 2, 3}}, Data: []byte("state")}
	r := Restore{
		HardState: HardState{Term: 5, VotedFor: 2},
		Snapshot:  snap,
		Entries:   []Entry{{Index: 11, Term: 4, Type: EntryNormal, Data: []byte("y")}},
	}
	n := RestartNode(testParams(1, []uint64{1, 2, 3}), r)
	if n.SnapIndex() != 10 || n.SnapTerm() != 3 {
		t.Fatalf("snapshot 位置: %d/%d", n.SnapIndex(), n.SnapTerm())
	}
	if n.CommitIndex() != 10 || n.LastApplied() != 10 {
		t.Fatalf("commit/applied が snapshot 位置から始まらない: %d/%d", n.CommitIndex(), n.LastApplied())
	}
	if n.LastIndex() != 11 {
		t.Fatalf("snapshot 以降のログ: %d", n.LastIndex())
	}
	if tm, ok := n.TermAt(10); !ok || tm != 3 {
		t.Fatalf("スナップショット地点の term が引けない")
	}
}
