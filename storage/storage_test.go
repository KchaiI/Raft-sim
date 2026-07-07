package storage

import (
	"testing"

	"raftsim/raft"
)

func ent(i, t uint64, data string) raft.Entry {
	return raft.Entry{Index: i, Term: t, Type: raft.EntryNormal, Data: []byte(data)}
}

func TestApplyAndLoadRoundTrip(t *testing.T) {
	d := New()
	d.Apply(&raft.Persist{
		HardState: &raft.HardState{Term: 3, VotedFor: 2},
		Entries:   []raft.Entry{ent(1, 1, "a"), ent(2, 3, "b")},
	})
	r := d.Load()
	if r.HardState.Term != 3 || r.HardState.VotedFor != 2 {
		t.Fatalf("HardState: %+v", r.HardState)
	}
	if len(r.Entries) != 2 || r.Entries[1].Index != 2 {
		t.Fatalf("Entries: %+v", r.Entries)
	}
}

func TestApplyTruncatesConflicts(t *testing.T) {
	d := New()
	d.Apply(&raft.Persist{Entries: []raft.Entry{ent(1, 1, "a"), ent(2, 1, "b"), ent(3, 1, "c")}})
	// index 2 から上書き
	d.Apply(&raft.Persist{Entries: []raft.Entry{ent(2, 2, "B")}})
	r := d.Load()
	if len(r.Entries) != 2 {
		t.Fatalf("truncate されない: %+v", r.Entries)
	}
	if r.Entries[1].Term != 2 || string(r.Entries[1].Data) != "B" {
		t.Fatalf("上書きが反映されない: %+v", r.Entries[1])
	}
}

func TestApplySnapshotCompactsLog(t *testing.T) {
	d := New()
	d.Apply(&raft.Persist{Entries: []raft.Entry{ent(1, 1, "a"), ent(2, 1, "b"), ent(3, 1, "c")}})
	snap := &raft.Snapshot{Index: 2, Term: 1, Config: raft.Config{Voters: []uint64{1, 2, 3}}}
	d.Apply(&raft.Persist{Snapshot: snap})
	r := d.Load()
	if r.Snapshot == nil || r.Snapshot.Index != 2 {
		t.Fatalf("snapshot がない: %+v", r.Snapshot)
	}
	if len(r.Entries) != 1 || r.Entries[0].Index != 3 {
		t.Fatalf("圧縮後のログが不正: %+v", r.Entries)
	}
}

func TestApplySnapshotReplaceLog(t *testing.T) {
	d := New()
	d.Apply(&raft.Persist{Entries: []raft.Entry{ent(1, 1, "a"), ent(2, 1, "b")}})
	snap := &raft.Snapshot{Index: 5, Term: 3, Config: raft.Config{Voters: []uint64{1}}}
	d.Apply(&raft.Persist{Snapshot: snap, ReplaceLog: true})
	r := d.Load()
	if len(r.Entries) != 0 {
		t.Fatalf("ReplaceLog でログが残った: %+v", r.Entries)
	}
	// スナップショット直後からの追記
	d.Apply(&raft.Persist{Entries: []raft.Entry{ent(6, 3, "x")}})
	if r := d.Load(); len(r.Entries) != 1 || r.Entries[0].Index != 6 {
		t.Fatalf("スナップショット後の追記が不正: %+v", r.Entries)
	}
}

// Load が深いコピーを返し、後の Apply の影響を受けないこと。
func TestLoadIsDeepCopy(t *testing.T) {
	d := New()
	d.Apply(&raft.Persist{Entries: []raft.Entry{ent(1, 1, "a")}})
	r1 := d.Load()
	d.Apply(&raft.Persist{Entries: []raft.Entry{ent(1, 2, "MUT")}})
	if r1.Entries[0].Term != 1 || string(r1.Entries[0].Data) != "a" {
		t.Fatalf("Load の結果が後続 Apply で破壊された: %+v", r1.Entries[0])
	}
}

func TestApplyNilIsNoop(t *testing.T) {
	d := New()
	d.Apply(nil)
	if r := d.Load(); r.HardState.Term != 0 || len(r.Entries) != 0 {
		t.Fatalf("nil Apply が状態を変えた")
	}
}
