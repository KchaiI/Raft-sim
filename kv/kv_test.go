package kv

import (
	"bytes"
	"testing"
)

func TestApplyBasicOps(t *testing.T) {
	s := NewStore()
	res, fresh := s.Apply(Command{ClientID: 1, Seq: 1, Op: OpPut, Key: "k", Value: "v1"})
	if !res.OK || !fresh {
		t.Fatalf("Put: %+v fresh=%v", res, fresh)
	}
	res, _ = s.Apply(Command{ClientID: 1, Seq: 2, Op: OpGet, Key: "k"})
	if !res.OK || res.Value != "v1" {
		t.Fatalf("Get: %+v", res)
	}
	res, _ = s.Apply(Command{ClientID: 1, Seq: 3, Op: OpGet, Key: "nokey"})
	if res.OK {
		t.Fatalf("存在しないキーの Get が OK: %+v", res)
	}
	res, _ = s.Apply(Command{ClientID: 1, Seq: 4, Op: OpCAS, Key: "k", Expect: "v1", Value: "v2"})
	if !res.OK {
		t.Fatalf("CAS 成功すべき: %+v", res)
	}
	res, _ = s.Apply(Command{ClientID: 1, Seq: 5, Op: OpCAS, Key: "k", Expect: "v1", Value: "v3"})
	if res.OK || res.Value != "v2" {
		t.Fatalf("CAS 失敗 + 現在値観測すべき: %+v", res)
	}
}

// exactly-once: 同一 (clientID, seq) の再適用は状態を変えずキャッシュ応答を返す。
func TestSessionDeduplication(t *testing.T) {
	s := NewStore()
	s.Apply(Command{ClientID: 1, Seq: 1, Op: OpPut, Key: "k", Value: "v0"})
	cmd := Command{ClientID: 1, Seq: 2, Op: OpCAS, Key: "k", Expect: "v0", Value: "v1"}
	res1, fresh1 := s.Apply(cmd)
	if !res1.OK || !fresh1 {
		t.Fatalf("初回 CAS: %+v", res1)
	}
	// 再送された同一コマンド (ログに 2 回 commit された状況)
	res2, fresh2 := s.Apply(cmd)
	if fresh2 {
		t.Fatal("重複が fresh 扱いされた (exactly-once 違反)")
	}
	if res2 != res1 {
		t.Fatalf("キャッシュ応答が異なる: %+v vs %+v", res2, res1)
	}
	if v, _ := s.Get("k"); v != "v1" {
		t.Fatalf("二重適用された: %q", v)
	}
	// 別クライアントは独立
	res3, fresh3 := s.Apply(Command{ClientID: 2, Seq: 1, Op: OpCAS, Key: "k", Expect: "v0", Value: "x"})
	if !fresh3 || res3.OK {
		t.Fatalf("別クライアントの CAS: %+v fresh=%v", res3, fresh3)
	}
}

func TestCommandCodecRoundTrip(t *testing.T) {
	cmds := []Command{
		{ClientID: 1, Seq: 9, Op: OpGet, Key: "k"},
		{ClientID: 1<<63 + 5, Seq: 1 << 40, Op: OpPut, Key: "キー", Value: "値"},
		{ClientID: 7, Seq: 3, Op: OpCAS, Key: "", Value: "new", Expect: "old"},
	}
	for _, c := range cmds {
		got := DecodeCommand(c.Encode())
		if got != c {
			t.Fatalf("roundtrip: %+v != %+v", got, c)
		}
	}
}

func TestSnapshotRoundTripAndDeterminism(t *testing.T) {
	s := NewStore()
	for i := 0; i < 50; i++ {
		s.Apply(Command{ClientID: uint64(i%7 + 1), Seq: uint64(i/7 + 1), Op: OpPut,
			Key: string(rune('a' + i%13)), Value: string(rune('A' + i%26))})
	}
	b1 := s.Snapshot()
	b2 := s.Snapshot()
	if !bytes.Equal(b1, b2) {
		t.Fatal("スナップショットのエンコードが非決定 (D-005 違反)")
	}
	r := RestoreStore(b1)
	if !bytes.Equal(r.Snapshot(), b1) {
		t.Fatal("復元後のスナップショットが一致しない")
	}
	// セッションも復元される (重複排除が引き継がれる)
	res, fresh := r.Apply(Command{ClientID: 1, Seq: s.sessions[1].lastSeq, Op: OpPut, Key: "zz", Value: "dup"})
	if fresh {
		t.Fatalf("復元後に重複排除が効かない: %+v", res)
	}
}
