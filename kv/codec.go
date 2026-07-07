package kv

import (
	"encoding/binary"
	"fmt"
	"sort"
)

// 決定論的バイナリコーデック (DECISIONS.md D-005)。
// gob/json は map の順序で出力が非決定になるため使わない。

func appendStr(b []byte, s string) []byte {
	b = binary.BigEndian.AppendUint32(b, uint32(len(s)))
	return append(b, s...)
}

func readStr(b []byte) (string, []byte) {
	n := binary.BigEndian.Uint32(b)
	return string(b[4 : 4+n]), b[4+n:]
}

// Encode はコマンドをバイト列にする。
func (c Command) Encode() []byte {
	b := make([]byte, 0, 32+len(c.Key)+len(c.Value)+len(c.Expect))
	b = append(b, byte(c.Op))
	b = binary.BigEndian.AppendUint64(b, c.ClientID)
	b = binary.BigEndian.AppendUint64(b, c.Seq)
	b = appendStr(b, c.Key)
	b = appendStr(b, c.Value)
	b = appendStr(b, c.Expect)
	return b
}

// DecodeCommand は Encode の逆変換。
func DecodeCommand(b []byte) Command {
	c := Command{Op: OpType(b[0])}
	c.ClientID = binary.BigEndian.Uint64(b[1:])
	c.Seq = binary.BigEndian.Uint64(b[9:])
	rest := b[17:]
	c.Key, rest = readStr(rest)
	c.Value, rest = readStr(rest)
	c.Expect, _ = readStr(rest)
	return c
}

// Snapshot はストア全体 (データ + セッション) を決定論的にエンコードする。
func (s *Store) Snapshot() []byte {
	keys := make([]string, 0, len(s.data))
	for k := range s.data {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	clients := make([]uint64, 0, len(s.sessions))
	for c := range s.sessions {
		clients = append(clients, c)
	}
	sort.Slice(clients, func(i, j int) bool { return clients[i] < clients[j] })

	b := binary.BigEndian.AppendUint32(nil, uint32(len(keys)))
	for _, k := range keys {
		b = appendStr(b, k)
		b = appendStr(b, s.data[k])
	}
	b = binary.BigEndian.AppendUint32(b, uint32(len(clients)))
	for _, c := range clients {
		sess := s.sessions[c]
		b = binary.BigEndian.AppendUint64(b, c)
		b = binary.BigEndian.AppendUint64(b, sess.lastSeq)
		if sess.lastResult.OK {
			b = append(b, 1)
		} else {
			b = append(b, 0)
		}
		b = appendStr(b, sess.lastResult.Value)
	}
	return b
}

// RestoreStore はスナップショットからストアを復元する。
func RestoreStore(b []byte) *Store {
	s := NewStore()
	if len(b) == 0 {
		return s
	}
	nk := binary.BigEndian.Uint32(b)
	rest := b[4:]
	for i := uint32(0); i < nk; i++ {
		var k, v string
		k, rest = readStr(rest)
		v, rest = readStr(rest)
		s.data[k] = v
	}
	nc := binary.BigEndian.Uint32(rest)
	rest = rest[4:]
	for i := uint32(0); i < nc; i++ {
		id := binary.BigEndian.Uint64(rest)
		seq := binary.BigEndian.Uint64(rest[8:])
		ok := rest[16] == 1
		rest = rest[17:]
		var val string
		val, rest = readStr(rest)
		s.sessions[id] = &session{lastSeq: seq, lastResult: Result{Value: val, OK: ok}}
	}
	return s
}

// String はデバッグ表示。
func (c Command) String() string {
	return fmt.Sprintf("%s(c%d#%d %q v=%q e=%q)", c.Op, c.ClientID, c.Seq, c.Key, c.Value, c.Expect)
}
