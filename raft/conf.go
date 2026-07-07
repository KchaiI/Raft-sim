package raft

import "encoding/binary"

// confRecord はある index 時点で有効になった構成。
type confRecord struct {
	index uint64
	cfg   Config
}

// encodeConfig は構成を決定論的にエンコードする (DECISIONS.md D-005)。
func encodeConfig(c Config) []byte {
	buf := make([]byte, 4+8*len(c.Voters))
	binary.BigEndian.PutUint32(buf, uint32(len(c.Voters)))
	for i, v := range c.Voters {
		binary.BigEndian.PutUint64(buf[4+8*i:], v)
	}
	return buf
}

func decodeConfig(b []byte) Config {
	n := binary.BigEndian.Uint32(b)
	c := Config{Voters: make([]uint64, n)}
	for i := range c.Voters {
		c.Voters[i] = binary.BigEndian.Uint64(b[4+8*i:])
	}
	return c
}

func (n *Node) initConfHistory(index uint64, base Config) {
	n.confHistory = []confRecord{{index: index, cfg: base.Clone()}}
	n.config = base.Clone()
	n.configIndex = index
}

// setConfig は構成エントリの追加 (append / 受信複製) を反映する。
// 構成はエントリが append された時点で直ちに有効になる (博士論文 §4.1)。
func (n *Node) setConfig(index uint64, cfg Config) {
	n.config = cfg.Clone()
	n.configIndex = index
	n.confHistory = append(n.confHistory, confRecord{index: index, cfg: cfg.Clone()})
	if n.state == StateLeader {
		for _, v := range n.config.Voters {
			if v != n.id && n.prs[v] == nil {
				n.prs[v] = &progress{next: n.log.lastIndex() + 1}
			}
		}
	}
}

// truncateConfigsFrom はログ truncate に伴い構成を巻き戻す。
// フォロワーが未 commit の構成エントリごと suffix を削るケースで必須。
func (n *Node) truncateConfigsFrom(index uint64) {
	for len(n.confHistory) > 1 && n.confHistory[len(n.confHistory)-1].index >= index {
		n.confHistory = n.confHistory[:len(n.confHistory)-1]
	}
	last := n.confHistory[len(n.confHistory)-1]
	n.config = last.cfg.Clone()
	n.configIndex = last.index
}

// configAt は index 時点で有効だった構成 (スナップショット作成用)。
func (n *Node) configAt(index uint64) Config {
	for i := len(n.confHistory) - 1; i >= 0; i-- {
		if n.confHistory[i].index <= index {
			return n.confHistory[i].cfg.Clone()
		}
	}
	return n.confHistory[0].cfg.Clone()
}

// pruneConfHistory はスナップショット地点以前の履歴を畳む。
func (n *Node) pruneConfHistory(index uint64) {
	for len(n.confHistory) > 1 && n.confHistory[1].index <= index {
		n.confHistory = n.confHistory[1:]
	}
}
