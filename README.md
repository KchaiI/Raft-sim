# raft-sim — Raft 実装 + 決定論的障害注入検証

Raft 合意アルゴリズム (Ongaro & Ousterhout, 2014 / Ongaro 博士論文) を**論文のみを
一次資料として** Go で完全実装し、決定論的シミュレーションによる障害注入で
正しさを機械的に検証するプロジェクト。外部依存はゼロ (Go 標準ライブラリのみ)。

> **10,000 シード × ランダム障害シナリオ (3/5/7 ノード混合・全障害有効) を実行し、
> 安全性不変条件・線形化可能性の違反ゼロ。任意の失敗はシード指定で 100% 再現可能。**

実測 (`make verify`, Apple Silicon):

```
ソーク完了: 10000 シード (3/5/7 ノード混合, 全障害有効), 55.4s
  完了クライアント操作: 266106 (4803 ops/s 処理)
  処理イベント: 47117258 (850516 events/s)
  安全性不変条件・線形化可能性の違反: 0
coverage OK: raft コア 92.8% (>= 90%)
```

## クイックスタート

```sh
make verify                                  # 全検証 (下記) を一発実行 (~7分)

go run ./cmd/raftsim run    -seed 42         # 1 シードだけ実行して結果表示
go run ./cmd/raftsim soak   -seeds 10000     # ソーク単体
go run ./cmd/raftsim replay -seed 42 -o t.txt  # 失敗シードを決定的に再現しトレース出力
```

`make verify` は SPEC.md §4.4 の Definition of Done をすべて機械検査する:

| ステップ | 内容 |
|---|---|
| `vet` | `go vet ./...` |
| `check-imports` | Raft コアが `time` / `math/rand` / `sync` を import しないことを構文解析で検査 |
| `check-deps` | `go.mod` の外部依存ゼロ (`go list -m all` が自モジュールのみ) |
| `coverage` | 全テスト実行 + Raft コアのカバレッジ ≥ 90% を検査 |
| `soak` | 10,000 シードソーク。違反があれば該当シードを表示して失敗 |

テストだけ回す場合: `make test` (単体 + 受け入れ + シナリオスイート、~5分)。

## 実装スコープ

| 機能 | 実装内容 |
|---|---|
| リーダー選出 | ランダム化タイムアウト + **Pre-Vote** (博士論文 §9.6) + leader stickiness (§4.2.3) + CheckQuorum (§6.2) |
| ログ複製 | AppendEntries、一貫性チェック、conflict index による高速バックアップ (§5.3) |
| 永続化 | currentTerm / votedFor / log[] を耐久ストレージへ。**fsync 境界をモデル化** |
| スナップショット | ログ圧縮 + InstallSnapshot RPC、遅延フォロワーへの転送 (§7) |
| メンバーシップ変更 | 博士論文 §4 の **single-server change** + 新サーバーの catch-up フェーズ |
| リーダーシップ移譲 | TimeoutNow (博士論文 §3.10)。リーダー自身の除去に必要 |
| クライアントセッション | clientID + seqNo による **exactly-once** (重複リクエスト排除、博士論文 §6.3) |
| KV ストア | Get / Put / CAS。線形化 Read もログ経由 (ReadIndex は SPEC で任意のため不実装) |

## アーキテクチャ

```
raft/        Raft コア (純粋ステートマシン。I/O なし・時計なし・乱数は注入)
storage/     シミュレート耐久ストレージ (1 Apply = 1 fsync 単位)
kv/          線形化可能 KV ステートマシン + セッション (決定論的コーデック)
sim/         決定論的シミュレータ (仮想時間イベントキュー、障害注入、クライアント)
checker/     安全性不変条件チェッカー + 線形化可能性チェッカー (WGL 自作)
cmd/raftsim  CLI (run / soak / replay)
tools/       importcheck (禁止 import の機械検査)
```

### 核となる設計判断: Raft コアは純粋な決定論的ステートマシン

```go
func (n *Node) Step(in Input) Output
```

- **入力**: `Tick` (仮想タイマー 1 目盛)、`Receive` (メッセージ受信)、`Propose`、
  `ProposeConfChange`、`TransferLeadership`、`CreateSnapshot`
- **出力**: 送信すべき `Messages`、fsync すべき `Persist`、apply すべき `Applied`、
  スナップショット復元指示 `ApplySnapshot`

goroutine・wall clock・グローバル乱数を一切持たず、乱数 (選出タイムアウトの
ランダム化) は `RNG` インターフェースで注入する。同じ状態 + 同じ入力 (+ 同じ乱数系列)
→ 同じ出力。これによりシミュレータが時間と通信を完全に支配でき、
**1 シード = 1 宇宙**の再現性が成立する。

呼び出し側の契約は 1 つ: **`Persist` を fsync してから `Messages` を送る**。
シミュレータはこの順序を強制し、fsync 前クラッシュでは同一 Step のメッセージごと
消滅させる (Raft の「永続化してから応答する」前提の正確なモデル化)。

### 決定論の作り方 (シミュレータ)

- **仮想時間**: イベントキュー駆動 (単位 µs)。`(時刻, 挿入順序)` の複合キーで
  全順序化し、同時刻イベントのタイブレークも決定論化
- **乱数**: すべて `rand.New(rand.NewSource(seed))` から派生
- **並行性**: シミュレーションループは単一 goroutine。Raft コアに goroutine がない
  ため競合が存在しない
- **map 反復の排除**: 投票集計・quorum 判定・送信順序など挙動に影響する箇所は
  すべてソート済みスライスを反復 (Go の map 反復順はランダム)
- **シリアライズ**: gob/JSON は map 順序で非決定になるため、自作の決定論的
  バイナリコーデックを使用

決定論自体もテストされる: 同一シード 2 回実行のイベントトレースの**バイト一致**を、
障害・クライアント・スナップショット・構成変更をすべて有効にして検査する回帰テストが
各マイルストーンにある。

### 障害モデル (SPEC §3.3)

| 障害 | 実装 |
|---|---|
| メッセージ喪失 / 重複 | 送信時に確率判定 (ソークでは 10% / 5%) |
| メッセージ遅延 | 指数分布 (最大 400ms ≒ 数 election timeout)。順序入替はこれで自然発生 |
| ネットワーク分断 | ランダム 2/3 分割 → 指数分布時間後に回復。在空メッセージは配送される (遅延した古いメッセージの到着として現実にも起こる) |
| ノードクラッシュ | 揮発状態を全喪失。fsync 済み永続状態のみで再起動 |
| **fsync 境界クラッシュ** | 1 `Persist` = 1 fsync 単位。fsync 前クラッシュでは Persist と同一 Step のメッセージがすべて消滅 |
| クロックスキュー | ノードごとに tick 間隔へ ±20% の倍率 (再起動で引き直し) |
| 構成変更チャーン | ランダム AddServer / RemoveServer (リーダー自身の除去含む) + リーダーシップ移譲 |

## 検証 (このプロジェクトの本体)

### 安全性不変条件 — 毎イベント後に検査

論文 Figure 3 の 5 性質 + α を、全量検査と等価な**増分検査**で毎イベント実行する
([checker/invariants.go](checker/invariants.go)):

1. **Election Safety** — term → リーダーの大域マップで重複検出
2. **Leader Append-Only** — 連続在任中の lastIndex 単調性 + 旧末尾エントリの term 不変
3. **Log Matching** — `(index, term) → (内容ハッシュ, 直前エントリの term)` の大域連鎖
   マップ。同一 `(index, term)` の内容と親が全ログで一致すれば、帰納的に prefix 全体の
   一致が従う。検査コストは新規エントリ数に対して O(1)
4. **Leader Completeness** — commit 観測済みエントリが、それより高い term のリーダーの
   ログに存在することを検査。「commit したリーダーの term の健全な上界」を観測ノードの
   currentTerm から求める (分断中の stale リーダーへの偽陽性を避けるために必須)
5. **State Machine Safety** — apply された `(index → 内容)` の大域一致 + apply 順序の連続性

加えて: term / commitIndex の単調性 (再起動時の揮発リセットは許容)、構成の正当性
(非空・昇順、リーダー在任中の single-server 変更)、**exactly-once**
(同一 clientID+seqNo が異なるログ index で二度状態を変えたら違反)。

### 線形化可能性チェッカー — 自作 WGL

[checker/linear.go](checker/linear.go):

- クライアント操作の invoke/response 実時間区間と結果を記録し、実行後に検査
- **Wing & Gong アルゴリズム**を (linearized 集合ビットマスク, レジスタ状態) の
  メモ化つき DFS として実装
- **P-compositionality**: 各操作は単一キーにのみ触れるため、キーごとの射影検査に分割
- 応答のない操作 (クラッシュ等で結果不明) は「効いた/効かなかった」両方の世界を探索
- チェッカー自体の正当性は既知の合法 12 種 / 違法 9 種の履歴ベクタで単体検証
  (stale read、読み戻り、二重 CAS 成功、未完了書き込みの可視性など)

### 狙い撃ちシナリオスイート (ランダムソークに加えて)

| シナリオ | テスト |
|---|---|
| **論文 Figure 8 の忠実再現** (旧 term エントリの間接コミット問題) | [raft/figure8_test.go](raft/figure8_test.go) |
| 分断されたリーダーがクライアント応答できない (stale read 防止) | [sim/kv_test.go](sim/kv_test.go) |
| スナップショット転送中のクラッシュ (転送先の反復クラッシュ + 転送元) | [sim/snapshot_test.go](sim/snapshot_test.go) |
| メンバーシップ変更中のリーダークラッシュ | [sim/membership_test.go](sim/membership_test.go) |
| リーダー自身をクラスタから除去 / リーダーシップ移譲 | [sim/membership_test.go](sim/membership_test.go) |
| 5 ノード中 2 ノード永久停止で可用性継続 / 3 ノード停止で安全に停止 | [sim/scenario_test.go](sim/scenario_test.go) |
| 全 Persist が fsync 前に失われる極端ケース (リーダーが誕生しない) | [sim/persistence_test.go](sim/persistence_test.go) |
| fsync 境界クラッシュ + 全クラスタ再起動で commit 済みデータ生存 | [sim/persistence_test.go](sim/persistence_test.go) |

## 開発の記録

M0 (シミュレータ骨格) → M7 (ソーク + 仕上げ) の 8 マイルストーンを、
「受け入れテストが通ること」を完了条件として 1 コミットずつ積んでいる
(`git log --oneline` 参照)。

- [DECISIONS.md](DECISIONS.md) — 仕様の曖昧点に対する 15 の判断とその理由
  (fsync 境界の意味論、クライアント通信の障害モデル、catch-up 完了判定など)
- [NOTES.md](NOTES.md) — 実装中の教訓。特に:
  - **チェッカーも分散システムの部分観測性を尊重する必要がある** (stale リーダーは
    後続 term の commit を持たなくて正当 — Leader Completeness の過強化は偽陽性になる)
  - **リーダーシップ移譲は TimeoutNow 送信後も提案を拒否し続ける** (さもないと移譲対象が
    常に一歩遅れて当選できない)
  - **catch-up 中のフォロワーの構成は 1 バッチで複数段進む** (single-server 変更の規律は
    リーダー側でのみ観測可能)
  - **スライスのエイリアシング**はシミュレーションの静かな破壊者 (送信エントリは必ずコピー)

## 性能

正しさが唯一の評価軸だが、参考値としてソーク実行時のスループットを測定・表示する。
上記の実測では仮想時間 5 秒 × 10,000 宇宙 (約 4,700 万イベント) を実時間 1 分弱で
検査している。

## 一次資料

- Diego Ongaro and John Ousterhout, "In Search of an Understandable Consensus Algorithm (Extended Version)", 2014
- Diego Ongaro, "Consensus: Bridging Theory and Practice", Ph.D. dissertation, Stanford University, 2014

外部 Raft 実装 (etcd/raft 等) のコードは参照していない。
