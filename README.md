# raft-sim — Raft 実装 + 決定論的障害注入検証

Raft 合意アルゴリズム (Ongaro & Ousterhout 2014, および Ongaro の博士論文) を
**論文のみを一次資料として** Go で完全実装し、決定論的シミュレーションによる
障害注入で正しさを機械的に検証するプロジェクト。

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

検証は `make verify` 一発で走る (SPEC.md §4.4 の Definition of Done をすべて機械検査):

```
make verify
├── go vet
├── check-imports   # Raft コアに time / math/rand / sync がないことの機械検査
├── check-deps      # go.mod の外部依存ゼロ (標準ライブラリのみ)
├── coverage        # 全テスト実行 + Raft コアのカバレッジ >= 90% を機械検査
│   ├── Raft コア単体テスト (選出・複製・スナップショット・Figure 8 忠実再現)
│   ├── 線形化チェッカー自己テスト (既知の合法/違法履歴ベクタ)
│   └── シミュレーション受け入れテスト (M1〜M6 の全シナリオスイート)
└── soak            # 10,000 シードソーク (違反ゼロを確認、失敗時はシードを表示)
```

失敗シードの再現:

```
go run ./cmd/raftsim replay -seed 12345 -o trace.txt   # 決定的に再現しトレース出力
go run ./cmd/raftsim run    -seed 12345                # 結果サマリのみ
go run ./cmd/raftsim soak   -seeds 10000               # ソーク単体
```

## 実装スコープ

| 機能 | 実装 |
|---|---|
| リーダー選出 | ランダム化タイムアウト + **Pre-Vote** (博士論文 §9.6) + leader stickiness (§4.2.3) + CheckQuorum (§6.2) |
| ログ複製 | AppendEntries、一貫性チェック、conflict index による高速バックアップ (§5.3) |
| 永続化 | currentTerm / votedFor / log[] を耐久ストレージへ。**fsync 境界をモデル化** |
| スナップショット | ログ圧縮 + InstallSnapshot RPC、遅延フォロワーへの転送 (§7) |
| メンバーシップ変更 | 博士論文 §4 の **single-server change** + 新サーバーの catch-up フェーズ |
| リーダーシップ移譲 | TimeoutNow (博士論文 §3.10)。リーダー自身の除去に必要 |
| クライアントセッション | clientID + seqNo による **exactly-once** (重複リクエスト排除、博士論文 §6.3) |
| KV ストア | Get / Put / CAS。線形化 Read もログ経由 (ReadIndex は不実装、SPEC で任意) |

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

- **入力**: `Tick` (仮想タイマー1目盛)、`Receive` (メッセージ)、`Propose`、
  `ProposeConfChange`、`TransferLeadership`、`CreateSnapshot`
- **出力**: 送信すべき `Messages`、fsync すべき `Persist`、apply すべき `Applied`、
  スナップショット復元指示 `ApplySnapshot`
- goroutine・wall clock・グローバル乱数を一切持たない。乱数 (選出タイムアウトの
  ランダム化) は `RNG` インターフェースでシミュレータから注入する。
  `raft` パッケージが `time` / `math/rand` / `sync` を import しないことは
  CI (`make check-imports`) が構文解析で機械検査する。

同じ状態 + 同じ入力 (+ 同じ乱数系列) → 同じ出力。これによりシミュレータが
時間と通信を完全に支配でき、**1 シード = 1 宇宙**の再現性が成立する。

### 決定論の作り方 (シミュレータ)

- **仮想時間**: イベントキュー駆動 (単位はマイクロ秒)。`(時刻, 挿入順序 seq)` の
  複合キーで全順序化し、同時刻イベントのタイブレークも決定論化
- **乱数**: すべて `rand.New(rand.NewSource(seed))` から派生。ノードの RNG も
  マスター RNG から系列を分岐
- **並行性**: シミュレーションループは単一 goroutine。Raft コアに goroutine が
  ないため競合が存在しない
- **map 反復の排除**: Go の map 反復順はランダムなので、投票集計・quorum 判定・
  送信順序など挙動に影響する箇所はすべてソート済みスライスを反復する
- **シリアライズ**: gob/JSON は map の順序で非決定になるため、コマンド・
  スナップショットは自作の長さ接頭辞つきバイナリコーデック (キーをソート)

決定論自体もテストされる: 同一シード 2 回実行のイベントトレースの**バイト一致**を、
障害注入・クライアント・スナップショット・構成変更をすべて有効にした構成で
検査する回帰テストが各マイルストーンにある。

### 障害モデル (SPEC §3.3)

| 障害 | 実装 |
|---|---|
| メッセージ喪失 / 重複 / 遅延 | 送信時に確率判定。遅延は指数分布 (順序入替はこれで自然発生) |
| ネットワーク分断 | ランダム 2/3 分割 → 指数分布時間後に回復。送信時点で判定 (在空メッセージは配送 = 遅延した古いメッセージとして到着) |
| ノードクラッシュ | 揮発状態を全喪失。fsync 済み永続状態のみで再起動 |
| **fsync 境界クラッシュ** | 1 つの `Persist` = 1 fsync 単位。fsync 前クラッシュではその Persist と**同一 Step が生成したメッセージがすべて消滅**する (「永続化してから応答する」という Raft の前提を正確にモデル化) |
| クロックスキュー | ノードごとに tick 間隔へ ±20% の固定倍率 (再起動で引き直し) |
| 構成変更チャーン | ランダムな AddServer / RemoveServer (リーダー自身の除去も含む) + リーダーシップ移譲 |

## 検証 (このプロジェクトの本体)

### 安全性不変条件 (毎イベント後に検査)

論文 Figure 3 の 5 性質 + α を、全量検査と等価な**増分検査**で毎イベント実行する:

1. **Election Safety** — term → リーダーの大域マップで重複を検出
2. **Leader Append-Only** — 連続在任中の lastIndex 単調性 + 旧末尾エントリの term 不変
3. **Log Matching** — `(index, term) → (内容ハッシュ, 直前エントリの term)` の大域連鎖マップ。
   同一 `(index, term)` の内容と親が全ログで一致すれば、帰納的に prefix 全体の一致が従う。
   検査コストは新規エントリ数に対して O(1)
4. **Leader Completeness** — commit 観測済みエントリを大域記録し、それ以後に観測された
   リーダーのログに存在することを検査。**commit したリーダーの term の上界** (観測ノードの
   currentTerm、観測のたび min で引き締め) より高い term のリーダーのみ検査対象にする。
   これは分断中の stale リーダー (自分より後の term の commit を持たなくて正当) への
   偽陽性を避けるために必要 — 詳細は NOTES.md
5. **State Machine Safety** — apply された `(index → 内容)` の大域一致 + apply 順序の連続性

加えて: term / commitIndex の単調性 (クラッシュ再起動の揮発リセットは許容)、
構成の正当性 (非空・昇順・重複なし、リーダー在任中の single-server 変更)、
exactly-once (同一 clientID+seqNo が異なるログ index で二度状態を変えたら違反)。

### 線形化可能性チェッカー (自作、checker/linear.go)

- クライアント操作の invoke/response 実時間区間と結果を記録
- **Wing & Gong アルゴリズム**を (linearized 集合のビットマスク, レジスタ状態) の
  メモ化つき DFS として実装
- **P-compositionality**: 各操作は単一キーにのみ触れるため、キーごとの射影が
  すべて線形化可能 ⟺ 全体が線形化可能。キー単位に分割して検査する
- 応答を受けていない操作 (クラッシュ等で結果不明) は「効いた世界」と
  「効かなかった世界」の両方を探索する
- チェッカー自体の正当性は既知の合法/違法履歴のテストベクタで単体検証
  (stale read、読み戻り、二重 CAS 成功、未完了書き込みの可視性など)

### 狙い撃ちシナリオスイート (ランダムに加えて)

- **論文 Figure 8 の忠実な再現** (`raft/figure8_test.go`): 5 ノードの全メッセージを
  手動配送し、「旧 term のエントリは過半数複製だけでは commit されない」ことと、
  その対 (自 term エントリの複製による間接 commit 後は巻き戻しリーダーが当選不能) を検査
- 分断されたリーダーがクライアント応答できないこと (stale read 防止)
- スナップショット転送中のクラッシュ (転送先の反復クラッシュ + 転送元クラッシュ)
- メンバーシップ変更中のリーダークラッシュ
- リーダー自身をクラスタから除去する変更
- リーダーシップ移譲 (移譲中の提案拒否を含む)
- 5 ノード中 2 ノード永久停止での可用性継続 / 3 ノード停止での安全な停止
  (過半数喪失後は 1 操作も完了しないことを履歴で確認)
- 全 Persist が fsync 前に失われる極端ケース (リーダーが決して誕生しないこと)
- fsync 境界クラッシュ + 全クラスタ再起動での commit 済みデータ生存

## 設計上の学び

実装中の教訓 (ハマったバグ、論文の読み違え、チェッカー設計の落とし穴) は
[NOTES.md](NOTES.md) に、仕様の曖昧点への判断は [DECISIONS.md](DECISIONS.md) に
記録している。特に:

- **チェッカーも分散システムの部分観測性を尊重する必要がある**。
  「現在の全リーダーは全 commit 済みエントリを持つ」は Leader Completeness の
  過強化で、stale リーダーへの偽陽性になる (NOTES.md M1)
- **リーダーシップ移譲は TimeoutNow を送った後も提案を拒否し続ける**。
  移譲対象が「常に一歩遅れ」て当選できなくなる (NOTES.md M6)
- **catch-up 中のフォロワーの構成は一気に複数段進む**。single-server 変更の
  規律はリーダー側でのみ観測可能 (NOTES.md M6)
- **スライスのエイリアシング**はシミュレーションの静かな破壊者。送信メッセージの
  エントリは必ずコピー (NOTES.md M1)

## 性能

正しさが唯一の評価軸だが、参考値としてソーク実行時のスループットを測定・表示する
(`raftsim soak` の出力: 処理イベント/秒、完了クライアント操作/秒)。上記の実測では
仮想時間 5 秒 × 10,000 宇宙 (約 4,700 万イベント) を実時間 1 分弱で検査している。

## 一次資料

- Diego Ongaro and John Ousterhout, "In Search of an Understandable Consensus Algorithm (Extended Version)", 2014
- Diego Ongaro, "Consensus: Bridging Theory and Practice", Ph.D. dissertation, Stanford University, 2014

外部 Raft 実装 (etcd/raft 等) のコードは参照していない。依存は Go 標準ライブラリのみ
(`go.mod` の require は空、`make check-deps` で機械検査)。
