# Phase 3: 長期記憶（Mental Models）実装完了

## 実装済みファイル

| ファイル | 行数 | 役割 |
|---|---|---|
| `postgresql/init/004_long_term.sql` | ~60行 | mental_models + chunks テーブル、HNSW/GIN インデックス |
| `agentcore/memory/visibility.py` | ~110行 | タグベース可視性制御（4モード: any/all/any_strict/all_strict）|
| `agentcore/memory/mental_model.py` | ~320行 | Mental Model CRUD（create/get/list/search/update/delete/refresh）|
| `agentcore/memory/disposition.py` | ~110行 | 性格特性（skepticism/literalism/empathy）読み込み + プロンプト生成 |
| `agentcore/memory/directive.py` | ~80行 | ディレクティブ読み込み + プロンプトセクション/リマインダー生成 |
| `agentcore/memory/reflect.py` | ~570行 | Reflect パイプライン（Bedrock Converse tool_use エージェントループ）|

## 修正済みファイル

| ファイル | 変更内容 |
|---|---|
| `agentcore/memory/consolidation.py` | Mental Model 自動リフレッシュ（_trigger_mental_model_refresh）追加 |
| `agentcore/memory/engine.py` | reflect() メソッド、セマフォ（search=32, put=5）追加 |
| `agentcore/core.py` | reflect_on ツール、システムプロンプト更新 |
| `docs/設計/記憶システム実装タスク.md` | Phase 3 全タスクに ✅ 追加 |
| `CLAUDE.md` | Phase 3 進捗テーブル更新 |

## 設計上の重要な決定

1. **Reflect は独自エージェントループ**: Strands Agent ではなく Bedrock Converse API の tool_use を直接利用。
   理由: 証拠ガードレール、ID検証、Disposition/Directive注入など低レベル制御が必要。
2. **Reflect モデル**: Sonnet クラス（環境変数 REFLECT_MODEL_ID）。深い推論に Haiku では不足。
3. **タグセキュリティ**: Mental Model リフレッシュ時、タグ付きモデルは tags_match="all_strict" で情報漏洩防止。
4. **リフレッシュ上限**: MAX_REFRESH_PER_CONSOLIDATION = 3。Consolidation ブロッキング防止。
5. **Disposition は banks.disposition JSONB**: 別テーブルではなく既存カラムを活用。
6. **Directives は banks.directives TEXT[]**: Hindsight は別テーブルだが、設計書に従い既存カラムを活用。
7. **Reflect タイムアウト**: 300秒（最大10イテレーション対応）。core.py で _REFLECT_TIMEOUT として設定。

## Reflect パイプラインの5つのツール

1. **search_mental_models**: Mental Model セマンティック検索（max_results クランプ: 20）
2. **search_observations**: Observation セマンティック検索（max_results クランプ: 50）
3. **recall**: Raw Facts セマンティック検索（observation 除外、max_results クランプ: 100）
4. **expand**: memory_unit の完全テキスト + chunks 取得（最大10件、chunks LIMIT 100）
5. **done**: 回答確定（証拠ガードレール + ID 検証 + ディレクティブ遵守確認）

## 検証結果（3.4）

テストスクリプト: `agentcore/test_script/test_phase3_verification.py` — **8/8 PASS**

1. DB マイグレーション（テーブル・インデックス・トリガー）
2. タグベース可視性制御（SQL句4モード + Pythonフィルタ4モード + SQL injection防止）
3. Disposition プロンプト生成（ニュートラル + 6パターン + 全軸同時）
4. ディレクティブ生成（セクション + リマインダー + 空ケース）
5. Mental Model CRUD（create/get/list/search/update/refreshable/delete、Bedrock Embedding実行）
6. Reflect ツール定義（5ツール + ディレクティブ有無切替）
7. Reflect done ツール（証拠ガードレール拒否 + 有効ID通過 + ハルシネーションID除去）
8. システムプロンプト構築（Disposition + Directives統合 + 最小構成）

## レビュー指摘と対応

- CRITICAL: import uuid をトップレベルに移動 → 修正済み
- HIGH: max_results クランプ追加 → 修正済み
- HIGH: visibility.py の column パラメータにバリデーション追加 → 修正済み
- HIGH: disposition.py の _clamp に型安全性追加 → 修正済み
- WARNING: consolidation.py のリフレッシュ上限追加 → 修正済み
- MEDIUM: chunks クエリに LIMIT 100 追加 → 修正済み

## DB スキーマ

### mental_models テーブル
- id, bank_id, name, description, content, source_query
- embedding vector(1024), entity_id, source_observation_ids UUID[]
- tags TEXT[], max_tokens, trigger JSONB
- last_refreshed_at, created_at, updated_at

### chunks テーブル
- id, bank_id, memory_unit_id, chunk_index, text
- embedding vector(1024), metadata JSONB, created_at
