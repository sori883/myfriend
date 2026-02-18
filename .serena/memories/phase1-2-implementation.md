# Phase 1.2 アプリケーション実装完了

## 実装済みファイル

| ファイル | 行数 | 役割 |
|---|---|---|
| `agentcore/memory/engine.py` | ~75行 | MemoryEngine コアクラス（retain/recall 公開IF） |
| `agentcore/memory/extraction.py` | ~195行 | LLMファクト抽出（Bedrock Converse, Fact dataclass） |
| `agentcore/memory/embedding.py` | ~90行 | Embedding生成（Titan Embed V2, 1024次元） |
| `agentcore/memory/entity.py` | ~135行 | エンティティ解決（pg_trgm similarity, 閾値0.6） |
| `agentcore/memory/retain.py` | ~210行 | Retainパイプライン（抽出→Embedding→重複検出→DB保存） |
| `agentcore/memory/recall.py` | ~200行 | Recallパイプライン（セマンティック+BM25→RRF融合） |
| `agentcore/memory/bedrock_client.py` | ~27行 | Bedrockクライアント共有シングルトン（スレッドセーフ） |
| `agentcore/memory/__init__.py` | ~3行 | MemoryEngineエクスポート |
| `agentcore/main.py` | ~155行 | Strands Agentツール統合（remember/recall_memories） |

## 設計上の重要な決定

1. **bank_id はクロージャで固定**: LLM がbank_idを変更できないようにinvoke内でtool定義
2. **async/sync ブリッジ**: ThreadPoolExecutor + new_event_loop パターン
3. **Bedrockクライアント共有**: bedrock_client.py でスレッドセーフなシングルトン
4. **重複チェック**: event→12時間バケット+cosine, conversation→cosineのみ
5. **BM25クエリ**: websearch_to_tsquery（ユーザークエリをそのまま受付可能）
6. **モデルID**: 環境変数で設定可能（EXTRACTION_MODEL_ID, EMBEDDING_MODEL_ID）

## レビューで修正した問題

- CRITICAL: asyncioイベントループの競合 → ThreadPoolExecutor
- CRITICAL: bank_id IDOR → クロージャ固定 + UUIDバリデーション
- HIGH: 入力バリデーション追加（長さ制限、空文字チェック）
- HIGH: fact_kind/fact_type のLLM出力バリデーション
- MEDIUM: Bedrockクライアント重複→共通化+threading.Lock
- MEDIUM: conversation ファクトの重複チェック追加
- MEDIUM: Embedding並列数制限（セマフォ5）
- MEDIUM: プロンプトインジェクション対策（区切りマーカー）

# Phase 2.1 ローカルインフラ実装完了

## 実装済みファイル

| ファイル | 行数 | 役割 |
|---|---|---|
| `postgresql/init/003_mid_term.sql` | ~100行 | 中期記憶テーブル + インデックス |
| `agentcore/memory/scheduler.py` | ~183行 | Consolidation スケジューラー（asyncio + CLI） |
| `agentcore/memory/engine.py` | ~95行 | MemoryEngine（スケジューラー統合済み） |

## 設計上の重要な決定

1. スケジューラー: asyncio.create_task でバックグラウンド実行
2. MemoryEngine 統合: initialize() で起動、close() で逆順停止
3. 多重起動防止: initialize() で scheduler is None ガード
4. CLI: scheduler.py の __main__ で単発/連続実行モード
5. _execute_consolidation: Phase 2.2.1 まではスタブ（COUNT のみ）
