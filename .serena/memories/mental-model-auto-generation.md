# Mental Model 自動生成（Consolidation 連動）

## 概要

Consolidation 実行後、Observation が十分蓄積されたエンティティに Mental Model を自動生成する。

## 実装ファイル

| ファイル | 変更内容 |
|---|---|
| `agentcore/memory/consolidation.py` | エンティティリンク引き継ぎ、affected_observation_ids 追跡、トリガー呼び出し |
| `agentcore/memory/mental_model_trigger.py` | **新規** リフレッシュ + 自動生成ロジック（consolidation.py から分離） |
| `agentcore/memory/mental_model.py` | `create_mental_model` に `entity_id` パラメータ追加 |
| `postgresql/init/004_long_term.sql` | `idx_mental_models_bank_entity` ユニークインデックス追加 |

## 処理フロー

```
consolidate()
  ├── Raw Facts → Observations（既存）
  ├── エンティティリンク引き継ぎ（_execute_create_action / _execute_update_action）
  ├── affected_observation_ids 収集
  ├── 鮮度更新（既存）
  ├── Mental Model リフレッシュ（trigger_mental_model_refresh, 最大3件）
  └── Mental Model 自動生成（trigger_mental_model_generation, 最大2件）
        ├── affected_observation_ids → エンティティ候補を取得
        ├── Observation 数 >= 5 && Mental Model 未作成 のエンティティを選定
        ├── 重複チェック（entity_id + pg_trgm similarity >= 0.8）
        ├── Reflect(source_query, max_iterations=5) で content を生成
        └── create_mental_model() で保存（entity_id + refresh_after_consolidation=true）
```

## 定数

| 定数 | 値 | 場所 |
|---|---|---|
| MIN_OBSERVATIONS_FOR_GENERATION | 5 | mental_model_trigger.py |
| MAX_GENERATION_PER_CONSOLIDATION | 2 | mental_model_trigger.py |
| MAX_REFRESH_PER_CONSOLIDATION | 3 | mental_model_trigger.py |
| Reflect max_iterations | 5 | mental_model_trigger.py |
| 重複判定 similarity | 0.8 | mental_model_trigger.py |
| 最小 content 長 | 50 文字 | mental_model_trigger.py |

## 重複防止

1. SQL: `_find_generation_candidates` で entity_id による LEFT JOIN チェック
2. App: `_check_mental_model_exists` で entity_id + pg_trgm similarity 二重チェック
3. DB: `idx_mental_models_bank_entity` UNIQUE インデックス（最終防御）

## モデル ID 遅延評価

`python -m memory.scheduler` ではインポートチェーン（`memory/__init__.py` → `engine.py` → 各モジュール）が
`load_dotenv()` より先に実行されるため、モジュールレベルの `os.environ.get()` が `.env` を読めない問題があった。

全5ファイルで定数を遅延評価関数に変更:
- `reflect.py`: `_get_reflect_model_id()`
- `reranker.py`: `_get_rerank_model_id()`, `_get_rerank_model_arn()`
- `embedding.py`: `_get_embedding_model_id()`
- `consolidation.py`: `_get_consolidation_model_id()`
- `extraction.py`: `_get_extraction_model_id()`

## 検証結果

- テストスクリプト `test_phase3_verification.py`: 10/10 PASS
- E2E 手動検証:
  - 自動生成成功（田中太郎, mm_generated=1, Reflect iterations=4）
  - 重複防止確認（2回目以降 mm_generated=0）
  - Mental Model content に有用な要約が含まれることを確認