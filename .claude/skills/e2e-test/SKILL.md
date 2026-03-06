---
name: e2e-test
description: docs/タスク/ の検証項目に基づいて E2E テストスクリプトを生成・実行する。DB セットアップ、async 関数の直接呼び出し、ツールバリデーション、クリーンアップを含む。
---

# E2E テストスキル

`docs/タスク/` の検証セクションを入力とし、PostgreSQL + async Python による E2E テストスクリプトを生成・実行する。

## 発動タイミング

- ユーザーが「テストして」「E2E テスト」「検証して」と指示した場合
- 実装タスク完了後に検証セクションの項目をテストする場合
- `docs/タスク/` のファイルをテスト対象として指定された場合

## 前提条件

- Docker で PostgreSQL が起動済み（`postgresql/docker-compose.yml`）
- `uv sync` でパッケージがインストール済み
- 外部 API（Tavily 等）を使うテストは環境変数が設定済み

## テストスクリプトの構成

### ファイル配置

```
agentcore/e2e_test.py    # メインのテストスクリプト
```

### 実行方法

```bash
cd agentcore && uv run e2e_test.py
```

### スクリプト構造テンプレート

```python
"""E2E テスト: {対象タスク名}

Usage:
    cd agentcore
    uv run e2e_test.py
"""
from dotenv import load_dotenv
load_dotenv(".env.local")

import asyncio
import json
import logging
import sys

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")

# テスト用の固定 UUID
BANK_ID = "00000000-0000-0000-0000-000000000001"
ENTITY_ID = "00000000-0000-0000-0000-000000000010"

passed = 0
failed = 0


def ok(name: str, detail: str = ""):
    global passed
    passed += 1
    msg = f"  PASS: {name}"
    if detail:
        msg += f" — {detail}"
    print(msg)


def ng(name: str, detail: str = ""):
    global failed
    failed += 1
    msg = f"  FAIL: {name}"
    if detail:
        msg += f" — {detail}"
    print(msg)


def assert_test(name: str, condition: bool, detail: str = ""):
    if condition:
        ok(name, detail)
    else:
        ng(name, detail)


# =========================================================================
# テスト関数（タスクごとにセクションを分ける）
# =========================================================================

async def test_task_XX(pool):
    print("\n" + "=" * 60)
    print("Task XX: タスク名")
    print("=" * 60)

    # --- XX-1: セクション名 ---
    print("\n--- XX-1: セクション名 ---")
    # テストロジック...
    assert_test("テスト名", condition, f"detail={value}")


# =========================================================================
# メイン
# =========================================================================

async def setup_test_data(pool):
    """テスト用の bank + entity を作成する"""
    async with pool.acquire() as conn:
        await conn.execute(
            """INSERT INTO banks (id, name, mission)
               VALUES ($1::uuid, 'テスト用bank', 'E2Eテスト用')
               ON CONFLICT (id) DO NOTHING""",
            BANK_ID,
        )
        await conn.execute(
            """INSERT INTO entities (id, bank_id, canonical_name, entity_type)
               VALUES ($1::uuid, $2::uuid, 'テストユーザー', 'person')
               ON CONFLICT (id) DO NOTHING""",
            ENTITY_ID, BANK_ID,
        )
        await conn.execute(
            "UPDATE banks SET owner_entity_id = $1::uuid WHERE id = $2::uuid",
            ENTITY_ID, BANK_ID,
        )
    print("  テストデータを作成しました")


async def main():
    from memory.db import get_pool
    pool = await get_pool()

    print("セットアップ")
    print("=" * 60)
    await setup_test_data(pool)

    try:
        await test_task_XX(pool)
    finally:
        # クリーンアップ（FK 制約の順序に注意）
        print("\n" + "=" * 60)
        print("クリーンアップ")
        print("=" * 60)
        async with pool.acquire() as conn:
            await conn.execute("DELETE FROM recommendation_history WHERE bank_id = $1::uuid", BANK_ID)
            await conn.execute("DELETE FROM preference_profiles WHERE bank_id = $1::uuid", BANK_ID)
            await conn.execute("UPDATE banks SET owner_entity_id = NULL WHERE id = $1::uuid", BANK_ID)
            await conn.execute("DELETE FROM entities WHERE bank_id = $1::uuid", BANK_ID)
            await conn.execute("DELETE FROM banks WHERE id = $1::uuid", BANK_ID)
        print("  テストデータを削除しました")
        await pool.close()

    # 結果サマリ
    print("\n" + "=" * 60)
    total = passed + failed
    print(f"結果: {passed}/{total} passed, {failed}/{total} failed")
    print("=" * 60)
    if failed > 0:
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())
```

## テスト作成手順

### 1. 対象タスクの検証項目を読む

```bash
cat "docs/タスク/XX_タスク名.md"
```

検証セクション（`## XX.N 検証`）のチェックリストを抽出する。

### 2. テスト対象のモジュールを特定

タスクファイルの各セクションから対象モジュールとその公開関数を特定する。

- **内部関数テスト**: `recommendation/src/recommendation/` 配下のモジュールを直接インポートし、async 関数を `await` で呼ぶ
- **ツールバリデーションテスト**: `core.py` の `_build_tools(BANK_ID)` でツール一覧を取得し、各ツールを直接呼び出す

### 3. テスト関数を作成

タスクファイルのセクション構成（`## XX.1`, `## XX.2`, ...）に対応させる。

```python
async def test_task_XX(pool):
    # --- XX-1: セクション名 ---
    # --- XX-2: セクション名 ---
    # ...
```

### 4. テストカテゴリ

各テストは以下のいずれかに分類される:

| カテゴリ | テスト方法 | 例 |
|---------|-----------|-----|
| **DB 操作** | async 関数を直接呼び出し → DB 状態を SELECT で検証 | UPSERT、EMA 更新、履歴記録 |
| **ビジネスロジック** | 関数の戻り値を検証 | スコア計算、フィルタリング、名寄せ |
| **エラーケース** | 異常入力に対するレスポンスを検証 | 空入力、無効 UUID、上限超過 |
| **認可** | 別の bank_id でアクセスし拒否されることを確認 | FK チェック、bank_id WHERE 句 |
| **外部 API** | 実 API を呼び出しレスポンス形式を検証 | Tavily 検索結果の構造 |
| **ツール定義** | `_build_tools` でツール取得 → バリデーション入力で呼び出し | 空文字、無効カテゴリ |

### 5. 検証パターン

#### 正常系

```python
result = await some_function(pool, BANK_ID, valid_input)
assert_test(
    "テスト名",
    "expected_key" in result and result["expected_key"] == expected_value,
    f"actual={result.get('expected_key')}",
)
```

#### DB 状態の直接検証

```python
async with pool.acquire() as conn:
    row = await conn.fetchrow(
        "SELECT column FROM table WHERE bank_id = $1::uuid AND ...",
        BANK_ID,
    )
assert_test("DB 値の確認", row is not None and abs(row["column"] - expected) < 0.01)
```

#### エラー系

```python
result = await some_function(pool, BANK_ID, invalid_input)
assert_test(
    "エラーが返る",
    "error" in result,
    f"error={result.get('error')}",
)
```

#### ツールバリデーション（sync ツール）

```python
from core import _build_tools
tools = _build_tools(BANK_ID)
tool = next(t for t in tools if t.__name__ == "tool_name")
result = json.loads(tool(arg="invalid"))
assert_test("無効入力でエラー", "error" in result)
```

#### 二重書き込み防止

```python
# 1回目: 成功
fb1 = await record_feedback(pool, BANK_ID, rec_id, True, "item")
assert_test("1回目成功", fb1.get("updated") == 1)

# 2回目: 拒否
fb2 = await record_feedback(pool, BANK_ID, rec_id, False)
assert_test("二重書き込み防止", fb2.get("updated") == 0 and "error" in fb2)
```

#### 認可チェック

```python
result = await some_function(pool, "00000000-0000-0000-0000-999999999999", target_id)
assert_test("bank_id 認可チェック", result.get("updated", 0) == 0)
```

## クリーンアップの FK 制約順序

テストデータ削除時は外部キー制約を考慮した順序で行う:

```
1. recommendation_history  (bank_id FK)
2. preference_profiles     (bank_id FK)
3. banks.owner_entity_id = NULL  (entities FK 解除)
4. entities                (bank_id FK)
5. banks
```

## 重要な注意点

- **LLM 呼び出しを避ける**: `extract_preferences` 等の LLM 関数は直接呼ばず、その出力（`ExtractedPreference` 等）を手動で作成して `persist_preferences` に渡す
- **dataclass を使う**: dict ではなく各モジュール定義の dataclass（`ExtractedPreference` 等）を使用する
- **テスト用 UUID は固定値**: `BANK_ID`, `ENTITY_ID` は `00000000-...` 形式の固定 UUID を使う
- **外部 API テストは環境変数チェック**: API キー未設定の場合は RuntimeError をハンドリングする
- **exit code**: 失敗テストがあれば `sys.exit(1)` で非ゼロ終了
- **`dotenv` の読み込み**: スクリプト冒頭で `load_dotenv(".env.local")` を呼ぶ（import 前）
