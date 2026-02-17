# チェックポイントコマンド

ワークフローにチェックポイントを作成または検証する。

## 使い方

`/checkpoint [create|verify|list] [名前]`

## チェックポイントの作成

チェックポイント作成時:

1. `/verify quick` を実行して現在の状態がクリーンであることを確認
2. チェックポイント名でgit stashまたはコミットを作成
3. チェックポイントを `.claude/checkpoints.log` に記録:

```bash
echo "$(date +%Y-%m-%d-%H:%M) | $CHECKPOINT_NAME | $(git rev-parse --short HEAD)" >> .claude/checkpoints.log
```

4. チェックポイント作成を報告

## チェックポイントの検証

チェックポイントに対して検証する場合:

1. ログからチェックポイントを読み取り
2. 現在の状態をチェックポイントと比較:
   - チェックポイント以降に追加されたファイル
   - チェックポイント以降に変更されたファイル
   - テストの合格率の比較
   - カバレッジの比較

3. 報告:
```
CHECKPOINT COMPARISON: $NAME
============================
変更ファイル: X
テスト: +Y合格 / -Z失敗
カバレッジ: +X% / -Y%
ビルド: [PASS/FAIL]
```

## チェックポイントの一覧

全チェックポイントを以下の情報と共に表示:
- 名前
- タイムスタンプ
- Git SHA
- ステータス（current、behind、ahead）

## ワークフロー

典型的なチェックポイントフロー:

```
[開始] --> /checkpoint create "feature-start"
   |
[実装] --> /checkpoint create "core-done"
   |
[テスト] --> /checkpoint verify "core-done"
   |
[リファクタリング] --> /checkpoint create "refactor-done"
   |
[PR] --> /checkpoint verify "feature-start"
```

## 引数

$ARGUMENTS:
- `create <名前>` - 名前付きチェックポイントを作成
- `verify <名前>` - 名前付きチェックポイントに対して検証
- `list` - 全チェックポイントを表示
- `clear` - 古いチェックポイントを削除（最新5件を保持）
