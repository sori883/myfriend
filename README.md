# 使用方法

```bash
# DB起動
cd postgresql && docker compose up -d

# AgentCore起動
uv sync
uv run local.py

# バッチ
cd batch
uv run python local.py --interval 60

# フロントエンド
cd front
pnpm run dev

# CDK
pnpm cdk:dev deploy
```

# モデル

- 原則ローカルで使用する予定で外部に露見させない
- 外部で使用する場合は容易に取り出せない状態とする
  - 対象者を絞った限定的な公開
  - Next.jsのミドルウェアで認証設定
  - モデルを暗号化


詳細は下記を参照
docs/設計/フロントエンド/モデル保護.md


https://mk22.booth.pm/items/5007531

# docs

下記参照

- docs/設計
  - AWS
  - フロントエンド
  - 記憶システム

# 実装マイルストーン
[x] 記憶保持、検索

[x] 外見の作成

[ ] 話しかけて欲しい
- 知らないことを教えて欲しい
- 目的を持った会話だけじゃなくてしょうもないことを言って欲しい
- 独り言があっても良い

[ ] 感情の度合い
- 外的要因による感情の変化
- ユーザーに対する感情数値変動

[ ] 最終目標:ハッピーエンド
- 仮想世界の具現化

