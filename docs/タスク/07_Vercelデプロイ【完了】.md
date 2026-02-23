# 7. Vercel デプロイ

フロントエンドを Vercel にデプロイし、アクセス制御・API レート制限を設定する。

## 7.1 パスワード保護（Next.js Middleware） ✅

- URL パラメータ認証 (`?token=<ハッシュ値>`) → httpOnly cookie セット → 30日間認証
- ローカル開発時は自動スキップ
- セキュリティ: 定時間比較、派生セッショントークン

## 7.2 API Gateway レート制限 ✅

- 全環境の `dailyQuota` を 50 に変更（1日50回まで）
- 超過時は API Gateway が 429 Too Many Requests を返却
