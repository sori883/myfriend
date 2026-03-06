.PHONY: dev front agent batch db db-down db-reset logs clean help

# ---------------------------------------------------------------------------
# 一括起動
# ---------------------------------------------------------------------------

## ローカル開発環境を一括起動（DB + agent + batch + front）
dev: db agent batch front

# ---------------------------------------------------------------------------
# 個別サービス
# ---------------------------------------------------------------------------

## フロントエンド（Next.js）を起動
front:
	cd front && pnpm dev &

## エージェント（Strands Agent）を起動
agent:
	cd agentcore && uv run local.py &

## Consolidation バッチ（60秒間隔で連続実行）
batch:
	cd batch && uv run python local.py --interval 60 &

## PostgreSQL を起動
db:
	cd postgresql && docker compose up -d db

# ---------------------------------------------------------------------------
# 停止・リセット
# ---------------------------------------------------------------------------

## バックグラウンドプロセスを停止
clean:
	@echo "Stopping background processes..."
	-@pkill -f "uv run local.py" 2>/dev/null || true
	-@pkill -f "uv run python local.py" 2>/dev/null || true
	-@pkill -f "next dev" 2>/dev/null || true
	@echo "Done"

## PostgreSQL を停止
db-down:
	cd postgresql && docker compose down

## PostgreSQL のデータを削除して再初期化
db-reset:
	cd postgresql && docker compose down -v && rm -rf data && docker compose up -d db

## 全停止（プロセス + DB）
down: clean db-down

# ---------------------------------------------------------------------------
# ログ・状態確認
# ---------------------------------------------------------------------------

## Docker コンテナの状態を確認
logs:
	cd postgresql && docker compose ps

# ---------------------------------------------------------------------------
# テスト
# ---------------------------------------------------------------------------

## E2E テストを実行
test-e2e:
	cd agentcore && uv run e2e_test.py

# ---------------------------------------------------------------------------
# ヘルプ
# ---------------------------------------------------------------------------

## コマンド一覧を表示
help:
	@echo "Usage: make <target>"
	@echo ""
	@echo "Targets:"
	@echo "  dev        DB + agent + batch + front を一括起動"
	@echo "  front      フロントエンド (Next.js, port 3000)"
	@echo "  agent      エージェント (Strands, port 8080)"
	@echo "  batch      Consolidation バッチ (60秒間隔)"
	@echo "  db         PostgreSQL を起動"
	@echo "  clean      front + agent + batch のプロセスを停止"
	@echo "  db-down    PostgreSQL を停止"
	@echo "  db-reset   PostgreSQL のデータを削除して再起動"
	@echo "  down       全停止 (clean + db-down)"
	@echo "  logs       Docker コンテナの状態確認"
	@echo "  test-e2e   E2E テストを実行"
