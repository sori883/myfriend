.PHONY: dev front agent batch db db-down db-reset logs clean seed aws-restore help

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
# バックアップ・リストア
# ---------------------------------------------------------------------------

BACKUP_DIR    := postgresql/backups
TIMESTAMP     := $(shell date +%Y%m%d_%H%M%S)
CONTAINER     := postgresql-db-1
DB_NAME       := myfriend
DB_USER       := postgres
STACK_NAME    := MyfriendStack
RESTORE_FUNC  := myfriend-db-restore

## データのみバックアップ（postgresql/backups/ に保存）
db-backup:
	@mkdir -p $(BACKUP_DIR)
	docker exec $(CONTAINER) pg_dump -U $(DB_USER) -d $(DB_NAME) \
		--data-only --no-owner --no-privileges \
		--disable-triggers \
		--exclude-table='_migration_history' \
		> $(BACKUP_DIR)/backup_$(TIMESTAMP).sql
	@echo "Backup saved: $(BACKUP_DIR)/backup_$(TIMESTAMP).sql"

## スキーマ+データの完全バックアップ
db-backup-full:
	@mkdir -p $(BACKUP_DIR)
	docker exec $(CONTAINER) pg_dump -U $(DB_USER) -d $(DB_NAME) \
		--no-owner --no-privileges \
		--exclude-table='_migration_history' \
		> $(BACKUP_DIR)/full_$(TIMESTAMP).sql
	@echo "Full backup saved: $(BACKUP_DIR)/full_$(TIMESTAMP).sql"

## 最新のバックアップをリストア（既存データを上書き）
db-restore:
	@LATEST=$$(ls -t $(BACKUP_DIR)/backup_*.sql 2>/dev/null | head -1); \
	if [ -z "$$LATEST" ]; then \
		echo "Error: No backup files found in $(BACKUP_DIR)/"; \
		exit 1; \
	fi; \
	echo "Restoring from: $$LATEST"; \
	docker exec -i $(CONTAINER) psql -U $(DB_USER) -d $(DB_NAME) < "$$LATEST"; \
	echo "Restore complete"

## 指定ファイルからリストア（例: make db-restore-file FILE=postgresql/backups/backup_20260307.sql）
db-restore-file:
	@if [ -z "$(FILE)" ]; then \
		echo "Error: FILE is required. Usage: make db-restore-file FILE=path/to/backup.sql"; \
		exit 1; \
	fi
	@echo "Restoring from: $(FILE)"
	docker exec -i $(CONTAINER) psql -U $(DB_USER) -d $(DB_NAME) < $(FILE)
	@echo "Restore complete"

## バックアップを初期データとして配置（db-reset 時に自動投入される）
db-seed:
	@LATEST=$$(ls -t $(BACKUP_DIR)/backup_*.sql 2>/dev/null | head -1); \
	if [ -z "$$LATEST" ]; then \
		echo "Error: No backup files found in $(BACKUP_DIR)/"; \
		exit 1; \
	fi; \
	cp "$$LATEST" postgresql/init/006_seed_data.sql; \
	echo "Seed data created: postgresql/init/006_seed_data.sql"; \
	echo "Run 'make db-reset' to apply"

## バックアップ一覧を表示
db-backup-list:
	@ls -lh $(BACKUP_DIR)/*.sql 2>/dev/null || echo "No backups found in $(BACKUP_DIR)/"

## SQLバックアップをAWS Auroraにリストア（最新 or FILE=指定）
aws-restore:
	@if [ -n "$(FILE)" ]; then \
		SQL_FILE="$(FILE)"; \
	else \
		SQL_FILE=$$(ls -t $(BACKUP_DIR)/backup_*.sql 2>/dev/null | head -1); \
	fi; \
	if [ -z "$$SQL_FILE" ]; then \
		echo "Error: No backup files found. Run 'make db-backup' first or specify FILE=path"; \
		exit 1; \
	fi; \
	BUCKET=$$(aws cloudformation describe-stacks \
		--stack-name $(STACK_NAME) \
		--query "Stacks[0].Outputs[?contains(OutputKey,'RestoreBucketName')].OutputValue" \
		--output text); \
	if [ -z "$$BUCKET" ]; then \
		echo "Error: Could not find restore bucket. Deploy CDK first."; \
		exit 1; \
	fi; \
	KEY=$$(basename "$$SQL_FILE"); \
	echo "Uploading $$SQL_FILE to s3://$$BUCKET/$$KEY ..."; \
	aws s3 cp "$$SQL_FILE" "s3://$$BUCKET/$$KEY"; \
	echo "Invoking Lambda $(RESTORE_FUNC) ..."; \
	aws lambda invoke \
		--function-name $(RESTORE_FUNC) \
		--payload "$$(printf '{"key":"%s"}' "$$KEY")" \
		--cli-binary-format raw-in-base64-out \
		/dev/stdout; \
	echo ""

# ---------------------------------------------------------------------------
# ログ・状態確認
# ---------------------------------------------------------------------------

## Docker コンテナの状態を確認
logs:
	cd postgresql && docker compose ps

# ---------------------------------------------------------------------------
# テスト
# ---------------------------------------------------------------------------

## ペルソナ発話データをDBに投入
seed:
	cd agentcore && uv run python test_script/seed_persona.py

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
	@echo "  db-reset        PostgreSQL のデータを削除して再起動"
	@echo "  down            全停止 (clean + db-down)"
	@echo "  logs            Docker コンテナの状態確認"
	@echo "  seed            ペルソナ発話データをDBに投入"
	@echo "  test-e2e        E2E テストを実行"
	@echo ""
	@echo "Backup/Restore:"
	@echo "  db-backup       データのみバックアップ"
	@echo "  db-backup-full  スキーマ+データの完全バックアップ"
	@echo "  db-backup-list  バックアップ一覧を表示"
	@echo "  db-restore      最新のバックアップをリストア"
	@echo "  db-restore-file FILE=path  指定ファイルからリストア"
	@echo "  db-seed         最新バックアップを初期データとして配置"
	@echo "  aws-restore     SQLバックアップをAWS Auroraにリストア"
