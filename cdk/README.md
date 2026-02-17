# AWS CDK テンプレート

AWS CDKを使用したAWS CDKのテンプレートプロジェクトです。

## セットアップ

### 依存関係のインストール

```bash
pnpm install
```

### 環境変数ファイルの作成

各環境用の `.env` ファイルを作成します。

```bash
# 開発環境
.env.dev
ACCOUNT_ID=あなたのAWSアカウントID

# ステージング環境
.env.stg
ACCOUNT_ID=あなたのAWSアカウントID

# 本番環境
.env.prd
ACCOUNT_ID=あなたのAWSアカウントID
```

### 環境別パラメータ

各環境のパラメータは `parameter/index.ts` で管理されています。

### 主な差分

| リソース | dev | stg | prd |
|---------|-----|-----|-----|
| VPC AZ 数 | 2 | 2 | 3 |
| NAT Gateway | 1 | 1 | 3 |
| EC2 インスタンス | t3.micro | t3.micro | t3.large |
| RDS バックアップ | 1日 | 1日 | 30日 |
| RDS 削除保護 | 無効 | 無効 | 有効 |

パラメータをカスタマイズする場合は、`parameter/index.ts` を編集してください。

## プロジェクト構成

```
.
├── bin/
│   └── aws-cdk-template.ts    # CDK アプリのエントリーポイント
├── lib/
│   ├── app-stack.ts            # アプリケーションスタック
│   ├── data-stack.ts           # データスタック
│   ├── infra-stack.ts          # インフラスタック
│   └── constructs/             # 再利用可能なコンストラクト
│       ├── ec2/                # EC2 関連
│       ├── network/            # VPC、VPCエンドポイント
│       ├── rds/                # RDS 関連
│       └── security-group/     # セキュリティグループ
├── parameter/
│   ├── index.ts                # パラメータ定義
│   ├── envname-type.ts         # 環境名の型定義
│   └── validate-dotenv.ts      # 環境変数バリデーション
└── test/                       # テストファイル
```

## 使い方

### CDK

```bash
# 開発環境
pnpm cdk:dev <command>  # 例: pnpm cdk:dev synth

# ステージング環境
pnpm cdk:stg <command>

# 本番環境
pnpm cdk:prd <command>
```

### テスト

```bash
# 開発環境でテスト
pnpm test:dev

# ステージング環境でテスト
pnpm test:stg

# 本番環境でテスト
pnpm test:prd
```

### コード品質

```bash
# リント (構文チェック)
pnpm lint

# リント (自動修正)
pnpm lint:fix

# フォーマット確認
pnpm format

# フォーマット (自動修正)
pnpm format:fix

# 型チェック
pnpm typecheck
```

## 構成されるリソース

- **InfraStack (インフラスタック)**
  - VPC (Virtual Private Cloud)
  - サブネット (Public / Private / EgressPrivate)
  - セキュリティグループ
  - VPC エンドポイント

- **DataStack (データスタック)**
  - Aurora PostgreSQL クラスター

- **AppStack (アプリケーションスタック)**
  - EC2 インスタンス
