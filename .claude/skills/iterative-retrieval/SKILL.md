---
name: iterative-retrieval
description: サブエージェントのコンテキスト問題を解決するための、段階的なコンテキスト検索精緻化パターン
---

# 反復的検索パターン

マルチエージェントワークフローにおける「コンテキスト問題」を解決する。サブエージェントは作業を開始するまで必要なコンテキストが分からない。

## 問題

サブエージェントは限られたコンテキストで起動される。以下が不明:
- どのファイルに関連コードが含まれているか
- コードベースにどのようなパターンが存在するか
- プロジェクトでどのような用語が使われているか

標準的なアプローチは失敗する:
- **全て送る**: コンテキスト制限を超える
- **何も送らない**: エージェントが重要な情報を欠く
- **必要なものを推測する**: 多くの場合間違う

## 解決策: 反復的検索

コンテキストを段階的に精緻化する4フェーズのループ:

```
┌─────────────────────────────────────────────┐
│                                             │
│   ┌──────────┐      ┌──────────┐            │
│   │ DISPATCH │─────▶│ EVALUATE │            │
│   └──────────┘      └──────────┘            │
│        ▲                  │                 │
│        │                  ▼                 │
│   ┌──────────┐      ┌──────────┐            │
│   │   LOOP   │◀─────│  REFINE  │            │
│   └──────────┘      └──────────┘            │
│                                             │
│        最大3サイクル後、続行                   │
└─────────────────────────────────────────────┘
```

### フェーズ1: DISPATCH（ディスパッチ）

候補ファイルを収集するための初期広範クエリ:

```javascript
// 高レベルの意図から開始
const initialQuery = {
  patterns: ['src/**/*.ts', 'lib/**/*.ts'],
  keywords: ['authentication', 'user', 'session'],
  excludes: ['*.test.ts', '*.spec.ts']
};

// 検索エージェントにディスパッチ
const candidates = await retrieveFiles(initialQuery);
```

### フェーズ2: EVALUATE（評価）

取得したコンテンツの関連性を評価:

```javascript
function evaluateRelevance(files, task) {
  return files.map(file => ({
    path: file.path,
    relevance: scoreRelevance(file.content, task),
    reason: explainRelevance(file.content, task),
    missingContext: identifyGaps(file.content, task)
  }));
}
```

スコアリング基準:
- **高 (0.8-1.0)**: 対象機能を直接実装している
- **中 (0.5-0.7)**: 関連するパターンや型を含む
- **低 (0.2-0.4)**: 間接的に関連
- **なし (0-0.2)**: 無関係、除外

### フェーズ3: REFINE（精緻化）

評価に基づいて検索条件を更新:

```javascript
function refineQuery(evaluation, previousQuery) {
  return {
    // 高関連性ファイルで発見された新しいパターンを追加
    patterns: [...previousQuery.patterns, ...extractPatterns(evaluation)],

    // コードベースで見つかった用語を追加
    keywords: [...previousQuery.keywords, ...extractKeywords(evaluation)],

    // 確認済みの無関係パスを除外
    excludes: [...previousQuery.excludes, ...evaluation
      .filter(e => e.relevance < 0.2)
      .map(e => e.path)
    ],

    // 特定のギャップをターゲット
    focusAreas: evaluation
      .flatMap(e => e.missingContext)
      .filter(unique)
  };
}
```

### フェーズ4: LOOP（ループ）

精緻化された条件で繰り返し（最大3サイクル）:

```javascript
async function iterativeRetrieve(task, maxCycles = 3) {
  let query = createInitialQuery(task);
  let bestContext = [];

  for (let cycle = 0; cycle < maxCycles; cycle++) {
    const candidates = await retrieveFiles(query);
    const evaluation = evaluateRelevance(candidates, task);

    // 十分なコンテキストがあるか確認
    const highRelevance = evaluation.filter(e => e.relevance >= 0.7);
    if (highRelevance.length >= 3 && !hasCriticalGaps(evaluation)) {
      return highRelevance;
    }

    // 精緻化して続行
    query = refineQuery(evaluation, query);
    bestContext = mergeContext(bestContext, highRelevance);
  }

  return bestContext;
}
```

## 実践例

### 例1: バグ修正のコンテキスト

```
タスク: "認証トークンの有効期限バグを修正"

サイクル1:
  DISPATCH: src/**で"token"、"auth"、"expiry"を検索
  EVALUATE: auth.ts (0.9)、tokens.ts (0.8)、user.ts (0.3)が見つかった
  REFINE: "refresh"、"jwt"キーワードを追加; user.tsを除外

サイクル2:
  DISPATCH: 精緻化された条件で検索
  EVALUATE: session-manager.ts (0.95)、jwt-utils.ts (0.85)が見つかった
  REFINE: 十分なコンテキスト（高関連性ファイル2つ）

結果: auth.ts, tokens.ts, session-manager.ts, jwt-utils.ts
```

### 例2: 機能実装

```
タスク: "APIエンドポイントにレート制限を追加"

サイクル1:
  DISPATCH: routes/**で"rate"、"limit"、"api"を検索
  EVALUATE: マッチなし - コードベースは"throttle"という用語を使用
  REFINE: "throttle"、"middleware"キーワードを追加

サイクル2:
  DISPATCH: 精緻化された条件で検索
  EVALUATE: throttle.ts (0.9)、middleware/index.ts (0.7)が見つかった
  REFINE: ルーターパターンが必要

サイクル3:
  DISPATCH: "router"、"express"パターンを検索
  EVALUATE: router-setup.ts (0.8)が見つかった
  REFINE: 十分なコンテキスト

結果: throttle.ts, middleware/index.ts, router-setup.ts
```

## エージェントとの統合

エージェントプロンプトで使用:

```markdown
このタスクのコンテキストを取得する際:
1. 広範なキーワード検索から開始
2. 各ファイルの関連性を評価（0-1スケール）
3. まだ不足しているコンテキストを特定
4. 検索条件を精緻化して繰り返す（最大3サイクル）
5. 関連性 >= 0.7のファイルを返す
```

## ベストプラクティス

1. **広く始めて、段階的に絞る** - 初期クエリを過度に特定しない
2. **コードベースの用語を学ぶ** - 最初のサイクルで命名規約が判明することが多い
3. **不足しているものを追跡** - 明示的なギャップ特定が精緻化を推進
4. **「十分」で停止** - 高関連性3ファイルは中程度10ファイルに勝る
5. **自信を持って除外** - 低関連性ファイルは関連性が高くなることはない

## 関連

- [The Longform Guide](https://x.com/affaanmustafa/status/2014040193557471352) - サブエージェントオーケストレーションセクション
- `continuous-learning` スキル - 時間とともに改善されるパターン用
- `~/.claude/agents/` のエージェント定義
