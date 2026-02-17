# コーディングスタイル

## イミュータビリティ（CRITICAL）

常に新しいオブジェクトを作成し、絶対にミューテーションしない:

```javascript
// NG: ミューテーション
function updateUser(user, name) {
  user.name = name  // ミューテーション!
  return user
}

// OK: イミュータビリティ
function updateUser(user, name) {
  return {
    ...user,
    name
  }
}
```

## ファイル構成

多数の小さなファイル > 少数の大きなファイル:
- 高凝集・低結合
- 通常200〜400行、最大800行
- 大きなコンポーネントからユーティリティを抽出する
- 種類別ではなく、機能/ドメイン別に整理する

## エラーハンドリング

常にエラーを包括的に処理する:

```typescript
try {
  const result = await riskyOperation()
  return result
} catch (error) {
  console.error('操作に失敗しました:', error)
  throw new Error('ユーザー向けの詳細なエラーメッセージ')
}
```

## 入力バリデーション

常にユーザー入力をバリデーションする:

```typescript
import { z } from 'zod'

const schema = z.object({
  email: z.string().email(),
  age: z.number().int().min(0).max(150)
})

const validated = schema.parse(input)
```

## コード品質チェックリスト

作業完了前に確認:
- [ ] コードが読みやすく、適切な命名がされている
- [ ] 関数が小さい（50行未満）
- [ ] ファイルが集中している（800行未満）
- [ ] 深いネストがない（4レベル以上はNG）
- [ ] 適切なエラーハンドリング
- [ ] console.log文がない
- [ ] ハードコードされた値がない
- [ ] ミューテーションがない（イミュータブルパターンを使用）
