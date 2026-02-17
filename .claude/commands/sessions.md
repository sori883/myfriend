# セッションコマンド

Claude Codeセッション履歴の管理 - `~/.claude/sessions/` に保存されたセッションの一覧表示、読み込み、エイリアス設定、編集。

## 使い方

`/sessions [list|load|alias|info|help] [オプション]`

## アクション

### セッション一覧

メタデータ、フィルタリング、ページネーション付きで全セッションを表示。

```bash
/sessions                              # 全セッションを一覧表示（デフォルト）
/sessions list                         # 上記と同じ
/sessions list --limit 10              # 10件のセッションを表示
/sessions list --date 2026-02-01       # 日付でフィルタ
/sessions list --search abc            # セッションIDで検索
```

**スクリプト:**
```bash
node -e "
const sm = require('./scripts/lib/session-manager');
const aa = require('./scripts/lib/session-aliases');

const result = sm.getAllSessions({ limit: 20 });
const aliases = aa.listAliases();
const aliasMap = {};
for (const a of aliases) aliasMap[a.sessionPath] = a.name;

console.log('Sessions (showing ' + result.sessions.length + ' of ' + result.total + '):');
console.log('');
console.log('ID        Date        Time     Size     Lines  Alias');
console.log('────────────────────────────────────────────────────');

for (const s of result.sessions) {
  const alias = aliasMap[s.filename] || '';
  const size = sm.getSessionSize(s.sessionPath);
  const stats = sm.getSessionStats(s.sessionPath);
  const id = s.shortId === 'no-id' ? '(none)' : s.shortId.slice(0, 8);
  const time = s.modifiedTime.toTimeString().slice(0, 5);

  console.log(id.padEnd(8) + ' ' + s.date + '  ' + time + '   ' + size.padEnd(7) + '  ' + String(stats.lineCount).padEnd(5) + '  ' + alias);
}
"
```

### セッションの読み込み

セッションの内容を読み込み表示（IDまたはエイリアスで指定）。

```bash
/sessions load <id|エイリアス>             # セッションを読み込み
/sessions load 2026-02-01             # 日付で指定（no-idセッション用）
/sessions load a1b2c3d4               # 短縮IDで指定
/sessions load my-alias               # エイリアス名で指定
```

**スクリプト:**
```bash
node -e "
const sm = require('./scripts/lib/session-manager');
const aa = require('./scripts/lib/session-aliases');
const id = process.argv[1];

// まずエイリアスとして解決を試みる
const resolved = aa.resolveAlias(id);
const sessionId = resolved ? resolved.sessionPath : id;

const session = sm.getSessionById(sessionId, true);
if (!session) {
  console.log('セッションが見つかりません: ' + id);
  process.exit(1);
}

const stats = sm.getSessionStats(session.sessionPath);
const size = sm.getSessionSize(session.sessionPath);
const aliases = aa.getAliasesForSession(session.filename);

console.log('セッション: ' + session.filename);
console.log('パス: ~/.claude/sessions/' + session.filename);
console.log('');
console.log('統計:');
console.log('  行数: ' + stats.lineCount);
console.log('  総アイテム: ' + stats.totalItems);
console.log('  完了: ' + stats.completedItems);
console.log('  進行中: ' + stats.inProgressItems);
console.log('  サイズ: ' + size);
console.log('');

if (aliases.length > 0) {
  console.log('エイリアス: ' + aliases.map(a => a.name).join(', '));
  console.log('');
}

if (session.metadata.title) {
  console.log('タイトル: ' + session.metadata.title);
  console.log('');
}

if (session.metadata.started) {
  console.log('開始: ' + session.metadata.started);
}

if (session.metadata.lastUpdated) {
  console.log('最終更新: ' + session.metadata.lastUpdated);
}
" "$ARGUMENTS"
```

### エイリアスの作成

セッションに覚えやすいエイリアスを作成。

```bash
/sessions alias <id> <名前>           # エイリアスを作成
/sessions alias 2026-02-01 today-work # "today-work"というエイリアスを作成
```

**スクリプト:**
```bash
node -e "
const sm = require('./scripts/lib/session-manager');
const aa = require('./scripts/lib/session-aliases');

const sessionId = process.argv[1];
const aliasName = process.argv[2];

if (!sessionId || !aliasName) {
  console.log('使い方: /sessions alias <id> <名前>');
  process.exit(1);
}

// セッションのファイル名を取得
const session = sm.getSessionById(sessionId);
if (!session) {
  console.log('セッションが見つかりません: ' + sessionId);
  process.exit(1);
}

const result = aa.setAlias(aliasName, session.filename);
if (result.success) {
  console.log('✓ エイリアス作成: ' + aliasName + ' → ' + session.filename);
} else {
  console.log('✗ エラー: ' + result.error);
  process.exit(1);
}
" "$ARGUMENTS"
```

### エイリアスの削除

既存のエイリアスを削除。

```bash
/sessions alias --remove <名前>        # エイリアスを削除
/sessions unalias <名前>               # 上記と同じ
```

**スクリプト:**
```bash
node -e "
const aa = require('./scripts/lib/session-aliases');

const aliasName = process.argv[1];
if (!aliasName) {
  console.log('使い方: /sessions alias --remove <名前>');
  process.exit(1);
}

const result = aa.deleteAlias(aliasName);
if (result.success) {
  console.log('✓ エイリアス削除: ' + aliasName);
} else {
  console.log('✗ エラー: ' + result.error);
  process.exit(1);
}
" "$ARGUMENTS"
```

### セッション情報

セッションの詳細情報を表示。

```bash
/sessions info <id|エイリアス>              # セッションの詳細を表示
```

**スクリプト:**
```bash
node -e "
const sm = require('./scripts/lib/session-manager');
const aa = require('./scripts/lib/session-aliases');

const id = process.argv[1];
const resolved = aa.resolveAlias(id);
const sessionId = resolved ? resolved.sessionPath : id;

const session = sm.getSessionById(sessionId, true);
if (!session) {
  console.log('セッションが見つかりません: ' + id);
  process.exit(1);
}

const stats = sm.getSessionStats(session.sessionPath);
const size = sm.getSessionSize(session.sessionPath);
const aliases = aa.getAliasesForSession(session.filename);

console.log('セッション情報');
console.log('════════════════════');
console.log('ID:          ' + (session.shortId === 'no-id' ? '(なし)' : session.shortId));
console.log('ファイル名:    ' + session.filename);
console.log('日付:        ' + session.date);
console.log('更新日時:    ' + session.modifiedTime.toISOString().slice(0, 19).replace('T', ' '));
console.log('');
console.log('内容:');
console.log('  行数:         ' + stats.lineCount);
console.log('  総アイテム:   ' + stats.totalItems);
console.log('  完了:     ' + stats.completedItems);
console.log('  進行中:   ' + stats.inProgressItems);
console.log('  サイズ:          ' + size);
if (aliases.length > 0) {
  console.log('エイリアス:     ' + aliases.map(a => a.name).join(', '));
}
" "$ARGUMENTS"
```

### エイリアス一覧

全セッションエイリアスを表示。

```bash
/sessions aliases                      # 全エイリアスを一覧表示
```

**スクリプト:**
```bash
node -e "
const aa = require('./scripts/lib/session-aliases');

const aliases = aa.listAliases();
console.log('セッションエイリアス (' + aliases.length + '件):');
console.log('');

if (aliases.length === 0) {
  console.log('エイリアスが見つかりません。');
} else {
  console.log('名前          セッションファイル                    タイトル');
  console.log('─────────────────────────────────────────────────────────────');
  for (const a of aliases) {
    const name = a.name.padEnd(12);
    const file = (a.sessionPath.length > 30 ? a.sessionPath.slice(0, 27) + '...' : a.sessionPath).padEnd(30);
    const title = a.title || '';
    console.log(name + ' ' + file + ' ' + title);
  }
}
"
```

## 引数

$ARGUMENTS:
- `list [オプション]` - セッション一覧
  - `--limit <n>` - 最大表示数（デフォルト: 50）
  - `--date <YYYY-MM-DD>` - 日付でフィルタ
  - `--search <パターン>` - セッションIDで検索
- `load <id|エイリアス>` - セッション内容を読み込み
- `alias <id> <名前>` - セッションにエイリアスを作成
- `alias --remove <名前>` - エイリアスを削除
- `unalias <名前>` - `--remove` と同じ
- `info <id|エイリアス>` - セッション統計を表示
- `aliases` - 全エイリアスを一覧表示
- `help` - このヘルプを表示

## 使用例

```bash
# 全セッションを一覧表示
/sessions list

# 今日のセッションにエイリアスを作成
/sessions alias 2026-02-01 today

# エイリアスでセッションを読み込み
/sessions load today

# セッション情報を表示
/sessions info today

# エイリアスを削除
/sessions alias --remove today

# 全エイリアスを一覧表示
/sessions aliases
```

## 注意事項

- セッションは `~/.claude/sessions/` にMarkdownファイルとして保存される
- エイリアスは `~/.claude/session-aliases.json` に保存される
- セッションIDは短縮可能（最初の4-8文字で通常一意に特定可能）
- 頻繁に参照するセッションにはエイリアスを使用
