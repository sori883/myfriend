#!/bin/bash
# 戦略的コンパクト提案ツール
# PreToolUseまたは定期的に実行し、論理的な区切りで手動圧縮を提案する
#
# 自動圧縮より手動圧縮が優れている理由:
# - 自動圧縮は任意のタイミング（タスクの途中など）で発生する
# - 戦略的な圧縮は論理的なフェーズを通じてコンテキストを保持する
# - 調査後、実行前に圧縮
# - マイルストーン完了後、次の開始前に圧縮
#
# フック設定（~/.claude/settings.json内）:
# {
#   "hooks": {
#     "PreToolUse": [{
#       "matcher": "Edit|Write",
#       "hooks": [{
#         "type": "command",
#         "command": "~/.claude/skills/strategic-compact/suggest-compact.sh"
#       }]
#     }]
#   }
# }
#
# 圧縮を提案する基準:
# - セッションが長時間実行されている
# - 大量のツールコールが行われた
# - リサーチ/調査から実装への移行時
# - 計画が確定した時

# ツールコール数を追跡（一時ファイルでインクリメント）
COUNTER_FILE="/tmp/claude-tool-count-$$"
THRESHOLD=${COMPACT_THRESHOLD:-50}

# カウンターの初期化またはインクリメント
if [ -f "$COUNTER_FILE" ]; then
  count=$(cat "$COUNTER_FILE")
  count=$((count + 1))
  echo "$count" > "$COUNTER_FILE"
else
  echo "1" > "$COUNTER_FILE"
  count=1
fi

# 閾値到達後に圧縮を提案
if [ "$count" -eq "$THRESHOLD" ]; then
  echo "[StrategicCompact] $THRESHOLD tool calls reached - consider /compact if transitioning phases" >&2
fi

# 閾値後は定期的に提案
if [ "$count" -gt "$THRESHOLD" ] && [ $((count % 25)) -eq 0 ]; then
  echo "[StrategicCompact] $count tool calls - good checkpoint for /compact if context is stale" >&2
fi
