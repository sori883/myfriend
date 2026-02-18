import { useState } from 'hono/jsx'

const BANK_ID = '00000000-0000-4000-8000-000000000001'

export default function Chat() {
  const [prompt, setPrompt] = useState('')
  const [response, setResponse] = useState('')
  const [loading, setLoading] = useState(false)

  const handleSubmit = async (e: Event) => {
    e.preventDefault()
    if (!prompt.trim() || loading) return

    setResponse('')
    setLoading(true)

    try {
      const res = await fetch('/api/invoke', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ bank_id: BANK_ID, prompt: prompt.trim() }),
      })

      if (!res.ok) {
        const text = await res.text()
        setResponse(`Error ${res.status}: ${text}`)
        return
      }

      const reader = res.body!.getReader()
      const decoder = new TextDecoder()
      let text = ''

      while (true) {
        const { done, value } = await reader.read()
        if (done) break
        text += decoder.decode(value, { stream: true })
        setResponse(text)
      }
    } catch (err) {
      setResponse(`Error: ${err}`)
    } finally {
      setLoading(false)
    }
  }

  return (
    <div class="max-w-2xl mx-auto px-4">
      <div class="mb-4 text-sm text-gray-500 font-mono">
        bank_id: {BANK_ID}
      </div>

      <form onSubmit={handleSubmit} class="mb-6">
        <div class="flex gap-2">
          <input
            type="text"
            value={prompt}
            onInput={(e: InputEvent) => setPrompt((e.target as HTMLInputElement).value)}
            placeholder="メッセージを入力..."
            class="flex-1 px-3 py-2 border border-gray-300 rounded bg-white"
            disabled={loading}
          />
          <button
            type="submit"
            disabled={loading || !prompt.trim()}
            class="px-4 py-2 bg-blue-600 text-white rounded hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed"
          >
            {loading ? '...' : '送信'}
          </button>
        </div>
      </form>

      {response && (
        <div class="p-4 bg-white border border-gray-200 rounded whitespace-pre-wrap text-sm leading-relaxed">
          {response}
        </div>
      )}
    </div>
  )
}
