import { createRoute } from 'honox/factory'
import Chat from '../islands/chat'

export default createRoute((c) => {
  return c.render(
    <div class="min-h-screen bg-gray-50 py-8">
      <title>AgentCore Requester</title>
      <h1 class="text-2xl font-bold text-center mb-6">AgentCore Requester</h1>
      <Chat />
    </div>
  )
})
