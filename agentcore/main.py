from dotenv import load_dotenv
load_dotenv()
  
from bedrock_agentcore.runtime import BedrockAgentCoreApp
from strands import Agent

app = BedrockAgentCoreApp()


@app.entrypoint
async def invoke(payload):
    agent = Agent(
        model="openai.gpt-oss-120b-1:0",
        system_prompt="""
            You are a helpful assistant.    
        """,
    )

    stream = agent.stream_async(payload.get("prompt"))

    async for event in stream:
        if "data" in event and isinstance(event["data"], str):
            yield event["data"]


async def check_db():
    """DB 接続確認用"""
    import asyncio
    from memory.db import get_pool, close_pool

    pool = await get_pool()
    row = await pool.fetchrow("SELECT 1 AS ok")
    print(f"DB connection OK: {row}")

    tables = await pool.fetch(
        "SELECT tablename FROM pg_tables WHERE schemaname = 'public' ORDER BY tablename"
    )
    print(f"Tables: {[t['tablename'] for t in tables]}")

    extensions = await pool.fetch(
        "SELECT extname FROM pg_extension ORDER BY extname"
    )
    print(f"Extensions: {[e['extname'] for e in extensions]}")

    await close_pool()


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "check-db":
        import asyncio
        asyncio.run(check_db())
    else:
        app.run()