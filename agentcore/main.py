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


if __name__ == "__main__":
    app.run()