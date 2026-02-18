from dotenv import load_dotenv
load_dotenv()

import atexit
import json
import logging
import os

from bedrock_agentcore.runtime import BedrockAgentCoreApp

from core import validate_bank_id, stream_agent, shutdown_sync

logger = logging.getLogger(__name__)

app = BedrockAgentCoreApp()

AGENT_MODEL_ID = os.environ.get("AGENT_MODEL_ID", "openai.gpt-oss-120b-1:0")

atexit.register(shutdown_sync)


@app.entrypoint
async def invoke(payload):
    try:
        bank_id = validate_bank_id(payload.get("bank_id", ""))
    except ValueError:
        yield json.dumps({"error": "Invalid or missing bank_id. Expected a valid UUID."})
        return

    prompt = payload.get("prompt")
    if not prompt or not str(prompt).strip():
        yield json.dumps({"error": "prompt is required."})
        return

    async for chunk in stream_agent(bank_id, str(prompt).strip(), AGENT_MODEL_ID):
        yield chunk


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    app.run()
