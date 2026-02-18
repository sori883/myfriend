"""Local development server for the Strands Agent.

Usage:
    cd agentcore
    uv run local.py

Hosts the agent on http://localhost:8080/invoke
"""
from dotenv import load_dotenv
load_dotenv()

import logging
import os

from aiohttp import web

from core import validate_bank_id, stream_agent, shutdown

logger = logging.getLogger(__name__)

AGENT_MODEL_ID = os.environ.get("AGENT_MODEL_ID", "anthropic.claude-3-5-sonnet-20240620-v1:0")


async def handle_invoke(request: web.Request) -> web.StreamResponse:
    try:
        body = await request.json()
    except Exception:
        return web.json_response({"error": "Invalid JSON"}, status=400)

    try:
        bank_id = validate_bank_id(body.get("bank_id", ""))
    except ValueError:
        return web.json_response(
            {"error": "Invalid or missing bank_id. Expected a valid UUID."}, status=400
        )

    prompt = body.get("prompt", "")
    if not isinstance(prompt, str) or not prompt.strip():
        return web.json_response({"error": "prompt must be a non-empty string"}, status=400)

    response = web.StreamResponse(
        status=200,
        headers={"Content-Type": "text/plain; charset=utf-8"},
    )
    await response.prepare(request)

    try:
        async for chunk in stream_agent(bank_id, prompt.strip(), AGENT_MODEL_ID):
            await response.write(chunk.encode("utf-8"))
    except Exception:
        logger.error("Agent stream error", exc_info=True)
        await response.write(b"\n[Error: Agent execution failed]")

    await response.write_eof()
    return response


@web.middleware
async def cors_middleware(request: web.Request, handler):
    if request.method == "OPTIONS":
        resp = web.Response()
    else:
        resp = await handler(request)
    resp.headers["Access-Control-Allow-Origin"] = "*"
    resp.headers["Access-Control-Allow-Methods"] = "POST, OPTIONS"
    resp.headers["Access-Control-Allow-Headers"] = "Content-Type"
    return resp


async def on_shutdown(_app: web.Application) -> None:
    await shutdown()


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    logging.getLogger("strands.agent").setLevel(logging.DEBUG)
    logging.getLogger("strands.models").setLevel(logging.DEBUG)

    app = web.Application(middlewares=[cors_middleware])
    app.router.add_post("/invoke", handle_invoke)
    app.on_shutdown.append(on_shutdown)

    port = int(os.environ.get("PORT", "8080"))
    logger.info("Starting local agent server on http://localhost:%d/invoke", port)
    web.run_app(app, host="127.0.0.1", port=port)


if __name__ == "__main__":
    main()
