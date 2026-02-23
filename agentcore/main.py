from dotenv import load_dotenv
load_dotenv()

import atexit
import json
import logging
import os
from urllib.parse import quote_plus

logger = logging.getLogger(__name__)


def _resolve_database_url() -> None:
    """DATABASE_URL を解決する。ローカルは環境変数、CDK は Secrets Manager。"""
    secret_arn = os.environ.get("DB_SECRET_ARN")

    # DB_SECRET_ARN が設定されている場合は常に Secrets Manager から解決する
    # (.env の DATABASE_URL よりも優先)
    if not secret_arn:
        if os.environ.get("DATABASE_URL"):
            return
        raise RuntimeError("DATABASE_URL or DB_SECRET_ARN must be set")

    import boto3

    region = os.environ.get("AWS_REGION", "ap-northeast-1")
    client = boto3.client("secretsmanager", region_name=region)
    response = client.get_secret_value(SecretId=secret_arn)
    secret = json.loads(response["SecretString"])

    host = os.environ.get("DB_HOST", secret.get("host", "localhost"))
    port = secret.get("port", 5432)
    username = quote_plus(secret["username"])
    password = quote_plus(secret["password"])
    dbname = os.environ.get("DB_NAME", secret.get("dbname", "myfriend"))

    os.environ["DATABASE_URL"] = (
        f"postgresql://{username}:{password}@{host}:{port}/{dbname}"
    )
    os.environ["DB_USE_SSL"] = "true"
    logger.info("DATABASE_URL resolved from Secrets Manager (host=%s)", host)


_resolve_database_url()

from bedrock_agentcore.runtime import BedrockAgentCoreApp

from core import validate_bank_id, stream_agent, shutdown_sync

app = BedrockAgentCoreApp()

AGENT_MODEL_ID = os.environ.get("AGENT_MODEL_ID", "anthropic.claude-3-5-sonnet-20240620-v1:0")

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

    history = payload.get("messages", [])

    async for chunk in stream_agent(bank_id, str(prompt).strip(), AGENT_MODEL_ID, history):
        yield chunk


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    app.run()
