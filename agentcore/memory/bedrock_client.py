"""Bedrock Runtime クライアントの共有シングルトン

extraction.py と embedding.py から共通利用される。
スレッドセーフな初期化パターンを提供する。
"""

import os
import threading

import boto3

_client = None
_lock = threading.Lock()


def get_bedrock_runtime_client():
    """Bedrock Runtime クライアントを取得する（スレッドセーフ）"""
    global _client
    if _client is None:
        with _lock:
            if _client is None:
                _client = boto3.client(
                    "bedrock-runtime",
                    region_name=os.environ.get("AWS_REGION", "ap-northeast-1"),
                )
    return _client
