import { promisify } from 'util';
import { pipeline as streamPipeline, Readable, Writable } from 'stream';
import { randomUUID } from 'crypto';
import type { APIGatewayProxyEvent, Context } from 'aws-lambda';
import {
  BedrockAgentCoreClient,
  InvokeAgentRuntimeCommand,
} from '@aws-sdk/client-bedrock-agentcore';

// 環境変数からAgentCoreのARNを取得
const agentCoreArn = process.env.AGENT_RUNTIME_ARN;
if (!agentCoreArn) {
  throw new Error('AGENT_RUNTIME_ARN must be set in environment variables');
}

// ストリームパイプライン処理をPromise化
const asyncPipeline = promisify(streamPipeline);

// BedrockAgentCoreクライアントの初期化
const agentCoreClient = new BedrockAgentCoreClient({
  region: process.env.AWS_REGION || 'ap-northeast-1',
});

// UUID バリデーション
const UUID_REGEX =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

// クライアントリクエストの構造定義
interface ClientRequest {
  sessionId?: string;
  prompt: string;
  bank_id: string;
}

// Lambda Response Streaming用の型定義
type StreamHandler = (
  event: APIGatewayProxyEvent,
  responseStream: NodeJS.WritableStream,
  context: Context
) => Promise<void>;

// awslambdaグローバルオブジェクトの型定義
declare const awslambda: {
  streamifyResponse: (handler: StreamHandler) => StreamHandler;
  HttpResponseStream: {
    from: (
      stream: NodeJS.WritableStream,
      metadata: {
        statusCode: number;
        headers: Record<string, string>;
      }
    ) => NodeJS.WritableStream;
  };
};

export const handler = awslambda.streamifyResponse(
  async (
    event: APIGatewayProxyEvent,
    responseStream: NodeJS.WritableStream,
    context: Context
  ) => {
    try {
      // リクエストボディからパラメータを解析
      const requestParams = parseClientRequest(event);

      // Server-Sent Events形式でレスポンスを返す設定
      const responseMetadata = {
        statusCode: 200,
        headers: {
          'Content-Type': 'text/event-stream',
          'Cache-Control': 'no-cache',
          'X-Accel-Buffering': 'no',
        },
      };

      const httpStream = awslambda.HttpResponseStream.from(
        responseStream as Writable,
        responseMetadata
      );

      // AgentCore Runtimeへのリクエストを構築
      const invokeCommand = new InvokeAgentRuntimeCommand({
        agentRuntimeArn: agentCoreArn,
        runtimeSessionId: requestParams.sessionId,
        payload: new TextEncoder().encode(
          JSON.stringify({
            prompt: requestParams.prompt,
            bank_id: requestParams.bank_id,
          })
        ),
        qualifier: 'DEFAULT',
      });

      // AgentCoreを実行してレスポンスを取得
      const runtimeResponse = await agentCoreClient.send(invokeCommand);

      // レスポンスストリームをクライアントへパイプライン接続
      await asyncPipeline(runtimeResponse.response as Readable, httpStream);
    } catch (error) {
      // エラー情報をストリームに書き込み（JSON.stringify で安全にエスケープ）
      try {
        const safeMessage =
          error instanceof Error
            ? error.message.slice(0, 200)
            : 'Internal server error';
        responseStream.write(
          `data: ${JSON.stringify({ error: safeMessage })}\n\n`
        );
      } catch (_streamError) {
        // stream write failed, nothing more we can do
      } finally {
        responseStream.end();
      }
    }
  }
);

/**
 * リクエストイベントからクライアントパラメータを抽出
 */
const parseClientRequest = (event: APIGatewayProxyEvent): ClientRequest => {
  let params: Partial<ClientRequest>;

  // POSTリクエストでBase64エンコードされている場合
  if (event.isBase64Encoded && event.body) {
    try {
      const decodedBody = Buffer.from(event.body, 'base64').toString('utf-8');
      params = JSON.parse(decodedBody);
    } catch {
      throw new Error('Invalid JSON in request body');
    }
  }
  // POSTリクエストで通常のJSON
  else if (event.body) {
    try {
      params = JSON.parse(event.body);
    } catch {
      throw new Error('Invalid JSON in request body');
    }
  }
  // GETリクエストのクエリパラメータ
  else if (event.queryStringParameters?.prompt) {
    params = {
      prompt: event.queryStringParameters.prompt,
      sessionId: event.queryStringParameters.sessionId,
      bank_id: event.queryStringParameters.bank_id,
    };
  }
  // デフォルト
  else {
    throw new Error('No prompt provided');
  }

  // bank_id バリデーション
  if (!params.bank_id || !UUID_REGEX.test(params.bank_id)) {
    throw new Error('Valid bank_id (UUID) is required');
  }

  // prompt バリデーション
  if (!params.prompt || typeof params.prompt !== 'string') {
    throw new Error('prompt is required');
  }

  const MAX_PROMPT_LENGTH = 10000;
  if (params.prompt.length > MAX_PROMPT_LENGTH) {
    throw new Error(`prompt must be ${MAX_PROMPT_LENGTH} characters or less`);
  }

  // sessionId バリデーション
  if (params.sessionId) {
    if (!UUID_REGEX.test(params.sessionId)) {
      throw new Error('sessionId must be a valid UUID');
    }
  } else {
    params.sessionId = randomUUID();
  }

  return params as ClientRequest;
};
