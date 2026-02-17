import { promisify } from "util";
import { pipeline as streamPipeline, Readable, Writable } from "stream";
import { randomUUID } from "crypto";
import type { APIGatewayProxyEvent, Context } from "aws-lambda";
import {
  BedrockAgentCoreClient,
  InvokeAgentRuntimeCommand,
} from "@aws-sdk/client-bedrock-agentcore";

// 環境変数からAgentCoreのARNを取得
const agentCoreArn = process.env.AGENT_RUNTIME_ARN;
if (!agentCoreArn) {
  throw new Error("AGENT_ARN must be set in environment variables");
}

// ストリームパイプライン処理をPromise化
const asyncPipeline = promisify(streamPipeline);

// BedrockAgentCoreクライアントの初期化
const agentCoreClient = new BedrockAgentCoreClient({
  region: process.env.AWS_REGION || "us-west-2",
});

// クライアントリクエストの構造定義
interface ClientRequest {
  sessionId?: string;
  prompt: string;
}

// Lambda Response Streaming用の型定義
type StreamHandler = (
  event: APIGatewayProxyEvent,
  responseStream: NodeJS.WritableStream,
  context: Context,
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
      },
    ) => NodeJS.WritableStream;
  };
};

export const handler = awslambda.streamifyResponse(
  async (
    event: APIGatewayProxyEvent,
    responseStream: NodeJS.WritableStream,
    context: Context,
  ) => {
    console.log("Incoming request:", JSON.stringify(event, null, 2));

    try {
      // リクエストボディからパラメータを解析
      const requestParams = parseClientRequest(event);
      console.log("Parsed request parameters:", requestParams);

      // Server-Sent Events形式でレスポンスを返す設定
      const responseMetadata = {
        statusCode: 200,
        headers: {
          "Content-Type": "text/event-stream",
          "Cache-Control": "no-cache",
          "X-Accel-Buffering": "no",
        },
      };

      const httpStream = awslambda.HttpResponseStream.from(
        responseStream as Writable,
        responseMetadata,
      );

      // AgentCore Runtimeへのリクエストを構築
      const invokeCommand = new InvokeAgentRuntimeCommand({
        agentRuntimeArn: agentCoreArn,
        runtimeSessionId: requestParams.sessionId,
        payload: new TextEncoder().encode(
          JSON.stringify({
            prompt: requestParams.prompt,
          }),
        ),
        qualifier: "DEFAULT",
      });

      console.log("Calling AgentCore Runtime...");

      // AgentCoreを実行してレスポンスを取得
      const runtimeResponse = await agentCoreClient.send(invokeCommand);

      console.log("Received response, initiating stream transfer...");

      // レスポンスストリームをクライアントへパイプライン接続
      await asyncPipeline(runtimeResponse.response as Readable, httpStream);

      console.log("Stream transfer completed");
    } catch (error) {
      console.error("Request processing failed:", error);

      // エラー情報をストリームに書き込み
      try {
        responseStream.write('data: {"error": "Internal server error"}\n\n');
        if (error instanceof Error) {
          responseStream.write(`data: {"error": "${error.message}"}\n\n`);
        }
      } catch (streamError) {
        console.error("Error stream write failed:", streamError);
      } finally {
        responseStream.end();
      }
    }
  },
);

/**
 * リクエストイベントからクライアントパラメータを抽出
 */
const parseClientRequest = (event: APIGatewayProxyEvent): ClientRequest => {
  let params: ClientRequest;

  // POSTリクエストでBase64エンコードされている場合
  if (event.isBase64Encoded && event.body) {
    const decodedBody = Buffer.from(event.body, "base64").toString("utf-8");
    params = JSON.parse(decodedBody);
  }
  // POSTリクエストで通常のJSON
  else if (event.body) {
    params = JSON.parse(event.body);
  }
  // GETリクエストのクエリパラメータ
  else if (event.queryStringParameters?.prompt) {
    params = {
      prompt: event.queryStringParameters.prompt,
      sessionId: event.queryStringParameters.sessionId,
    };
  }
  // デフォルトのプロンプト
  else {
    params = { prompt: "こんにちは、元気ですか？" };
  }

  // セッションIDが未指定の場合は新規生成
  params.sessionId = params.sessionId || randomUUID();

  return params;
};
