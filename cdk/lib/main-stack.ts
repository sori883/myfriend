import type { Construct } from 'constructs';
import * as cdk from 'aws-cdk-lib';
import type * as ec2 from 'aws-cdk-lib/aws-ec2';
import type * as secretsmanager from 'aws-cdk-lib/aws-secretsmanager';

import type { ParameterType } from '../parameter';
import { AgentCore } from './constructs/agentcore';
import { ApiGateway } from './constructs/api-gateway';
import { ProxyLambda } from './constructs/proxy-lambda';

interface MemoryRefs {
  readonly vpc: ec2.IVpc;
  readonly privateSubnets: ec2.ISubnet[];
  readonly auroraSecurityGroup: ec2.ISecurityGroup;
  readonly dbSecret: secretsmanager.ISecret;
  readonly dbHost: string;
  readonly databaseName: string;
}

interface StackProps extends cdk.StackProps {
  readonly parameter: ParameterType;
  /**
   * MemoryStack が存在する場合のみ渡す。未指定なら AgentCore は Public モードで動作。
   */
  readonly memory?: MemoryRefs;
}

export class MainStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props: StackProps) {
    super(scope, id, props);
    const { parameter, memory } = props;

    // AgentCore（memory 有無で network mode を切替）
    const agentCore = new AgentCore(this, 'AgentCore', {
      prefix: parameter.prefix,
      runtimeEnv: {
        AGENT_MODEL_ID: parameter.dotEnv.AGENT_MODEL_ID,
        EXTRACTION_MODEL_ID: parameter.dotEnv.EXTRACTION_MODEL_ID,
        EMBEDDING_MODEL_ID: parameter.dotEnv.EMBEDDING_MODEL_ID,
        RERANK_MODEL_ID: parameter.dotEnv.RERANK_MODEL_ID,
        REFLECT_MODEL_ID: parameter.dotEnv.REFLECT_MODEL_ID,
        PREFERENCE_MODEL_ID: parameter.dotEnv.PREFERENCE_MODEL_ID,
        TAVILY_API_KEY: parameter.dotEnv.TAVILY_API_KEY,
      },
      memory,
    });

    // ProxyLambda（AgentCore ARN 参照）
    const proxyLambda = new ProxyLambda(this, 'ProxyLambda', {
      prefix: parameter.prefix,
      agentCoreRuntime: agentCore.agentCoreRuntime,
    });

    // ApiGateway（ProxyLambda 参照 + API Key 認証）
    const apiGateway = new ApiGateway(this, 'APIGateway', {
      prefix: parameter.prefix,
      lambdaFunction: proxyLambda.function,
      dailyQuota: parameter.diffEnv.api.dailyQuota,
    });

    // API Key ID を出力（値の取得: aws apigateway get-api-key --api-key <id> --include-value）
    new cdk.CfnOutput(this, 'ApiKeyId', {
      value: apiGateway.apiKey.keyId,
    });

    // memory 連携状態を出力（診断用）
    new cdk.CfnOutput(this, 'MemoryEnabled', {
      value: memory ? 'true' : 'false',
    });
  }
}
