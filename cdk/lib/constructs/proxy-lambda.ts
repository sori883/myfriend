import * as path from 'path';
import * as cdk from 'aws-cdk-lib';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as nodejs from 'aws-cdk-lib/aws-lambda-nodejs';
import { Construct } from 'constructs';

import type { Runtime as AgentCoreRuntime } from '@aws-cdk/aws-bedrock-agentcore-alpha';

interface Props {
  readonly agentCoreRuntime: AgentCoreRuntime;
}

export class ProxyLambda extends Construct {
  public readonly function: lambda.IFunction;

  constructor(scope: Construct, id: string, props: Props) {
    super(scope, id);

    const { agentCoreRuntime } = props;

    this.function = new nodejs.NodejsFunction(this, 'AgentCoreProxyFunction', {
      functionName: 'myfriend-agentcore-proxy',
      entry: path.join(__dirname, '../../resources/proxy-lambda/index.ts'),
      handler: 'handler',
      runtime: lambda.Runtime.NODEJS_24_X,
      timeout: cdk.Duration.minutes(15),
      memorySize: 512,
      environment: {
        AGENT_RUNTIME_ARN: agentCoreRuntime.agentRuntimeArn,
      },
    });

    // AgentCore Runtime の呼び出し権限（runtime + runtime-endpoint を自動設定）
    agentCoreRuntime.grantInvoke(this.function);
  }
}
