import * as path from 'path';
import * as cdk from 'aws-cdk-lib';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import { Platform } from 'aws-cdk-lib/aws-ecr-assets';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as nodejs from 'aws-cdk-lib/aws-lambda-nodejs';
import { Construct } from 'constructs';
import { IBedrockAgentRuntime } from '@aws-cdk/aws-bedrock-agentcore-alpha';

interface Props {
  agentCoreRuntime: IBedrockAgentRuntime;
}

export class ProxyLambda extends Construct {
  public readonly function: lambda.IFunction;
  public readonly functionUrl: lambda.FunctionUrl;

  constructor(scope: Construct, id: string, props: Props) {
    super(scope, id);

    const { agentCoreRuntime } = props;

    this.function = new nodejs.NodejsFunction(this, 'AgentCoreProxyFunction', {
      functionName: 'agentcore-proxy',
      entry: path.join(__dirname, '../../resources/proxy-lambda/index.ts'),
      handler: 'handler',
      runtime: lambda.Runtime.NODEJS_24_X,
      timeout: cdk.Duration.minutes(15),
      memorySize: 512,
      environment: {
        AGENT_RUNTIME_ARN: agentCoreRuntime.agentRuntimeArn,
      },
    });

    // AmazonBedrockFullAccess マネージドポリシーを付与
    this.function.role?.addManagedPolicy(
      iam.ManagedPolicy.fromAwsManagedPolicyName('AmazonBedrockFullAccess')
    );

    // BedrockAgentCoreFullAccess マネージドポリシーを付与
    this.function.role?.addManagedPolicy(
      iam.ManagedPolicy.fromAwsManagedPolicyName('BedrockAgentCoreFullAccess')
    );
  }
}
