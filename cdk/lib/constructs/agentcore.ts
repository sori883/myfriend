import * as path from 'path';
import { Asset } from 'aws-cdk-lib/aws-s3-assets';
import { Construct } from 'constructs';
import * as agentcore from '@aws-cdk/aws-bedrock-agentcore-alpha';
import { BedrockFoundationModel } from '@aws-cdk/aws-bedrock-alpha';

export class AgentCore extends Construct {
  public readonly agentCoreRuntime: agentcore.IBedrockAgentRuntime;

  constructor(scope: Construct, id: string) {
    super(scope, id);

    // AgentCore Runtime Artifact
    const artifact = agentcore.AgentRuntimeArtifact.fromAsset(
      path.join(__dirname, '../../../agentcore')
    );

    // AgentCore Runtimeを定義
    this.agentCoreRuntime = new agentcore.Runtime(this, 'agent', {
      runtimeName: 'sample_agent',
      agentRuntimeArtifact: artifact,
      description: 'Sample agent',
    });

    // AgentCore Runtimeに対してBedrockへのアクセス権を付与
    const bedrockModel = BedrockFoundationModel.fromCdkFoundationModelId({
      modelId: 'openai.gpt-oss-120b-1:0',
    });
    bedrockModel.grantInvoke(this.agentCoreRuntime);
  }
}
