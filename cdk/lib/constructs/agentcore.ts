import * as path from 'path';
import * as cdk from 'aws-cdk-lib';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import type * as secretsmanager from 'aws-cdk-lib/aws-secretsmanager';
import { Construct } from 'constructs';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as agentcore from '@aws-cdk/aws-bedrock-agentcore-alpha';

interface MemoryConfig {
  readonly vpc: ec2.IVpc;
  readonly privateSubnets: ec2.ISubnet[];
  readonly auroraSecurityGroup: ec2.ISecurityGroup;
  readonly dbSecret: secretsmanager.ISecret;
  readonly dbHost: string;
  readonly databaseName: string;
}

interface Props {
  readonly prefix: string;
  readonly runtimeEnv: Record<string, string>;
  /**
   * 記憶システム連携設定。未指定の場合は AgentCore を Public モードで起動し、
   * DB 接続情報を渡さない（アプリ側が記憶機能を自動的に無効化する）。
   */
  readonly memory?: MemoryConfig;
}

export class AgentCore extends Construct {
  public readonly agentCoreRuntime: agentcore.Runtime;

  constructor(scope: Construct, id: string, props: Props) {
    super(scope, id);
    const { prefix, runtimeEnv, memory } = props;

    // AgentCore Runtime Artifact（ビルドコンテキスト: リポジトリルート）
    const artifact = agentcore.AgentRuntimeArtifact.fromAsset(
      path.join(__dirname, '../../..'),
      {
        file: 'agentcore/Dockerfile',
        exclude: [
          'cdk/cdk.out',
          'cdk/node_modules',
          'front',
          '.git',
          'docs',
          'postgresql',
          'agentcore/.venv',
          'agentcore/.env.local',
          'memory/.venv',
          'recommendation/.venv',
          'batch/.venv',
        ],
      }
    );

    // memory 有無で network mode と環境変数を切り替える
    const dbEnv: Record<string, string> = memory
      ? {
          DB_SECRET_ARN: memory.dbSecret.secretArn,
          DB_HOST: memory.dbHost,
          DB_NAME: memory.databaseName,
        }
      : {};

    this.agentCoreRuntime = new agentcore.Runtime(this, 'Agent', {
      // AgentCore runtime名はハイフン非対応のためスネークケースを使用
      runtimeName: `${prefix}_myfriend_agent`,
      agentRuntimeArtifact: artifact,
      description: 'Myfriend AI agent with memory system',
      ...(memory && {
        networkConfiguration: agentcore.RuntimeNetworkConfiguration.usingVpc(this, {
          vpc: memory.vpc as ec2.Vpc,
          vpcSubnets: { subnets: memory.privateSubnets },
        }),
      }),
      environmentVariables: {
        AWS_REGION: cdk.Stack.of(this).region,
        ...dbEnv,
        ...runtimeEnv,
      },
    });

    if (memory) {
      // AgentCore → Aurora (PostgreSQL 5432) の接続を許可
      this.agentCoreRuntime.connections.allowTo(
        memory.auroraSecurityGroup as ec2.SecurityGroup,
        ec2.Port.tcp(5432),
        'Allow AgentCore to Aurora PostgreSQL'
      );

      // Secrets Manager へのアクセス権限
      memory.dbSecret.grantRead(this.agentCoreRuntime);
    }

    // Bedrock フルアクセス権限（記憶有無に関わらず必須）
    this.agentCoreRuntime.addToRolePolicy(
      new iam.PolicyStatement({
        effect: iam.Effect.ALLOW,
        actions: ['bedrock:*'],
        resources: ['*'],
      })
    );
  }
}
