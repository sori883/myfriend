import * as path from 'path';
import * as cdk from 'aws-cdk-lib';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import type * as secretsmanager from 'aws-cdk-lib/aws-secretsmanager';
import { Construct } from 'constructs';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as agentcore from '@aws-cdk/aws-bedrock-agentcore-alpha';

interface Props {
  readonly vpc: ec2.IVpc;
  readonly privateSubnets: ec2.ISubnet[];
  readonly auroraSecurityGroup: ec2.ISecurityGroup;
  readonly dbSecret: secretsmanager.ISecret;
  readonly dbHost: string;
  readonly databaseName: string;
  readonly runtimeEnv: Record<string, string>;
}

export class AgentCore extends Construct {
  public readonly agentCoreRuntime: agentcore.Runtime;

  constructor(scope: Construct, id: string, props: Props) {
    super(scope, id);
    const { vpc, privateSubnets, auroraSecurityGroup, dbSecret, dbHost, databaseName, runtimeEnv } = props;

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

    // AgentCore Runtime（VPC モード: Aurora アクセス用）
    this.agentCoreRuntime = new agentcore.Runtime(this, 'Agent', {
      runtimeName: 'myfriend_agent',
      agentRuntimeArtifact: artifact,
      description: 'Myfriend AI agent with memory system',
      networkConfiguration: agentcore.RuntimeNetworkConfiguration.usingVpc(this, {
        vpc: vpc as ec2.Vpc,
        vpcSubnets: { subnets: privateSubnets },
      }),
      environmentVariables: {
        AWS_REGION: cdk.Stack.of(this).region,
        DB_SECRET_ARN: dbSecret.secretArn,
        DB_HOST: dbHost,
        DB_NAME: databaseName,
        ...runtimeEnv,
      },
    });

    // AgentCore → Aurora (PostgreSQL 5432) の接続を許可
    this.agentCoreRuntime.connections.allowTo(
      auroraSecurityGroup as ec2.SecurityGroup,
      ec2.Port.tcp(5432),
      'Allow AgentCore to Aurora PostgreSQL'
    );

    // Secrets Manager へのアクセス権限
    dbSecret.grantRead(this.agentCoreRuntime);

    // Bedrock フルアクセス権限
    this.agentCoreRuntime.addToRolePolicy(
      new iam.PolicyStatement({
        effect: iam.Effect.ALLOW,
        actions: ['bedrock:*'],
        resources: ['*'],
      })
    );
  }
}
