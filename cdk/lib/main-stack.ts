import * as path from 'path';
import type { Construct } from 'constructs';
import * as cdk from 'aws-cdk-lib';

import type { ParameterType } from '../parameter';
import { AgentCore } from './constructs/agentcore';
import { ApiGateway } from './constructs/api-gateway';
import { Batch } from './constructs/batch';
import { Database } from './constructs/database';
import { Migration } from './constructs/migration';
import { Network } from './constructs/network';
import { ProxyLambda } from './constructs/proxy-lambda';
import { Restore } from './constructs/restore';

interface StackProps extends cdk.StackProps {
  readonly parameter: ParameterType;
}

export class MainStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props: StackProps) {
    super(scope, id, props);
    const { parameter } = props;

    // 1. Network（VPC + SG + VPC Endpoints）
    const network = new Network(this, 'Network', {
      publicNats: parameter.diffEnv.vpc.publicNats,
      cidr: parameter.diffEnv.vpc.cidr,
      maxAzs: parameter.diffEnv.vpc.maxAzs,
      subnetConfigs: Object.values(parameter.diffEnv.vpc.subnets),
      subnetSelectionName: parameter.diffEnv.vpc.subnets.Private1.name,
      egressSubnetName: parameter.diffEnv.vpc.subnets.Private2.name,
    });

    // 2. Database（Aurora Serverless v2 + Secrets Manager）
    const database = new Database(this, 'Database', {
      vpc: network.vpc,
      isolatedSubnets: network.isolatedSubnets,
      auroraSecurityGroup: network.sgAurora,
      minAcu: parameter.diffEnv.aurora.minAcu,
      maxAcu: parameter.diffEnv.aurora.maxAcu,
      backupRetentionDays: parameter.diffEnv.aurora.backupRetentionDays,
      deletionProtection: parameter.diffEnv.aurora.deletionProtection,
      removalPolicy: parameter.diffEnv.aurora.removalPolicy,
    });

    // 3. Migration（Custom Resource → SQL 実行）
    new Migration(this, 'Migration', {
      vpc: network.vpc,
      lambdaSecurityGroup: network.sgLambda,
      isolatedSubnets: network.isolatedSubnets,
      dbSecret: database.secret,
      cluster: database.cluster,
      databaseName: database.databaseName,
    });

    // 4. Restore（S3 + Lambda → DB リストア）
    new Restore(this, 'Restore', {
      vpc: network.vpc,
      lambdaSecurityGroup: network.sgLambda,
      isolatedSubnets: network.isolatedSubnets,
      dbSecret: database.secret,
      cluster: database.cluster,
      databaseName: database.databaseName,
      backupDir: path.join(__dirname, '../../postgresql/backups'),
    });

    // 5. AgentCore（Runtime + Bedrock 権限 + DB 権限 + VPC モード）
    const agentCore = new AgentCore(this, 'AgentCore', {
      vpc: network.vpc,
      privateSubnets: network.privateSubnets,
      auroraSecurityGroup: network.sgAurora,
      dbSecret: database.secret,
      dbHost: database.clusterEndpoint,
      databaseName: database.databaseName,
      runtimeEnv: {
        AGENT_MODEL_ID: parameter.dotEnv.AGENT_MODEL_ID,
        EXTRACTION_MODEL_ID: parameter.dotEnv.EXTRACTION_MODEL_ID,
        EMBEDDING_MODEL_ID: parameter.dotEnv.EMBEDDING_MODEL_ID,
        RERANK_MODEL_ID: parameter.dotEnv.RERANK_MODEL_ID,
        REFLECT_MODEL_ID: parameter.dotEnv.REFLECT_MODEL_ID,
        PREFERENCE_MODEL_ID: parameter.dotEnv.PREFERENCE_MODEL_ID,
        TAVILY_API_KEY: parameter.dotEnv.TAVILY_API_KEY,
      },
    });

    // 6. ProxyLambda（AgentCore ARN 参照）
    const proxyLambda = new ProxyLambda(this, 'ProxyLambda', {
      agentCoreRuntime: agentCore.agentCoreRuntime,
    });

    // 7. ApiGateway（ProxyLambda 参照 + API Key 認証）
    const apiGateway = new ApiGateway(this, 'APIGateway', {
      lambdaFunction: proxyLambda.function,
      dailyQuota: parameter.diffEnv.api.dailyQuota,
    });

    // API Key ID を出力（値の取得: aws apigateway get-api-key --api-key <id> --include-value）
    new cdk.CfnOutput(this, 'ApiKeyId', {
      value: apiGateway.apiKey.keyId,
    });

    // 8. Batch（Lambda + EventBridge）
    new Batch(this, 'Batch', {
      vpc: network.vpc,
      lambdaSecurityGroup: network.sgLambda,
      isolatedSubnets: network.isolatedSubnets,
      dbSecret: database.secret,
      dbHost: database.clusterEndpoint,
      databaseName: database.databaseName,
      scheduleMinutes: parameter.diffEnv.batch.scheduleMinutes,
      timeoutSeconds: parameter.diffEnv.batch.timeoutSeconds,
      memoryMb: parameter.diffEnv.batch.memoryMb,
      enabled: parameter.diffEnv.batch.enabled,
    });
  }
}
