import * as path from 'path';
import type { Construct } from 'constructs';
import * as cdk from 'aws-cdk-lib';
import type * as ec2 from 'aws-cdk-lib/aws-ec2';
import type * as secretsmanager from 'aws-cdk-lib/aws-secretsmanager';
import * as ssm from 'aws-cdk-lib/aws-ssm';

import type { ParameterType } from '../parameter';
import { Batch } from './constructs/batch';
import { Database } from './constructs/database';
import { Migration } from './constructs/migration';
import { Network } from './constructs/network';
import { Restore } from './constructs/restore';

interface MemoryStackProps extends cdk.StackProps {
  readonly parameter: ParameterType;
}

/**
 * MemoryStack は Aurora を中心とした記憶システム一式を管理する。
 * MainStack から分離することで、デプロイ/削除を独立して行える。
 *
 * 連携は SSM Parameter Store 経由。MemoryStack が未デプロイの場合、
 * MainStack は AgentCore を Public モードで起動し、記憶機能は無効化される。
 */
export class MemoryStack extends cdk.Stack {
  public readonly vpc: ec2.IVpc;
  public readonly privateSubnets: ec2.ISubnet[];
  public readonly auroraSecurityGroup: ec2.ISecurityGroup;
  public readonly dbSecret: secretsmanager.ISecret;
  public readonly dbHost: string;
  public readonly databaseName: string;

  constructor(scope: Construct, id: string, props: MemoryStackProps) {
    super(scope, id, props);
    const { parameter } = props;

    // 1. Network（VPC + SG + S3 Gateway）
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
      prefix: parameter.prefix,
      vpc: network.vpc,
      isolatedSubnets: network.isolatedSubnets,
      auroraSecurityGroup: network.sgAurora,
      minAcu: parameter.diffEnv.aurora.minAcu,
      maxAcu: parameter.diffEnv.aurora.maxAcu,
      autoPauseMinutes: parameter.diffEnv.aurora.autoPauseMinutes,
      backupRetentionDays: parameter.diffEnv.aurora.backupRetentionDays,
      deletionProtection: parameter.diffEnv.aurora.deletionProtection,
      removalPolicy: parameter.diffEnv.aurora.removalPolicy,
    });

    // 3. Migration（Custom Resource → SQL 実行）
    new Migration(this, 'Migration', {
      prefix: parameter.prefix,
      vpc: network.vpc,
      lambdaSecurityGroup: network.sgLambda,
      isolatedSubnets: network.privateSubnets,
      dbSecret: database.secret,
      cluster: database.cluster,
      databaseName: database.databaseName,
    });

    // 4. Restore（S3 + Lambda → DB リストア）
    new Restore(this, 'Restore', {
      prefix: parameter.prefix,
      vpc: network.vpc,
      lambdaSecurityGroup: network.sgLambda,
      isolatedSubnets: network.privateSubnets,
      dbSecret: database.secret,
      cluster: database.cluster,
      databaseName: database.databaseName,
      backupDir: path.join(__dirname, '../../postgresql/backups'),
    });

    // 5. Batch（Lambda + EventBridge）
    new Batch(this, 'Batch', {
      prefix: parameter.prefix,
      vpc: network.vpc,
      lambdaSecurityGroup: network.sgLambda,
      isolatedSubnets: network.privateSubnets,
      dbSecret: database.secret,
      dbHost: database.clusterEndpoint,
      databaseName: database.databaseName,
      scheduleMinutes: parameter.diffEnv.batch.scheduleMinutes,
      timeoutSeconds: parameter.diffEnv.batch.timeoutSeconds,
      memoryMb: parameter.diffEnv.batch.memoryMb,
      enabled: parameter.diffEnv.batch.enabled,
    });

    // 公開プロパティ（MainStack から参照用）
    this.vpc = network.vpc;
    this.auroraSecurityGroup = network.sgAurora;
    this.dbSecret = database.secret;
    this.dbHost = database.clusterEndpoint;
    this.databaseName = database.databaseName;

    // AgentCore 用のサブネット（非対応 AZ を除外）
    const excludeAzs = parameter.diffEnv.vpc.agentCoreExcludeAzs;
    this.privateSubnets = excludeAzs
      ? network.privateSubnets.filter(s => !excludeAzs.includes(s.availabilityZone))
      : network.privateSubnets;

    // SSM Parameter Store にエクスポート（運用参照用）
    const ssmPrefix = `/myfriend/${parameter.prefix}/memory`;
    new ssm.StringParameter(this, 'SsmDbHost', {
      parameterName: `${ssmPrefix}/db-host`,
      stringValue: this.dbHost,
    });
    new ssm.StringParameter(this, 'SsmDbSecretArn', {
      parameterName: `${ssmPrefix}/db-secret-arn`,
      stringValue: this.dbSecret.secretArn,
    });
    new ssm.StringParameter(this, 'SsmDbName', {
      parameterName: `${ssmPrefix}/db-name`,
      stringValue: this.databaseName,
    });
  }
}
