import * as cdk from 'aws-cdk-lib';
import type * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as rds from 'aws-cdk-lib/aws-rds';
import * as secretsmanager from 'aws-cdk-lib/aws-secretsmanager';
import { Construct } from 'constructs';

interface Props {
  readonly prefix: string;
  readonly vpc: ec2.IVpc;
  readonly isolatedSubnets: ec2.ISubnet[];
  readonly auroraSecurityGroup: ec2.ISecurityGroup;
  readonly minAcu: number;
  readonly maxAcu: number;
  readonly autoPauseMinutes?: number;
  readonly backupRetentionDays: number;
  readonly deletionProtection: boolean;
  readonly removalPolicy: cdk.RemovalPolicy;
}

export class Database extends Construct {
  public readonly cluster: rds.IDatabaseCluster;
  public readonly secret: secretsmanager.ISecret;
  public readonly clusterEndpoint: string;
  public readonly databaseName: string;

  constructor(scope: Construct, id: string, props: Props) {
    super(scope, id);
    const {
      prefix,
      vpc,
      isolatedSubnets,
      auroraSecurityGroup,
      minAcu,
      maxAcu,
      autoPauseMinutes,
      backupRetentionDays,
      deletionProtection,
      removalPolicy,
    } = props;

    this.databaseName = 'myfriend';

    // Secrets Manager でDB認証情報を管理
    this.secret = new secretsmanager.Secret(this, 'DatabaseSecret', {
      secretName: `${prefix}-myfriend/aurora/credentials`,
      generateSecretString: {
        secretStringTemplate: JSON.stringify({ username: 'myfriend_admin' }),
        generateStringKey: 'password',
        excludePunctuation: true,
        passwordLength: 30,
      },
    });

    // Aurora Serverless v2 クラスター（最小構成）
    const cluster = new rds.DatabaseCluster(this, 'AuroraCluster', {
      engine: rds.DatabaseClusterEngine.auroraPostgres({
        version: rds.AuroraPostgresEngineVersion.VER_16_4,
      }),
      serverlessV2MinCapacity: minAcu,
      serverlessV2MaxCapacity: maxAcu,
      serverlessV2AutoPauseDuration:
        minAcu === 0 && autoPauseMinutes
          ? cdk.Duration.minutes(autoPauseMinutes)
          : undefined,
      credentials: rds.Credentials.fromSecret(this.secret),
      defaultDatabaseName: this.databaseName,
      vpc,
      vpcSubnets: { subnets: isolatedSubnets },
      securityGroups: [auroraSecurityGroup],
      writer: rds.ClusterInstance.serverlessV2('writer', {
        publiclyAccessible: false,
      }),
      backup: {
        retention: cdk.Duration.days(backupRetentionDays),
      },
      deletionProtection,
      removalPolicy,
      storageEncrypted: true,
    });
    this.cluster = cluster;

    this.clusterEndpoint = cluster.clusterEndpoint.hostname;
  }
}
