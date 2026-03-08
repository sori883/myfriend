import * as fs from 'fs';
import * as path from 'path';
import * as cdk from 'aws-cdk-lib';
import type * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as nodejs from 'aws-cdk-lib/aws-lambda-nodejs';
import type * as rds from 'aws-cdk-lib/aws-rds';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as s3deploy from 'aws-cdk-lib/aws-s3-deployment';
import type * as secretsmanager from 'aws-cdk-lib/aws-secretsmanager';
import { Construct } from 'constructs';

interface Props {
  readonly vpc: ec2.IVpc;
  readonly lambdaSecurityGroup: ec2.ISecurityGroup;
  readonly isolatedSubnets: ec2.ISubnet[];
  readonly dbSecret: secretsmanager.ISecret;
  readonly cluster: rds.IDatabaseCluster;
  readonly databaseName: string;
  readonly backupDir?: string;
}

export class Restore extends Construct {
  public readonly bucket: s3.IBucket;
  public readonly function: lambda.IFunction;

  constructor(scope: Construct, id: string, props: Props) {
    super(scope, id);
    const {
      vpc,
      lambdaSecurityGroup,
      isolatedSubnets,
      dbSecret,
      cluster,
      databaseName,
      backupDir,
    } = props;

    // リストア用 S3 バケット
    this.bucket = new s3.Bucket(this, 'RestoreBucket', {
      bucketName: `myfriend-db-restore-${cdk.Stack.of(this).account}`,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      autoDeleteObjects: true,
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      enforceSSL: true,
      encryption: s3.BucketEncryption.S3_MANAGED,
      lifecycleRules: [
        {
          expiration: cdk.Duration.days(7),
        },
      ],
    });

    // リストア Lambda
    const restoreFunction = new nodejs.NodejsFunction(
      this,
      'RestoreFunction',
      {
        functionName: 'myfriend-db-restore',
        entry: path.join(
          __dirname,
          '../../resources/restore/index.ts'
        ),
        handler: 'handler',
        runtime: lambda.Runtime.NODEJS_24_X,
        timeout: cdk.Duration.minutes(15),
        memorySize: 512,
        vpc,
        vpcSubnets: { subnets: isolatedSubnets },
        securityGroups: [lambdaSecurityGroup],
        environment: {
          DB_SECRET_ARN: dbSecret.secretArn,
          DB_NAME: databaseName,
          RESTORE_BUCKET: this.bucket.bucketName,
        },
        bundling: {
          nodeModules: ['pg'],
        },
      }
    );

    // 権限付与
    dbSecret.grantRead(restoreFunction);
    this.bucket.grantRead(restoreFunction);

    // Aurora クラスター作成後に利用可能
    restoreFunction.node.addDependency(cluster as unknown as Construct);

    this.function = restoreFunction;

    // デプロイ時にバックアップファイルを S3 にアップロード
    if (backupDir && fs.existsSync(backupDir)) {
      new s3deploy.BucketDeployment(this, 'BackupDeployment', {
        sources: [s3deploy.Source.asset(backupDir)],
        destinationBucket: this.bucket,
        prune: false,
      });
    }

    // バケット名を出力
    new cdk.CfnOutput(this, 'RestoreBucketName', {
      value: this.bucket.bucketName,
      description: 'S3 bucket for DB restore files',
    });
  }
}
