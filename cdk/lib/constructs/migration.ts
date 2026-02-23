import * as crypto from 'crypto';
import * as fs from 'fs';
import * as path from 'path';
import * as cdk from 'aws-cdk-lib';
import type * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as nodejs from 'aws-cdk-lib/aws-lambda-nodejs';
import * as cr from 'aws-cdk-lib/custom-resources';
import type * as rds from 'aws-cdk-lib/aws-rds';
import type * as secretsmanager from 'aws-cdk-lib/aws-secretsmanager';
import { Construct } from 'constructs';

interface Props {
  readonly vpc: ec2.IVpc;
  readonly lambdaSecurityGroup: ec2.ISecurityGroup;
  readonly isolatedSubnets: ec2.ISubnet[];
  readonly dbSecret: secretsmanager.ISecret;
  readonly cluster: rds.IDatabaseCluster;
  readonly databaseName: string;
}

export class Migration extends Construct {
  constructor(scope: Construct, id: string, props: Props) {
    super(scope, id);
    const {
      vpc,
      lambdaSecurityGroup,
      isolatedSubnets,
      dbSecret,
      cluster,
      databaseName,
    } = props;

    // マイグレーション Lambda
    const migrationFunction = new nodejs.NodejsFunction(
      this,
      'MigrationFunction',
      {
        functionName: 'myfriend-db-migration',
        entry: path.join(
          __dirname,
          '../../resources/migration/index.ts'
        ),
        handler: 'handler',
        runtime: lambda.Runtime.NODEJS_24_X,
        timeout: cdk.Duration.minutes(5),
        memorySize: 256,
        vpc,
        vpcSubnets: { subnets: isolatedSubnets },
        securityGroups: [lambdaSecurityGroup],
        environment: {
          DB_SECRET_ARN: dbSecret.secretArn,
          DB_NAME: databaseName,
        },
        bundling: {
          nodeModules: ['pg'],
          commandHooks: {
            beforeBundling: () => [],
            beforeInstall: () => [],
            afterBundling: (inputDir: string, outputDir: string) => [
              `cp -r ${inputDir}/resources/migration/sql ${outputDir}/sql`,
            ],
          },
        },
      }
    );

    // Secrets Manager へのアクセス権限
    dbSecret.grantRead(migrationFunction);

    // Custom Resource でデプロイ時に自動実行
    const provider = new cr.Provider(this, 'MigrationProvider', {
      onEventHandler: migrationFunction,
    });

    const migrationResource = new cdk.CustomResource(
      this,
      'MigrationCustomResource',
      {
        serviceToken: provider.serviceToken,
        properties: {
          // SQL ファイルのハッシュが変わった時のみマイグレーションを再実行
          migrationHash: this.computeSqlHash(),
        },
      }
    );

    // Aurora クラスター作成後に実行
    migrationResource.node.addDependency(cluster as unknown as Construct);
  }

  private computeSqlHash(): string {
    const sqlDir = path.join(__dirname, '../../resources/migration/sql');
    const files = fs.readdirSync(sqlDir).filter(f => f.endsWith('.sql')).sort();
    const hash = crypto.createHash('sha256');
    for (const file of files) {
      hash.update(fs.readFileSync(path.join(sqlDir, file)));
    }
    return hash.digest('hex');
  }
}
