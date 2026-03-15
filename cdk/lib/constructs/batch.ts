import * as path from 'path';
import * as cdk from 'aws-cdk-lib';
import type * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as ecr_assets from 'aws-cdk-lib/aws-ecr-assets';
import * as events from 'aws-cdk-lib/aws-events';
import * as targets from 'aws-cdk-lib/aws-events-targets';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import type * as secretsmanager from 'aws-cdk-lib/aws-secretsmanager';
import * as sqs from 'aws-cdk-lib/aws-sqs';
import { Construct } from 'constructs';

interface Props {
  readonly prefix: string;
  readonly vpc: ec2.IVpc;
  readonly lambdaSecurityGroup: ec2.ISecurityGroup;
  readonly isolatedSubnets: ec2.ISubnet[];
  readonly dbSecret: secretsmanager.ISecret;
  readonly dbHost: string;
  readonly databaseName: string;
  readonly scheduleMinutes: number;
  readonly timeoutSeconds: number;
  readonly memoryMb: number;
  readonly enabled: boolean;
}

export class Batch extends Construct {
  public readonly function: lambda.IFunction;

  constructor(scope: Construct, id: string, props: Props) {
    super(scope, id);
    const {
      prefix,
      vpc,
      lambdaSecurityGroup,
      isolatedSubnets,
      dbSecret,
      dbHost,
      databaseName,
      scheduleMinutes,
      timeoutSeconds,
      memoryMb,
      enabled,
    } = props;

    // DLQ
    const dlq = new sqs.Queue(this, 'BatchDLQ', {
      queueName: `${prefix}-myfriend-batch-dlq`,
      retentionPeriod: cdk.Duration.days(14),
    });

    // Batch Lambda（Docker Image）
    this.function = new lambda.DockerImageFunction(this, 'BatchFunction', {
      functionName: `${prefix}-myfriend-batch`,
      code: lambda.DockerImageCode.fromImageAsset(
        path.join(__dirname, '../../..'),
        {
          file: 'batch/Dockerfile',
          platform: ecr_assets.Platform.LINUX_AMD64,
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
            'batch/.venv',
            'batch/.env.local',
          ],
        }
      ),
      timeout: cdk.Duration.seconds(timeoutSeconds),
      memorySize: memoryMb,
      vpc,
      vpcSubnets: { subnets: isolatedSubnets },
      securityGroups: [lambdaSecurityGroup],
      environment: {
        DB_SECRET_ARN: dbSecret.secretArn,
        DB_HOST: dbHost,
        DB_NAME: databaseName,
      },
      deadLetterQueue: dlq,
    });

    // Secrets Manager へのアクセス権限
    dbSecret.grantRead(this.function);

    // Bedrock フルアクセス権限
    this.function.addToRolePolicy(
      new iam.PolicyStatement({
        effect: iam.Effect.ALLOW,
        actions: ['bedrock:*'],
        resources: ['*'],
      })
    );

    // EventBridge スケジュール
    if (enabled) {
      const rule = new events.Rule(this, 'BatchScheduleRule', {
        ruleName: `${prefix}-myfriend-batch-schedule`,
        schedule: events.Schedule.rate(
          cdk.Duration.minutes(scheduleMinutes)
        ),
      });
      rule.addTarget(new targets.LambdaFunction(this.function));
    }
  }
}
