import { RemovalPolicy } from 'aws-cdk-lib';
import * as ec2 from 'aws-cdk-lib/aws-ec2';

import type { EnvNameType } from './envname-type';
import { env } from './validate-dotenv';

export type ParameterType = ReturnType<typeof parameter>;

export const parameter = (envName: EnvNameType) => ({
  prefix: envName,
  region: 'ap-northeast-1',
  owner: 'sori883',
  project: 'myfriend',
  cost: `myfriend-${envName}`,
  dotEnv: { ...env },
  diffEnv: envDiffParameter(envName),
});

const envDiffParameter = (envName: EnvNameType) => {
  const params = {
    prd: {
      vpc: {
        cidr: '10.0.0.0/16',
        maxAzs: 2,
        publicNats: 1,
        subnets: {
          Private1: {
            name: 'Private1',
            subnetType: ec2.SubnetType.PRIVATE_ISOLATED,
            cidrMask: 24,
          },
          Private2: {
            name: 'Private2',
            subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS,
            cidrMask: 24,
          },
        },
      },
      aurora: {
        minAcu: 0.5,
        maxAcu: 8,
        backupRetentionDays: 7,
        deletionProtection: true,
        removalPolicy: RemovalPolicy.RETAIN,
      },
      batch: {
        scheduleMinutes: 5,
        timeoutSeconds: 780,
        memoryMb: 1024,
        enabled: true,
      },
      api: {
        dailyQuota: 50,
      },
    },
    stg: {
      vpc: {
        cidr: '10.0.0.0/16',
        maxAzs: 2,
        publicNats: 1,
        subnets: {
          Private1: {
            name: 'Private1',
            subnetType: ec2.SubnetType.PRIVATE_ISOLATED,
            cidrMask: 24,
          },
          Private2: {
            name: 'Private2',
            subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS,
            cidrMask: 24,
          },
        },
      },
      aurora: {
        minAcu: 0.5,
        maxAcu: 2,
        backupRetentionDays: 1,
        deletionProtection: false,
        removalPolicy: RemovalPolicy.DESTROY,
      },
      batch: {
        scheduleMinutes: 5,
        timeoutSeconds: 780,
        memoryMb: 1024,
        enabled: false,
      },
      api: {
        dailyQuota: 50,
      },
    },
    dev: {
      vpc: {
        cidr: '10.0.0.0/16',
        maxAzs: 2,
        publicNats: 1,
        subnets: {
          Private1: {
            name: 'Private1',
            subnetType: ec2.SubnetType.PRIVATE_ISOLATED,
            cidrMask: 24,
          },
          Private2: {
            name: 'Private2',
            subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS,
            cidrMask: 24,
          },
        },
      },
      aurora: {
        minAcu: 0.5,
        maxAcu: 2,
        backupRetentionDays: 1,
        deletionProtection: false,
        removalPolicy: RemovalPolicy.DESTROY,
      },
      batch: {
        scheduleMinutes: 5,
        timeoutSeconds: 780,
        memoryMb: 1024,
        enabled: true,
      },
      api: {
        dailyQuota: 50,
      },
    },
  };
  return params[envName];
};
