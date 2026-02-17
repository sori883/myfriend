import { RemovalPolicy } from 'aws-cdk-lib';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as rds from 'aws-cdk-lib/aws-rds';

import type { EnvNameType } from './envname-type';
import { env } from './validate-dotenv';

export type ParameterType = ReturnType<typeof parameter>;

/**
 * .env、環境差分パラメータ、共通パラメータをまとめる
 * @param envName EnvNameType
 * @returns パラメータ
 */
export const parameter = (envName: EnvNameType) => ({
  prefix: envName,
  region: 'ap-northeast-1',
  owner: 'sori883',
  project: 'cdk-template',
  cost: `cdk-template-${envName}`,
  // .envパラメータ
  dotEnv: { ...env },
  // 環境差分パラメータ
  diffEnv: envDiffParameter(envName),
});

/**
 * 環境差分があるパラメータを定義する
 * 例：性能パラメータなど
 * @param envName EnvNameType
 * @returns 環境差分パラメータ
 */
const envDiffParameter = (envName: EnvNameType) => {
  const params = {
    prd: {
      vpc: {
        cidr: '10.0.0.0/16',
        maxAzs: 1,
        publicNats: 0,
        subnets: {
          Private1: {
            name: 'Private1',
            subnetType: ec2.SubnetType.PRIVATE_ISOLATED,
            cidrMask: 24,
          },
          Private2: {
            name: 'Private2',
            subnetType: ec2.SubnetType.PRIVATE_ISOLATED,
            cidrMask: 24,
          },
        },
      },
    },
    stg: {
      vpc: {
        cidr: '10.0.0.0/16',
        maxAzs: 1,
        publicNats: 0,
        subnets: {
          Private1: {
            name: 'Private1',
            subnetType: ec2.SubnetType.PRIVATE_ISOLATED,
            cidrMask: 24,
          },
          Private2: {
            name: 'Private2',
            subnetType: ec2.SubnetType.PRIVATE_ISOLATED,
            cidrMask: 24,
          },
        },
      },
    },
    dev: {
      vpc: {
        cidr: '10.0.0.0/16',
        maxAzs: 1,
        publicNats: 0,
        subnets: {
          Private1: {
            name: 'Private1',
            subnetType: ec2.SubnetType.PRIVATE_ISOLATED,
            cidrMask: 24,
          },
          Private2: {
            name: 'Private2',
            subnetType: ec2.SubnetType.PRIVATE_ISOLATED,
            cidrMask: 24,
          },
        },
      },
    },
  };
  return params[envName];
};
