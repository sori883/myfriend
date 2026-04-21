#!/usr/bin/env node
import * as cdk from 'aws-cdk-lib/core';

import { MainStack } from '../lib/main-stack';
import { MemoryStack } from '../lib/memory-stack';
import { parameter as p } from '../parameter';
import { validateEnvName } from '../parameter/envname-type';

const app = new cdk.App();

const env = validateEnvName(app.node.tryGetContext('env'));
const parameter = p(env);

cdk.Tags.of(app).add('Project', parameter.project);
cdk.Tags.of(app).add('Cost', parameter.cost);
cdk.Tags.of(app).add('Owner', parameter.owner);

// memoryEnabled のオーバーライド: --context memory=true|false
const memoryContext = app.node.tryGetContext('memory');
const memoryEnabled =
  typeof memoryContext === 'string'
    ? memoryContext === 'true'
    : parameter.diffEnv.memoryEnabled;

// memoryEnabled=true のときだけ MemoryStack をデプロイする
const memoryStack = memoryEnabled
  ? new MemoryStack(app, 'Memory', {
      stackName: `${parameter.prefix}-Memory3`,
      env: { account: parameter.dotEnv.ACCOUNT_ID, region: parameter.region },
      parameter,
    })
  : undefined;

const mainStack = new MainStack(app, 'Main', {
  stackName: `${parameter.prefix}-Main3`,
  env: { account: parameter.dotEnv.ACCOUNT_ID, region: parameter.region },
  parameter,
  memory: memoryStack && {
    vpc: memoryStack.vpc,
    privateSubnets: memoryStack.privateSubnets,
    auroraSecurityGroup: memoryStack.auroraSecurityGroup,
    dbSecret: memoryStack.dbSecret,
    dbHost: memoryStack.dbHost,
    databaseName: memoryStack.databaseName,
  },
});

// スタック依存関係: Memory が先に作成されるべき
if (memoryStack) {
  mainStack.addDependency(memoryStack);
}
