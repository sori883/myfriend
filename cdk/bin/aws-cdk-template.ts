#!/usr/bin/env node
import * as cdk from 'aws-cdk-lib/core';

import { MainStack } from '../lib/main-stack';
import { parameter as p } from '../parameter';
import { validateEnvName } from '../parameter/envname-type';

const app = new cdk.App();

const env = validateEnvName(app.node.tryGetContext('env'));
const parameter = p(env);

cdk.Tags.of(app).add('Project', parameter.project);
cdk.Tags.of(app).add('Cost', parameter.cost);
cdk.Tags.of(app).add('Owner', parameter.owner);

new MainStack(app, 'Main', {
  stackName: `${parameter.prefix}-Main`,
  env: { account: parameter.dotEnv.ACCOUNT_ID, region: parameter.region },
  parameter,
});
