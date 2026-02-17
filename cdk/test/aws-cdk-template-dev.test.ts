import * as cdk from 'aws-cdk-lib';
import { Template } from 'aws-cdk-lib/assertions';

import { AppStack } from '../lib/app-stack';
import { DataStack } from '../lib/data-stack';
import { InfraStack } from '../lib/main-stack';
import { parameter as p } from '../parameter';
import { validateEnvName } from '../parameter/envname-type';

test('infraStack-snapshot-dev', () => {
  const app = new cdk.App({
    context: {
      env: 'dev',
    },
  });
  const env = validateEnvName(app.node.tryGetContext('env'));
  const parameter = p(env);

  const stack = new InfraStack(app, 'Infra', { parameter });

  const template = Template.fromStack(stack);
  expect(template.toJSON()).toMatchSnapshot();
});

test('appstack-snapshot-dev', () => {
  const app = new cdk.App({
    context: {
      env: 'dev',
    },
  });
  const env = validateEnvName(app.node.tryGetContext('env'));
  const parameter = p(env);

  const infraStack = new InfraStack(app, 'Infra', { parameter });
  const appStack = new AppStack(app, 'App', {
    parameter,
    vpc: infraStack.vpc,
    sgEc2: infraStack.sgEc2,
  });

  const template = Template.fromStack(appStack);
  expect(template.toJSON()).toMatchSnapshot();
});

test('datastack-snapshot-dev', () => {
  const app = new cdk.App({
    context: {
      env: 'dev',
    },
  });
  const env = validateEnvName(app.node.tryGetContext('env'));
  const parameter = p(env);

  const infraStack = new InfraStack(app, 'Infra', { parameter });
  const dataStack = new DataStack(app, 'Data', {
    parameter,
    vpc: infraStack.vpc,
    sgRds: infraStack.sgRds,
  });

  const template = Template.fromStack(dataStack);
  expect(template.toJSON()).toMatchSnapshot();
});
