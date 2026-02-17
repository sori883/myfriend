import type * as ec2 from 'aws-cdk-lib/aws-ec2';
import type { Construct } from 'constructs';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as cdk from 'aws-cdk-lib/core';

import type { ParameterType } from '../parameter';
import { AgentCore } from './constructs/agentcore';
import { ApiGateway } from './constructs/api-gateway';
import { Network } from './constructs/network';
import { ProxyLambda } from './constructs/proxy-lambda';

interface StackProps extends cdk.StackProps {
  readonly parameter: ParameterType;
}

export class MainStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props: StackProps) {
    super(scope, id, props);
    const { parameter } = props;

    const network = new Network(this, 'network', {
      publicNats: parameter.diffEnv.vpc.publicNats,
      cidr: parameter.diffEnv.vpc.cidr,
      maxAzs: parameter.diffEnv.vpc.maxAzs,
      subnetConfigs: Object.values(parameter.diffEnv.vpc.subnets),
      subnetSelectionName: parameter.diffEnv.vpc.subnets.Private1.name,
    });

    const agentCore = new AgentCore(this, 'agentcore');

    const proxyLambda = new ProxyLambda(this, 'ProxyLambda', {
      agentCoreRuntime: agentCore.agentCoreRuntime,
    });

    new ApiGateway(this, 'APIGateway', {
      lambdaFunction: proxyLambda.function,
    });
  }
}
