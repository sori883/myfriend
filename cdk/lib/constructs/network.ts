import * as ec2 from 'aws-cdk-lib/aws-ec2';
import { Construct } from 'constructs';

interface Props {
  readonly publicNats: number;
  readonly cidr: string;
  readonly maxAzs: number;
  readonly subnetConfigs: ec2.SubnetConfiguration[];
  readonly subnetSelectionName: string;
  readonly egressSubnetName?: string;
}

export class Network extends Construct {
  public readonly vpc: ec2.IVpc;
  public readonly sgVpce: ec2.ISecurityGroup;
  public readonly sgLambda: ec2.ISecurityGroup;
  public readonly sgAurora: ec2.ISecurityGroup;
  public readonly isolatedSubnets: ec2.ISubnet[];
  public readonly privateSubnets: ec2.ISubnet[];

  constructor(scope: Construct, id: string, props: Props) {
    super(scope, id);
    const { publicNats, cidr, maxAzs, subnetConfigs, subnetSelectionName, egressSubnetName } =
      props;

    if (egressSubnetName && publicNats === 0) {
      throw new Error(
        `egressSubnetName "${egressSubnetName}" が指定されていますが、publicNats が 0 のため NAT Gateway がありません。`
      );
    }

    /**
     * NATが必要か否かでPublicにするか定義
     */
    const isInternet =
      publicNats > 0
        ? {
            natGateways: publicNats,
            natGatewaySubnets: {
              subnetGroupName: 'NatPublic',
            },
          }
        : null;

    /**
     * VPC作成
     */
    this.vpc = new ec2.Vpc(this, 'Vpc', {
      ipAddresses: ec2.IpAddresses.cidr(cidr),
      maxAzs: maxAzs,
      ...isInternet,
      subnetConfiguration: [
        ...Object.values(subnetConfigs),
        ...((isInternet && [
          {
            name: 'NatPublic',
            subnetType: ec2.SubnetType.PUBLIC,
            cidrMask: 26,
            mapPublicIpOnLaunch: false,
          },
        ]) ||
          []),
      ],
    });

    this.isolatedSubnets = this.vpc.selectSubnets({
      subnetGroupName: subnetSelectionName,
    }).subnets;

    // NAT Gateway 経由でインターネットアクセス可能なサブネット（AgentCore 用）
    this.privateSubnets = egressSubnetName
      ? this.vpc.selectSubnets({ subnetGroupName: egressSubnetName }).subnets
      : this.isolatedSubnets;

    /**
     * Security Groups
     */

    // Lambda 用 SG（egress all）
    this.sgLambda = new ec2.SecurityGroup(this, 'LambdaSG', {
      vpc: this.vpc,
      description: 'Security group for Lambda functions',
      allowAllOutbound: true,
    });

    // Aurora 用 SG（Lambda SG からの 5432 inbound）
    this.sgAurora = new ec2.SecurityGroup(this, 'AuroraSG', {
      vpc: this.vpc,
      description: 'Security group for Aurora cluster',
      allowAllOutbound: false,
    });
    this.sgAurora.addIngressRule(
      this.sgLambda,
      ec2.Port.tcp(5432),
      'Allow PostgreSQL from Lambda'
    );

    // VPC Endpoint 用 SG
    this.sgVpce = new ec2.SecurityGroup(this, 'VpcEndpointSG', {
      vpc: this.vpc,
      description: 'Security group for VPC Endpoints',
      allowAllOutbound: true,
    });
    this.sgVpce.addIngressRule(
      ec2.Peer.ipv4(this.vpc.vpcCidrBlock),
      ec2.Port.tcp(443),
      'Allow HTTPS from VPC'
    );

    /**
     * VPC Endpoints
     */
    const subnetSelection = this.vpc.selectSubnets({
      subnetGroupName: subnetSelectionName,
    });

    // SSM
    this.vpc.addInterfaceEndpoint('SsmEndpoint', {
      service: ec2.InterfaceVpcEndpointAwsService.SSM,
      privateDnsEnabled: true,
      securityGroups: [this.sgVpce],
      subnets: subnetSelection,
    });

    // Secrets Manager
    this.vpc.addInterfaceEndpoint('SecretsManagerEndpoint', {
      service: ec2.InterfaceVpcEndpointAwsService.SECRETS_MANAGER,
      privateDnsEnabled: true,
      securityGroups: [this.sgVpce],
      subnets: subnetSelection,
    });

    // Bedrock Runtime
    this.vpc.addInterfaceEndpoint('BedrockRuntimeEndpoint', {
      service: ec2.InterfaceVpcEndpointAwsService.BEDROCK_RUNTIME,
      privateDnsEnabled: true,
      securityGroups: [this.sgVpce],
      subnets: subnetSelection,
    });

    // Bedrock Agent Runtime（Rerank API 用）
    this.vpc.addInterfaceEndpoint('BedrockAgentRuntimeEndpoint', {
      service: ec2.InterfaceVpcEndpointAwsService.BEDROCK_AGENT_RUNTIME,
      privateDnsEnabled: true,
      securityGroups: [this.sgVpce],
      subnets: subnetSelection,
    });

    // ECR API（AgentCore VPC モード用）
    this.vpc.addInterfaceEndpoint('EcrApiEndpoint', {
      service: ec2.InterfaceVpcEndpointAwsService.ECR,
      privateDnsEnabled: true,
      securityGroups: [this.sgVpce],
      subnets: subnetSelection,
    });

    // ECR Docker（AgentCore VPC モード用）
    this.vpc.addInterfaceEndpoint('EcrDockerEndpoint', {
      service: ec2.InterfaceVpcEndpointAwsService.ECR_DOCKER,
      privateDnsEnabled: true,
      securityGroups: [this.sgVpce],
      subnets: subnetSelection,
    });

    // CloudWatch Logs（AgentCore VPC モード用）
    this.vpc.addInterfaceEndpoint('CloudWatchLogsEndpoint', {
      service: ec2.InterfaceVpcEndpointAwsService.CLOUDWATCH_LOGS,
      privateDnsEnabled: true,
      securityGroups: [this.sgVpce],
      subnets: subnetSelection,
    });

    // S3 Gateway（無料）
    this.vpc.addGatewayEndpoint('S3Endpoint', {
      service: ec2.GatewayVpcEndpointAwsService.S3,
      subnets: [subnetSelection],
    });
  }
}
