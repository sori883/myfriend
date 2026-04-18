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

    // NAT Gateway 経由でインターネットアクセス可能なサブネット（Lambda/AgentCore 用）
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

    /**
     * VPC Endpoints（無料の S3 Gateway のみ）
     */
    this.vpc.addGatewayEndpoint('S3Endpoint', {
      service: ec2.GatewayVpcEndpointAwsService.S3,
    });
  }
}
