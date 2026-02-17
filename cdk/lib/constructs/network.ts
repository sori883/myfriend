import * as ec2 from 'aws-cdk-lib/aws-ec2';
import { Construct } from 'constructs';

interface Props {
  readonly publicNats: number;
  readonly cidr: string;
  readonly maxAzs: number;
  readonly subnetConfigs: ec2.SubnetConfiguration[];
  readonly subnetSelectionName: string;
}

export class Network extends Construct {
  public readonly vpc: ec2.IVpc;
  public readonly sgVpce: ec2.ISecurityGroup;

  constructor(scope: Construct, id: string, props: Props) {
    super(scope, id);
    const { publicNats, cidr, maxAzs, subnetConfigs, subnetSelectionName } =
      props;

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

    /**
     * VPCE
     */
    this.sgVpce = new ec2.SecurityGroup(this, 'VpcEndpointSG', {
      vpc: this.vpc,
      description: 'Security group for VPC Endpoints',
      allowAllOutbound: true,
    });
    // VPCからのHTTPS通信を許可
    this.sgVpce.addIngressRule(
      ec2.Peer.ipv4(this.vpc.vpcCidrBlock),
      ec2.Port.tcp(443),
      'Allow HTTPS from VPC'
    );

    this.vpc.addInterfaceEndpoint('SsmEndpoint', {
      service: ec2.InterfaceVpcEndpointAwsService.SSM,
      privateDnsEnabled: true,
      securityGroups: [this.sgVpce],
      subnets: this.vpc.selectSubnets({
        subnetGroupName: subnetSelectionName,
      }),
    });
  }
}
