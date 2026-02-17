import * as cdk from 'aws-cdk-lib';
import * as apigateway from 'aws-cdk-lib/aws-apigateway';
import type * as lambda from 'aws-cdk-lib/aws-lambda';
import { Construct } from 'constructs';

interface Props {
  readonly lambdaFunction: lambda.IFunction;
}

export class ApiGateway extends Construct {
  public readonly restApi: apigateway.IRestApi;

  constructor(scope: Construct, id: string, props: Props) {
    super(scope, id);

    const { lambdaFunction } = props;

    // LambdaRestApi作成（ストリーミング対応）
    // すべてのリクエストがLambdaに転送される
    // responseTransferMode: STREAM でストリーミングを有効化（最大15分）
    this.restApi = new apigateway.LambdaRestApi(this, 'RestApi', {
      handler: lambdaFunction,
      restApiName: 'FrontendApi',
      description: 'REST API for Frontend Lambda with response streaming',
      deployOptions: {
        stageName: 'v1',
      },
      integrationOptions: {
        proxy: true,
        responseTransferMode: apigateway.ResponseTransferMode.STREAM,
        timeout: cdk.Duration.minutes(15),
      },
    });
  }
}
