import * as cdk from 'aws-cdk-lib';
import * as apigateway from 'aws-cdk-lib/aws-apigateway';
import type * as lambda from 'aws-cdk-lib/aws-lambda';
import { Construct } from 'constructs';

interface Props {
  readonly prefix: string;
  readonly lambdaFunction: lambda.IFunction;
  readonly dailyQuota: number;
}

export class ApiGateway extends Construct {
  public readonly restApi: apigateway.IRestApi;
  public readonly apiKey: apigateway.IApiKey;

  constructor(scope: Construct, id: string, props: Props) {
    super(scope, id);

    const { prefix, lambdaFunction, dailyQuota } = props;

    // LambdaRestApi作成（ストリーミング対応 + API Key 必須）
    const restApi = new apigateway.LambdaRestApi(this, 'RestApi', {
      handler: lambdaFunction,
      restApiName: `${prefix}-myfriend-api`,
      description: 'REST API for Myfriend agent with response streaming',
      endpointTypes: [apigateway.EndpointType.REGIONAL],
      deployOptions: {
        stageName: 'v1',
      },
      defaultMethodOptions: {
        apiKeyRequired: true,
      },
      integrationOptions: {
        proxy: true,
        responseTransferMode: apigateway.ResponseTransferMode.STREAM,
        timeout: cdk.Duration.minutes(15),
      },
    });

    this.restApi = restApi;

    // API Key
    this.apiKey = restApi.addApiKey('ApiKey', {
      apiKeyName: `${prefix}-myfriend-api-key`,
    });

    // Usage Plan（スロットリング + 日次クォータ）
    const usagePlan = restApi.addUsagePlan('UsagePlan', {
      name: `${prefix}-myfriend-usage-plan`,
      apiStages: [
        { api: restApi, stage: restApi.deploymentStage },
      ],
      throttle: { rateLimit: 10, burstLimit: 20 },
      quota: { limit: dailyQuota, period: apigateway.Period.DAY },
    });
    usagePlan.addApiKey(this.apiKey);
  }
}
