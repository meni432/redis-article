import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as elasticache from 'aws-cdk-lib/aws-elasticache';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as kinesis from 'aws-cdk-lib/aws-kinesis';
import * as firehose from 'aws-cdk-lib/aws-kinesisfirehose';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as lambdaEventSources from 'aws-cdk-lib/aws-lambda-event-sources';

export class RedisStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // Create VPC And manage security group and Inbound Rules
    const vpc = new ec2.Vpc(this, 'RedisVPC', {
      ipAddresses: ec2.IpAddresses.cidr('10.0.0.0/16'),
      maxAzs: 2,
      natGateways: 1,
      subnetConfiguration: [
        {
          cidrMask: 24,
          name: 'RedisPublic',
          subnetType: ec2.SubnetType.PUBLIC,
        },
        {
          cidrMask: 24,
          name: 'RedisPrivate',
          subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS,
        },
      ],
    });

    const redisSecurityGroup = new ec2.SecurityGroup(this, 'RedisSecurityGroup', {
      vpc,
      securityGroupName: 'RedisSecurityGroup',
      description: 'Redis Security Group',
      allowAllOutbound: true,
    });

    const computeSecurityGroup = new ec2.SecurityGroup(this, 'ComputeSecurityGroup', {
      vpc,
      securityGroupName: 'ComputeSecurityGroup',
      description: 'Compute Security Group',
      allowAllOutbound: true,
    });

    const privateSubnetIds = vpc.selectSubnets({
      subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS,
    }).subnetIds;

    const redisSubnetGroup = new elasticache.CfnSubnetGroup(this, 'RedisSubnetGroup', {
      description: 'Redis Subnet Group',
      subnetIds: privateSubnetIds,
    });

    redisSecurityGroup.addIngressRule(computeSecurityGroup, ec2.Port.tcp(6379), 'Allow Redis Port 6379');

    // Create the redis cluster
    const redisCluster = new elasticache.CfnCacheCluster(this, 'RedisCluster', {
      cacheNodeType: 'cache.t4g.medium',
      engine: 'redis',
      numCacheNodes: 1,
      cacheSubnetGroupName: redisSubnetGroup.ref,
      vpcSecurityGroupIds: [redisSecurityGroup.securityGroupId],
    });

    new cdk.CfnOutput(this, 'RedisClusterEndpoint', {
      value: redisCluster.attrRedisEndpointAddress,
    });

    const bucketForFirehose = new s3.Bucket(this, 'BucketForFirehose', {
      bucketName: 'redis-stream',
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      encryption: s3.BucketEncryption.S3_MANAGED,
    });

  
    // Create the firehose delivery with lambda as source with direct put
    const firehoseDeliveryRole = new iam.Role(this, 'FirehoseDeliveryRole', {
      assumedBy: new iam.ServicePrincipal('firehose.amazonaws.com'),
    });

    bucketForFirehose.grantReadWrite(firehoseDeliveryRole);

    const firehoseDeliveryStream = new firehose.CfnDeliveryStream(this, 'FirehoseDeliveryStreamForRedisProject', {
      deliveryStreamName: 'FirehoseDeliveryStreamForRedisProject',
      deliveryStreamType: 'DirectPut',
      s3DestinationConfiguration: {
        bucketArn: bucketForFirehose.bucketArn,
        roleArn: firehoseDeliveryRole.roleArn,
        cloudWatchLoggingOptions: {
          enabled: true,
          logGroupName: 'FirehoseDeliveryStreamForRedisProject',
          logStreamName: 'FirehoseDeliveryStreamForRedisProject',
        },
      },
    });

    // Create lambda with access to redis cluster
    const lambdaFunction = new lambda.Function(this, 'LocationProcessor', {
      runtime: lambda.Runtime.NODEJS_20_X,
      code: lambda.Code.fromAsset('lambda'),
      handler: 'index.handler',
      vpc,
      vpcSubnets: {
        subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS,
      },
      securityGroups: [computeSecurityGroup],
      environment: {
        REDIS_HOST: redisCluster.attrRedisEndpointAddress,
        REDIS_PORT: redisCluster.attrRedisEndpointPort,
        KINESIS_FIREHOSE_NAME: firehoseDeliveryStream.ref,
      },
      timeout: cdk.Duration.seconds(10),
    });

    const kinesisIngressStream = new kinesis.Stream(this, 'KinesisIngressStream', {
      streamName: 'KinesisIngressStream',
      shardCount: 1,
    });

    new cdk.CfnOutput(this, 'KinesisIngressStreamName', {
      value: kinesisIngressStream.streamName,
    });

    kinesisIngressStream.grantRead(lambdaFunction);
    
    const eventSource = new lambdaEventSources.KinesisEventSource(kinesisIngressStream, {
      startingPosition: lambda.StartingPosition.TRIM_HORIZON,
    });

    lambdaFunction.addEventSource(eventSource);

    const firehosePutPolicy = new iam.PolicyStatement({
      effect: iam.Effect.ALLOW,
      actions: ['firehose:PutRecord', 'firehose:PutRecordBatch'],
      resources: [firehoseDeliveryStream.attrArn],
    });

    lambdaFunction.addToRolePolicy(firehosePutPolicy);
  }
}
