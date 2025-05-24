import { AwsClient, AwsClientV2 } from './AwsClient';
import { CohereClient } from "./Client";
import { AwsProps } from './aws-utils';
export declare class SagemakerClient extends AwsClient {
    constructor(_options: CohereClient.Options & AwsProps);
}
export declare class SagemakerClientV2 extends AwsClientV2 {
    constructor(_options: CohereClient.Options & AwsProps);
}
