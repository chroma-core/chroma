import { AwsProps } from './aws-utils';
import { AwsClient, AwsClientV2 } from './AwsClient';
import { CohereClient } from "./Client";
export declare class BedrockClient extends AwsClient {
    constructor(_options: CohereClient.Options & AwsProps);
}
export declare class BedrockClientV2 extends AwsClientV2 {
    constructor(_options: CohereClient.Options & AwsProps);
}
