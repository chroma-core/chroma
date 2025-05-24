import { AwsProps } from './aws-utils';
import { CohereClient } from "./Client";
import { CohereClientV2 } from './ClientV2';
export declare class AwsClient extends CohereClient {
    constructor(_options: CohereClient.Options & AwsProps);
}
export declare class AwsClientV2 extends CohereClientV2 {
    constructor(_options: CohereClient.Options & AwsProps);
}
