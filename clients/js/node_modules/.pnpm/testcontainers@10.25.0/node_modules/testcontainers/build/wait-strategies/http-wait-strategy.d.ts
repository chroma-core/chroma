import Dockerode from "dockerode";
import { BoundPorts } from "../utils/bound-ports";
import { AbstractWaitStrategy } from "./wait-strategy";
export interface HttpWaitStrategyOptions {
    abortOnContainerExit?: boolean;
}
export declare class HttpWaitStrategy extends AbstractWaitStrategy {
    private readonly path;
    private readonly port;
    private readonly options;
    private protocol;
    private method;
    private headers;
    private predicates;
    private _allowInsecure;
    private readTimeout;
    constructor(path: string, port: number, options: HttpWaitStrategyOptions);
    forStatusCode(statusCode: number): HttpWaitStrategy;
    forStatusCodeMatching(predicate: (statusCode: number) => boolean): HttpWaitStrategy;
    forResponsePredicate(predicate: (response: string) => boolean): HttpWaitStrategy;
    withMethod(method: string): HttpWaitStrategy;
    withHeaders(headers: {
        [key: string]: string;
    }): HttpWaitStrategy;
    withBasicCredentials(username: string, password: string): HttpWaitStrategy;
    withReadTimeout(readTimeout: number): HttpWaitStrategy;
    usingTls(): HttpWaitStrategy;
    allowInsecure(): HttpWaitStrategy;
    waitUntilReady(container: Dockerode.Container, boundPorts: BoundPorts): Promise<void>;
    private handleContainerExit;
    private getAgent;
}
