export interface RetryConfig {
  factor: number;
  minDelay: number;
  maxDelay: number;
  maxAttempts: number;
  jitter: boolean;
}

export const defaultRetryConfig: RetryConfig = {
  factor: 2.0,
  minDelay: 0.1,
  maxDelay: 5.0,
  maxAttempts: 5,
  jitter: true,
};
