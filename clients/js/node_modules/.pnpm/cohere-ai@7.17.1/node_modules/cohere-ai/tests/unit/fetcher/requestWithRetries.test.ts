import { RUNTIME } from "../../../src/core/runtime";
import { requestWithRetries } from "../../../src/core/fetcher/requestWithRetries";

describe("requestWithRetries", () => {
    let mockFetch: jest.Mock;
    let originalMathRandom: typeof Math.random;
    let setTimeoutSpy: jest.SpyInstance;

    beforeEach(() => {
        mockFetch = jest.fn();
        originalMathRandom = Math.random;

        // Mock Math.random for consistent jitter
        Math.random = jest.fn(() => 0.5);

        jest.useFakeTimers({ doNotFake: ["nextTick"] });
    });

    afterEach(() => {
        Math.random = originalMathRandom;
        jest.clearAllMocks();
        jest.clearAllTimers();
    });

    it("should retry on retryable status codes", async () => {
        setTimeoutSpy = jest.spyOn(global, "setTimeout").mockImplementation((callback) => {
            process.nextTick(callback);
            return null as any;
        });

        const retryableStatuses = [408, 409, 429, 500, 502];
        let callCount = 0;

        mockFetch.mockImplementation(async () => {
            if (callCount < retryableStatuses.length) {
                return new Response("", { status: retryableStatuses[callCount++] });
            }
            return new Response("", { status: 200 });
        });

        const responsePromise = requestWithRetries(() => mockFetch(), retryableStatuses.length);
        await jest.runAllTimersAsync();
        const response = await responsePromise;

        expect(mockFetch).toHaveBeenCalledTimes(retryableStatuses.length + 1);
        expect(response.status).toBe(200);
    });

    it("should respect maxRetries limit", async () => {
        setTimeoutSpy = jest.spyOn(global, "setTimeout").mockImplementation((callback) => {
            process.nextTick(callback);
            return null as any;
        });

        const maxRetries = 2;
        mockFetch.mockResolvedValue(new Response("", { status: 500 }));

        const responsePromise = requestWithRetries(() => mockFetch(), maxRetries);
        await jest.runAllTimersAsync();
        const response = await responsePromise;

        expect(mockFetch).toHaveBeenCalledTimes(maxRetries + 1);
        expect(response.status).toBe(500);
    });

    it("should not retry on success status codes", async () => {
        setTimeoutSpy = jest.spyOn(global, "setTimeout").mockImplementation((callback) => {
            process.nextTick(callback);
            return null as any;
        });

        const successStatuses = [200, 201, 202];

        for (const status of successStatuses) {
            mockFetch.mockReset();
            setTimeoutSpy.mockClear();
            mockFetch.mockResolvedValueOnce(new Response("", { status }));

            const responsePromise = requestWithRetries(() => mockFetch(), 3);
            await jest.runAllTimersAsync();
            await responsePromise;

            expect(mockFetch).toHaveBeenCalledTimes(1);
            expect(setTimeoutSpy).not.toHaveBeenCalled();
        }
    });

    it("should apply correct exponential backoff with jitter", async () => {
        setTimeoutSpy = jest.spyOn(global, "setTimeout").mockImplementation((callback) => {
            process.nextTick(callback);
            return null as any;
        });

        mockFetch.mockResolvedValue(new Response("", { status: 500 }));
        const maxRetries = 3;
        const expectedDelays = [1000, 2000, 4000];

        const responsePromise = requestWithRetries(() => mockFetch(), maxRetries);
        await jest.runAllTimersAsync();
        await responsePromise;

        // Verify setTimeout calls
        expect(setTimeoutSpy).toHaveBeenCalledTimes(expectedDelays.length);

        expectedDelays.forEach((delay, index) => {
            expect(setTimeoutSpy).toHaveBeenNthCalledWith(index + 1, expect.any(Function), delay);
        });

        expect(mockFetch).toHaveBeenCalledTimes(maxRetries + 1);
    });

    it("should handle concurrent retries independently", async () => {
        setTimeoutSpy = jest.spyOn(global, "setTimeout").mockImplementation((callback) => {
            process.nextTick(callback);
            return null as any;
        });

        mockFetch
            .mockResolvedValueOnce(new Response("", { status: 500 }))
            .mockResolvedValueOnce(new Response("", { status: 500 }))
            .mockResolvedValueOnce(new Response("", { status: 200 }))
            .mockResolvedValueOnce(new Response("", { status: 200 }));

        const promise1 = requestWithRetries(() => mockFetch(), 1);
        const promise2 = requestWithRetries(() => mockFetch(), 1);

        await jest.runAllTimersAsync();
        const [response1, response2] = await Promise.all([promise1, promise2]);

        expect(response1.status).toBe(200);
        expect(response2.status).toBe(200);
    });
});
