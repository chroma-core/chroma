import fs from "fs";
import { Fetcher, fetcherImpl } from "../../../src/core/fetcher/Fetcher";
import { join } from "path";

describe("Test fetcherImpl", () => {
    it("should handle successful request", async () => {
        const mockArgs: Fetcher.Args = {
            url: "https://httpbin.org/post",
            method: "POST",
            headers: { "X-Test": "x-test-header" },
            body: { data: "test" },
            contentType: "application/json",
            requestType: "json",
        };

        global.fetch = jest.fn().mockResolvedValue({
            ok: true,
            status: 200,
            text: () => Promise.resolve(JSON.stringify({ data: "test" })),
            json: () => ({ data: "test" }),
        });

        const result = await fetcherImpl(mockArgs);
        expect(result.ok).toBe(true);
        if (result.ok) {
            expect(result.body).toEqual({ data: "test" });
        }

        expect(global.fetch).toHaveBeenCalledWith(
            "https://httpbin.org/post",
            expect.objectContaining({
                method: "POST",
                headers: expect.objectContaining({ "X-Test": "x-test-header" }),
                body: JSON.stringify({ data: "test" }),
            })
        );
    });

    it("should send octet stream", async () => {
        const url = "https://httpbin.org/post/file";
        const mockArgs: Fetcher.Args = {
            url,
            method: "POST",
            headers: { "X-Test": "x-test-header" },
            contentType: "application/octet-stream",
            requestType: "bytes",
            duplex: "half",
            body: fs.createReadStream(join(__dirname, "test-file.txt")),
        };

        global.fetch = jest.fn().mockResolvedValue({
            ok: true,
            status: 200,
            text: () => Promise.resolve(JSON.stringify({ data: "test" })),
            json: () => Promise.resolve({ data: "test" }),
        });

        const result = await fetcherImpl(mockArgs);

        expect(global.fetch).toHaveBeenCalledWith(
            url,
            expect.objectContaining({
                method: "POST",
                headers: expect.objectContaining({ "X-Test": "x-test-header" }),
                body: expect.any(fs.ReadStream),
            })
        );
        expect(result.ok).toBe(true);
        if (result.ok) {
            expect(result.body).toEqual({ data: "test" });
        }
    });
});
