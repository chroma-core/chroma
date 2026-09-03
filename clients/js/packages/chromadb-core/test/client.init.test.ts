import { describe, expect, jest, test } from "@jest/globals";
import type { AdminClient } from "../src/AdminClient";
import { ChromaClient } from "../src/ChromaClient";

describe("ChromaClient.init", () => {
  test("shares initialization across concurrent callers", async () => {
    const client = new ChromaClient({
      auth: { provider: "token", credentials: "test-token" },
    });
    const adminClient = Reflect.get(client, "_adminClient") as AdminClient;

    let releaseIdentity!: () => void;
    const identityGate = new Promise<void>((resolve) => {
      releaseIdentity = resolve;
    });

    const identitySpy = jest
      .spyOn(client, "getUserIdentity")
      .mockImplementation(() => identityGate);
    const tenantSpy = jest
      .spyOn(adminClient, "getTenant")
      .mockResolvedValue({ name: "default_tenant" });
    const databaseSpy = jest
      .spyOn(adminClient, "getDatabase")
      .mockResolvedValue({
        id: "default_database",
        tenant: "default_tenant",
        name: "default_database",
      });

    const pending = [client.init(), client.init(), client.init()];
    releaseIdentity();
    await Promise.all(pending);

    expect(identitySpy).toHaveBeenCalledTimes(1);
    expect(tenantSpy).toHaveBeenCalledTimes(1);
    expect(databaseSpy).toHaveBeenCalledTimes(1);
  });
});
