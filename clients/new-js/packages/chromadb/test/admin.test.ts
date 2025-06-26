import { describe, expect, test } from "@jest/globals";
import { AdminClient, CloudClient } from "../src";

describe("AdminClient", () => {
  // connects to the unauthenticated chroma instance started in
  // the global jest setup file.
  const adminClient = new AdminClient({
    host: process.env.DEFAULT_CHROMA_INSTANCE_HOST || "localhost",
    port: parseInt(process.env.DEFAULT_CHROMA_INSTANCE_PORT || "8000"),
    ssl: false,
  });

  test("it should create the admin client connection", async () => {
    expect(adminClient).toBeDefined();
    expect(adminClient).toBeInstanceOf(AdminClient);
  });

  test("it should create and get a tenant", async () => {
    await adminClient.createTenant({ name: "testTenant" });
    const tenant = await adminClient.getTenant({ name: "testTenant" });
    expect(tenant).toBe("testTenant");
  });

  test("it should create and get a database for a tenant", async () => {
    await adminClient.createTenant({ name: "test3" });
    await adminClient.createDatabase({
      name: "test",
      tenant: "test3",
    });

    const getDatabase = await adminClient.getDatabase({
      name: "test",
      tenant: "test3",
    });
    expect(getDatabase).toBeDefined();
    expect(getDatabase).toHaveProperty("name");
    expect(getDatabase.name).toBe("test");
  });

  test("it should delete a database", async () => {
    await adminClient.createTenant({ name: "test4" });
    await adminClient.createDatabase({
      name: "test",
      tenant: "test4",
    });
    await adminClient.deleteDatabase({ name: "test", tenant: "test4" });

    await expect(
      adminClient.getDatabase({ name: "test", tenant: "test4" }),
    ).rejects.toThrow();
  });

  test("it should list databases for a tenant", async () => {
    await adminClient.createTenant({ name: "test2" });

    for (let i = 0; i < 5; i++) {
      await adminClient.createDatabase({
        name: `test${i}`,
        tenant: "test2",
      });
    }

    const firstTwoDatabases = await adminClient.listDatabases({
      tenant: "test2",
      limit: 2,
    });
    expect(firstTwoDatabases.map((d) => d.name)).toEqual(["test0", "test1"]);

    const lastThreeDatabases = await adminClient.listDatabases({
      tenant: "test2",
      offset: 2,
    });
    expect(lastThreeDatabases.map((d) => d.name)).toEqual([
      "test2",
      "test3",
      "test4",
    ]);
  });

  test("it should throw well-formatted errors", async () => {
    // Create it once, should succeed (or use a unique name guaranteed not to exist)
    const dbName = "test_unique_error_" + Date.now(); // Ensure unique name for first creation
    const tenantName = "foo";
    await adminClient.createDatabase({ name: dbName, tenant: tenantName });

    // Attempt to create it again, should fail
    try {
      await adminClient.createDatabase({
        name: dbName,
        tenant: tenantName,
      });
      // If it reaches here, the test failed because no error was thrown
      expect(true).toBe(false);
    } catch (error) {
      expect(error).toBeInstanceOf(Error);
    }
  });
});
