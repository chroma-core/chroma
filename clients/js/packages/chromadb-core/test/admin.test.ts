import { afterAll, beforeAll, describe, expect, test } from "@jest/globals";
import { AdminClient } from "../src/AdminClient";
import { ChromaError, ChromaUniqueError } from "../src/Errors";
import { startChromaContainer } from "./startChromaContainer";

describe("AdminClient", () => {
  // connects to the unauthenticated chroma instance started in
  // the global jest setup file.
  const adminClient = new AdminClient({
    path: process.env.DEFAULT_CHROMA_INSTANCE_URL,
  });

  test("it should create the admin client connection", async () => {
    expect(adminClient).toBeDefined();
    expect(adminClient).toBeInstanceOf(AdminClient);
  });

  test("it should create and get a tenant", async () => {
    await adminClient.createTenant({ name: "testTenant" });
    const tenant = await adminClient.getTenant({ name: "testTenant" });
    expect(tenant).toBeDefined();
    expect(tenant).toHaveProperty("name");
    expect(tenant.name).toBe("testTenant");
  });

  test("it should create and get a database for a tenant", async () => {
    await adminClient.createTenant({ name: "test3" });
    const database = await adminClient.createDatabase({
      name: "test",
      tenantName: "test3",
    });
    expect(database).toBeDefined();
    expect(database).toHaveProperty("name");
    expect(database.name).toBe("test");

    const getDatabase = await adminClient.getDatabase({
      name: "test",
      tenantName: "test3",
    });
    expect(getDatabase).toBeDefined();
    expect(getDatabase).toHaveProperty("name");
    expect(getDatabase.name).toBe("test");
  });

  test("it should delete a database", async () => {
    await adminClient.createTenant({ name: "test4" });
    await adminClient.createDatabase({
      name: "test",
      tenantName: "test4",
    });
    await adminClient.deleteDatabase({ name: "test", tenantName: "test4" });

    await expect(
      adminClient.getDatabase({ name: "test", tenantName: "test4" }),
    ).rejects.toThrow();
  });

  test("it should list databases for a tenant", async () => {
    await adminClient.createTenant({ name: "test2" });

    for (let i = 0; i < 5; i++) {
      await adminClient.createDatabase({
        name: `test${i}`,
        tenantName: "test2",
      });
    }

    const firstTwoDatabases = await adminClient.listDatabases({
      tenantName: "test2",
      limit: 2,
    });
    expect(firstTwoDatabases.map((d) => d.name)).toEqual(["test0", "test1"]);

    const lastThreeDatabases = await adminClient.listDatabases({
      tenantName: "test2",
      offset: 2,
    });
    expect(lastThreeDatabases.map((d) => d.name)).toEqual([
      "test2",
      "test3",
      "test4",
    ]);
  });

  // test that it can set the tenant and database
  test("it should set the tenant and database", async () => {
    // doesnt exist so should throw
    await expect(
      adminClient.setTenant({ tenant: "testTenant", database: "testDatabase" }),
    ).rejects.toThrow();

    await adminClient.createTenant({ name: "testTenant!" });
    await adminClient.createDatabase({
      name: "test3!",
      tenantName: "testTenant!",
    });

    await adminClient.setTenant({ tenant: "testTenant!", database: "test3!" });
    expect(adminClient.tenant).toBe("testTenant!");
    expect(adminClient.database).toBe("test3!");

    // doesnt exist so should throw
    await expect(
      adminClient.setDatabase({ database: "testDatabase2" }),
    ).rejects.toThrow();

    await adminClient.createDatabase({
      name: "testDatabase2",
      tenantName: "testTenant!",
    });
    await adminClient.setDatabase({ database: "testDatabase2" });

    expect(adminClient.database).toBe("testDatabase2");
  });

  test("it should throw well-formatted errors", async () => {
    // Create it once, should succeed (or use a unique name guaranteed not to exist)
    const dbName = "test_unique_error_" + Date.now(); // Ensure unique name for first creation
    const tenantName = "foo";
    await adminClient.createDatabase({ name: dbName, tenantName: tenantName });

    // Attempt to create it again, should fail
    try {
      await adminClient.createDatabase({
        name: dbName,
        tenantName: tenantName,
      });
      // If it reaches here, the test failed because no error was thrown
      expect(true).toBe(false);
    } catch (error) {
      expect(error).toBeInstanceOf(Error);
      expect(error).toBeInstanceOf(ChromaUniqueError);
    }
  });
});
