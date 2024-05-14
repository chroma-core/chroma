import { expect, test } from "@jest/globals";
import { AdminClient } from "../src/AdminClient";
import adminClient from "./initAdminClient";
import { ChromaError } from "../src/Errors";

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
  try {
    await adminClient.createDatabase({ name: "test", tenantName: "foo" });
    expect(false).toBe(true);
  } catch (error) {
    expect(error).toBeInstanceOf(Error);
    expect(error).toBeInstanceOf(ChromaError);
  }
});
