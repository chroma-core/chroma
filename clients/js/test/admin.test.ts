import { expect, test } from "@jest/globals";
import { AdminClient } from "../src/AdminClient";
import adminClient from "./initAdminClient";

test("it should create the admin client connection", async () => {
    expect(adminClient).toBeDefined();
    expect(adminClient).toBeInstanceOf(AdminClient);
});

test("it should create and get a tenant", async () => {
    await adminClient.createTenant({ name: "testTenant" });
    const tenant = await adminClient.getTenant({ name: "testTenant" });
    expect(tenant).toBeDefined();
    expect(tenant).toHaveProperty('name')
    expect(tenant.name).toBe("testTenant");
})

test("it should create and get a database for a tenant", async () => {
    await adminClient.createTenant({ name: "test3" });
    const database = await adminClient.createDatabase({ name: "test", tenantName: "test3" });
    expect(database).toBeDefined();
    expect(database).toHaveProperty('name')
    expect(database.name).toBe("test");

    const getDatabase = await adminClient.getDatabase({ name: "test", tenantName: "test3" });
    expect(getDatabase).toBeDefined();
    expect(getDatabase).toHaveProperty('name')
    expect(getDatabase.name).toBe("test");
})

// test("it should get the version", async () => {
//     const version = await chroma.version();
//     expect(version).toBeDefined();
//     expect(version).toMatch(/^[0-9]+\.[0-9]+\.[0-9]+$/);
// });
