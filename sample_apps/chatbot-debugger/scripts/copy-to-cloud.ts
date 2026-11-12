import {
  AdminClient,
  ChromaClient,
  Collection,
  DefaultEmbeddingFunction,
  Embeddings,
  IncludeEnum,
  Metadata,
} from "chromadb";
import { Result } from "@/lib/types";
import "dotenv/config";
import { getOpenAIEF } from "@/lib/ai-utils";
import { getAppParams } from "@/lib/utils";
import ora from "ora";
import { CLOUD_HOST } from "@/scripts/utils";
import yargs from "yargs";
import { hideBin } from "yargs/helpers";

/*
 * Use this script to copy the collections from your local Chroma DB to a Chroma Cloud.
 *
 * Make sure that your .env file is set properly to connect to Chroma Cloud:
 * - CHROMA_HOST = api.trychroma.com:8000
 * - CHROMA_TENANT = [Set to your tenant ID]
 * - CHROMA_DB_NAME = [The name you want for your new Chroma Cloud DB]
 * - CHROMA_CLOUD_API_KEY = [Your Chroma Cloud API key]
 *
 * The script assumes that your local Chroma DB is available at http://localhost:8000, with
 * `default_tenant` and `default_database` as the tenant and DB values respectively.
 * If you used other settings, please update the defaults args in this file, or pass your desired values
 * to the script (see example below).
 */

const argv = yargs(hideBin(process.argv))
  .usage(
    "Usage: npm run copy-to-cloud -- --host [local_host] --tenant [local_tenant] --db [local_db_name]",
  )
  .options({
    host: {
      description: "The host address for your local Chroma server",
      type: "string",
      demandOption: true,
      default: "http://localhost:8000",
    },
    tenant: {
      description: "The tenant in your local Chroma server",
      alias: "t",
      type: "string",
      demandOption: true,
      default: "default_tenant",
    },
    db: {
      description: "The name of your local Chroma DB",
      alias: "db",
      type: "string",
      demandOption: true,
      default: "default_database",
    },
  })
  .example(
    "npm run copy-to-cloud -- --host http://localhost:8000 --tenant default_tenant --db default_database",
    "Copy collections from Chroma DB at http://localhost:8000",
  )
  .help()
  .alias("help", "h").argv as {
  host: string;
  tenant: string;
  db: string;
};

const getLocalClient = async (
  path: string,
  tenant: string,
  database: string,
): Promise<Result<ChromaClient, Error>> => {
  const chromaClient = new ChromaClient({ path, tenant, database });
  try {
    await chromaClient.heartbeat();
    return { ok: true, value: chromaClient };
  } catch {
    return {
      ok: false,
      error: new Error(
        `Could not connect to your local Chroma server at ${path}`,
      ),
    };
  }
};

const getCloudClients = async (): Promise<
  Result<{ chromaClient: ChromaClient; adminClient: AdminClient }, Error>
> => {
  const appParamsResult = getAppParams();
  if (!appParamsResult.ok) {
    return appParamsResult;
  }

  if (appParamsResult.value.chromaClientParams.path !== CLOUD_HOST) {
    return {
      ok: false,
      error: new Error(
        `Your CHROMA_HOST environment variable should be set to ${CLOUD_HOST} in order to connect successfully to Chroma Cloud`,
      ),
    };
  }

  try {
    const adminClient = new AdminClient({
      ...appParamsResult.value.chromaClientParams,
    });

    await adminClient.listDatabases({
      limit: 1,
      tenantName: adminClient.tenant,
    });

    const chromaClient = new ChromaClient({
      ...appParamsResult.value.chromaClientParams,
    });

    await chromaClient.heartbeat();

    return { ok: true, value: { chromaClient, adminClient } };
  } catch {
    return {
      ok: false,
      error: new Error(
        "Could not connect to Chroma Cloud. Make sure your CHROMA_CLOUD_API_KEY, CHROMA_DB_NAME, and CHROMA_TENANT are set correctly in your .env file",
      ),
    };
  }
};

const setUpCloudDB = async (
  adminClient: AdminClient,
): Promise<Result<string, Error>> => {
  try {
    await adminClient.getDatabase({
      name: adminClient.database!,
      tenantName: adminClient.tenant,
    });
    return {
      ok: false,
      error: new Error(
        `Tenant ${adminClient.tenant} already has a DB with name ${adminClient.database}. You can either delete this DB or change the value for the DB_NAME constant in copy-to-cloud.ts`,
      ),
    };
  } catch {
    try {
      const db = await adminClient.createDatabase({
        name: adminClient.database,
        tenantName: adminClient.tenant,
      });
      return { ok: true, value: db.name };
    } catch {
      return {
        ok: false,
        error: new Error(
          `Failed to create DB ${adminClient.database} for tenant ${adminClient.tenant}`,
        ),
      };
    }
  }
};

const copyCollections = async (
  localClient: ChromaClient,
  cloudClient: ChromaClient,
): Promise<Result<string[], Error>> => {
  let collections: string[];
  try {
    collections = await localClient.listCollections();
  } catch {
    return { ok: false, error: new Error("Failed to list local collections") };
  }

  const spinner = ora(`Copying ${collections.length} collections...`).start();
  for (const collectionName of collections) {
    let embeddingFunction;
    if (collectionName.endsWith("-data")) {
      const openAIEFResult = await getOpenAIEF();
      if (!openAIEFResult.ok) {
        return openAIEFResult;
      }
      embeddingFunction = openAIEFResult.value;
    } else {
      embeddingFunction = new DefaultEmbeddingFunction();
    }

    let localCollection: Collection;
    let cloudCollection: Collection;

    try {
      localCollection = await localClient.getCollection({
        name: collectionName,
        embeddingFunction,
      });
    } catch {
      return {
        ok: false,
        error: new Error(`Failed to get local collection ${collectionName}`),
      };
    }

    try {
      cloudCollection = await cloudClient.createCollection({
        name: collectionName,
        embeddingFunction,
      });
    } catch {
      return {
        ok: false,
        error: new Error(
          `Failed to create collection ${collectionName} in DB ${cloudClient.database} for tenant ${cloudClient.tenant}`,
        ),
      };
    }

    try {
      const batchSize = 100;
      const collectionSize = await localCollection.count();

      for (let i = 0; i < collectionSize; i += batchSize) {
        const records = await localCollection.get({
          limit: batchSize,
          offset: i,
          include: [
            IncludeEnum.Documents,
            IncludeEnum.Embeddings,
            IncludeEnum.Metadatas,
          ],
        });

        const { ids, documents, metadatas, embeddings } = records;
        if (ids.length != documents.filter((d) => d !== null).length) {
          return {
            ok: false,
            error: new Error(
              `Some IDs in collection ${collectionName} do not have associated documents`,
            ),
          };
        }

        await cloudCollection.add({
          ids,
          documents: documents as string[],
          metadatas: metadatas as Metadata[],
          embeddings: embeddings as Embeddings,
        });
      }
    } catch {
      return {
        ok: false,
        error: new Error(`Failed to copy collection ${collectionName}`),
      };
    } finally {
      spinner.stop();
    }
  }
  console.log("Copied local Chroma DB to Chroma Cloud!");
  return { ok: true, value: collections };
};

const main = async () => {
  const localClientResult = await getLocalClient(
    argv.host,
    argv.tenant,
    argv.db,
  );
  if (!localClientResult.ok) {
    console.error(localClientResult.error.message);
    return;
  }

  const cloudClientsResult = await getCloudClients();
  if (!cloudClientsResult.ok) {
    console.error(cloudClientsResult.error.message);
    return;
  }

  const localClient = localClientResult.value;
  const cloudClient = cloudClientsResult.value.chromaClient;
  const cloudAdminClient = cloudClientsResult.value.adminClient;

  console.log("\nCopying local Chroma DB to Chroma Cloud!\n");
  console.log(`Local Chroma DB: ${argv.host}`);
  console.log(`Local Chroma tenant: ${argv.tenant}`);
  console.log(`Local DB name: ${argv.db}\n`);
  console.log(`Chroma Cloud tenant: ${cloudClient.tenant}`);
  console.log(`Chroma Cloud DB: ${cloudClient.database}`);
  console.log("--------------------------------------------\n");

  const setupDBResult = await setUpCloudDB(cloudAdminClient);
  if (!setupDBResult.ok) {
    console.error(setupDBResult.error.message);
    return;
  }

  const result = await copyCollections(localClient, cloudClient);
  if (!result.ok) {
    console.error(result.error.message);
    console.log();
  }
};

main().finally();
