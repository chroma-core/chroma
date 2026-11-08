import {
  ChromaValueError,
  EmbeddingFunction,
  EmbeddingFunctionSpace,
  registerEmbeddingFunction,
} from "chromadb";
import { validateConfigSchema } from "@chroma-core/ai-embeddings-common";
import * as common from "oci-common";
import * as generativeaiinference from "oci-generativeaiinference";

const NAME = "oci-genai";

// The OCI Generative AI EmbedText API rejects requests with more than 96 inputs.
const MAX_BATCH_SIZE = 96;

export type OCIGenAIAuthType =
  | "API_KEY"
  | "SECURITY_TOKEN"
  | "INSTANCE_PRINCIPAL"
  | "RESOURCE_PRINCIPAL";

export type OCIGenAITruncate = "NONE" | "START" | "END";

export type OCIGenAIInputType =
  | "SEARCH_DOCUMENT"
  | "SEARCH_QUERY"
  | "CLASSIFICATION"
  | "CLUSTERING"
  | "IMAGE";

const AUTH_TYPES: OCIGenAIAuthType[] = [
  "API_KEY",
  "SECURITY_TOKEN",
  "INSTANCE_PRINCIPAL",
  "RESOURCE_PRINCIPAL",
];
const TRUNCATE_MODES: OCIGenAITruncate[] = ["NONE", "START", "END"];
const INPUT_TYPES: OCIGenAIInputType[] = [
  "SEARCH_DOCUMENT",
  "SEARCH_QUERY",
  "CLASSIFICATION",
  "CLUSTERING",
  "IMAGE",
];

export interface OCIGenAIConfig {
  model_name: string;
  compartment_id: string;
  service_endpoint?: string | null;
  auth_type?: string;
  auth_profile?: string;
  auth_file_location?: string;
  truncate?: string;
  input_type?: string | null;
  output_dimensions?: number | null;
}

export interface OCIGenAIArgs {
  /** OCI Generative AI embedding model, e.g. "cohere.embed-v4.0" (default). */
  modelName?: string;
  /** OCID of the compartment authorized to call the Generative AI service. Required. */
  compartmentId: string;
  /**
   * Inference endpoint, e.g. "https://inference.generativeai.us-chicago-1.oci.oraclecloud.com".
   * Derived from the region of the OCI profile when omitted.
   */
  serviceEndpoint?: string;
  /** How to authenticate. Defaults to "API_KEY" (config file + API signing key). */
  authType?: OCIGenAIAuthType;
  /** Profile in the OCI config file (API_KEY / SECURITY_TOKEN). Defaults to "DEFAULT". */
  authProfile?: string;
  /** Path of the OCI config file (API_KEY / SECURITY_TOKEN). Defaults to "~/.oci/config". */
  authFileLocation?: string;
  /** Truncation strategy for inputs longer than the model context. Defaults to "END". */
  truncate?: OCIGenAITruncate;
  /**
   * Pin a single input type for every request. When omitted, documents use
   * SEARCH_DOCUMENT and queries use SEARCH_QUERY.
   */
  inputType?: OCIGenAIInputType;
  /** Requested embedding size for models that support it (e.g. 256/512/1024/1536 for cohere.embed-v4.0). */
  outputDimensions?: number;
}

/**
 * Embedding function backed by Oracle Cloud Infrastructure (OCI) Generative AI.
 *
 * Credentials are never stored in the collection configuration; only their
 * location (config file, profile) or the principal type is persisted.
 */
export class OCIGenAIEmbeddingFunction implements EmbeddingFunction {
  public readonly name = NAME;

  private readonly modelName: string;
  private readonly compartmentId: string;
  private readonly serviceEndpoint: string | undefined;
  private readonly authType: OCIGenAIAuthType;
  private readonly authProfile: string;
  private readonly authFileLocation: string;
  private readonly truncate: OCIGenAITruncate;
  private readonly inputType: OCIGenAIInputType | undefined;
  private readonly outputDimensions: number | undefined;

  // Created lazily: instance principals need an async builder and we do not
  // want to touch credentials until the first request.
  private clientPromise:
    | Promise<generativeaiinference.GenerativeAiInferenceClient>
    | undefined;

  constructor(args: OCIGenAIArgs) {
    const {
      modelName = "cohere.embed-v4.0",
      compartmentId,
      serviceEndpoint,
      authType = "API_KEY",
      authProfile = "DEFAULT",
      authFileLocation = "~/.oci/config",
      truncate = "END",
      inputType,
      outputDimensions,
    } = args ?? {};

    if (!compartmentId) {
      throw new ChromaValueError(
        "compartmentId is required: pass the OCID of the compartment that is authorized to call the OCI Generative AI service.",
      );
    }

    const normalizedAuthType = String(
      authType,
    ).toUpperCase() as OCIGenAIAuthType;
    if (!AUTH_TYPES.includes(normalizedAuthType)) {
      throw new ChromaValueError(
        `Unsupported authType '${authType}'. Expected one of ${AUTH_TYPES.join(", ")}.`,
      );
    }

    const normalizedTruncate = String(
      truncate,
    ).toUpperCase() as OCIGenAITruncate;
    if (!TRUNCATE_MODES.includes(normalizedTruncate)) {
      throw new ChromaValueError(
        `Unsupported truncate '${truncate}'. Expected one of ${TRUNCATE_MODES.join(", ")}.`,
      );
    }

    let normalizedInputType: OCIGenAIInputType | undefined;
    if (inputType !== undefined && inputType !== null) {
      normalizedInputType = String(
        inputType,
      ).toUpperCase() as OCIGenAIInputType;
      if (!INPUT_TYPES.includes(normalizedInputType)) {
        throw new ChromaValueError(
          `Unsupported inputType '${inputType}'. Expected one of ${INPUT_TYPES.join(", ")}.`,
        );
      }
    }

    if (outputDimensions !== undefined && outputDimensions <= 0) {
      throw new ChromaValueError(
        "outputDimensions must be a positive integer.",
      );
    }

    this.modelName = modelName;
    this.compartmentId = compartmentId;
    this.serviceEndpoint = serviceEndpoint;
    this.authType = normalizedAuthType;
    this.authProfile = authProfile;
    this.authFileLocation = authFileLocation;
    this.truncate = normalizedTruncate;
    this.inputType = normalizedInputType;
    this.outputDimensions = outputDimensions;
  }

  private async createClient(): Promise<generativeaiinference.GenerativeAiInferenceClient> {
    let provider: common.AuthenticationDetailsProvider;
    switch (this.authType) {
      case "API_KEY":
        provider = new common.ConfigFileAuthenticationDetailsProvider(
          this.authFileLocation,
          this.authProfile,
        );
        break;
      case "SECURITY_TOKEN":
        provider = new common.SessionAuthDetailProvider(
          this.authFileLocation,
          this.authProfile,
        );
        break;
      case "INSTANCE_PRINCIPAL":
        provider =
          await new common.InstancePrincipalsAuthenticationDetailsProviderBuilder().build();
        break;
      case "RESOURCE_PRINCIPAL":
        provider =
          common.ResourcePrincipalAuthenticationDetailsProvider.builder();
        break;
    }

    const client = new generativeaiinference.GenerativeAiInferenceClient({
      authenticationDetailsProvider: provider,
    });
    if (this.serviceEndpoint) {
      client.endpoint = this.serviceEndpoint;
    }
    return client;
  }

  private getClient(): Promise<generativeaiinference.GenerativeAiInferenceClient> {
    if (!this.clientPromise) {
      this.clientPromise = this.createClient();
    }
    return this.clientPromise;
  }

  private async embed(
    texts: string[],
    inputType: OCIGenAIInputType,
  ): Promise<number[][]> {
    if (texts.length === 0) {
      return [];
    }

    const client = await this.getClient();
    const embeddings: number[][] = [];

    for (let start = 0; start < texts.length; start += MAX_BATCH_SIZE) {
      const batch = texts.slice(start, start + MAX_BATCH_SIZE);
      const servingMode: generativeaiinference.models.OnDemandServingMode = {
        servingType:
          generativeaiinference.models.OnDemandServingMode.servingType,
        modelId: this.modelName,
      };
      const embedTextDetails: generativeaiinference.models.EmbedTextDetails = {
        inputs: batch,
        servingMode,
        compartmentId: this.compartmentId,
        truncate: this
          .truncate as generativeaiinference.models.EmbedTextDetails.Truncate,
        inputType:
          inputType as generativeaiinference.models.EmbedTextDetails.InputType,
        ...(this.outputDimensions !== undefined && {
          outputDimensions: this.outputDimensions,
        }),
      };

      let response: generativeaiinference.responses.EmbedTextResponse;
      try {
        response = await client.embedText({ embedTextDetails });
      } catch (e) {
        throw new Error(
          `Failed to generate OCI Generative AI embeddings: ${e}`,
        );
      }

      const result = response.embedTextResult?.embeddings;
      if (!result || result.length !== batch.length) {
        throw new Error("Failed to generate OCI Generative AI embeddings");
      }
      embeddings.push(...result);
    }

    return embeddings;
  }

  public async generate(texts: string[]): Promise<number[][]> {
    return this.embed(texts, this.inputType ?? "SEARCH_DOCUMENT");
  }

  public async generateForQueries(texts: string[]): Promise<number[][]> {
    return this.embed(texts, this.inputType ?? "SEARCH_QUERY");
  }

  public static buildFromConfig(
    config: OCIGenAIConfig,
  ): OCIGenAIEmbeddingFunction {
    return new OCIGenAIEmbeddingFunction({
      modelName: config.model_name,
      compartmentId: config.compartment_id,
      serviceEndpoint: config.service_endpoint ?? undefined,
      authType: (config.auth_type ?? "API_KEY") as OCIGenAIAuthType,
      authProfile: config.auth_profile ?? "DEFAULT",
      authFileLocation: config.auth_file_location ?? "~/.oci/config",
      truncate: (config.truncate ?? "END") as OCIGenAITruncate,
      inputType: (config.input_type ?? undefined) as
        | OCIGenAIInputType
        | undefined,
      outputDimensions: config.output_dimensions ?? undefined,
    });
  }

  public defaultSpace(): EmbeddingFunctionSpace {
    return "cosine";
  }

  public supportedSpaces(): EmbeddingFunctionSpace[] {
    return ["cosine", "l2", "ip"];
  }

  public getConfig(): OCIGenAIConfig {
    return {
      model_name: this.modelName,
      compartment_id: this.compartmentId,
      service_endpoint: this.serviceEndpoint ?? null,
      auth_type: this.authType,
      auth_profile: this.authProfile,
      auth_file_location: this.authFileLocation,
      truncate: this.truncate,
      input_type: this.inputType ?? null,
      output_dimensions: this.outputDimensions ?? null,
    };
  }

  public validateConfigUpdate(newConfig: Record<string, any>): void {
    if (this.getConfig().model_name !== newConfig.model_name) {
      throw new ChromaValueError("Model name cannot be updated");
    }
    if (
      (this.getConfig().output_dimensions ?? null) !==
      (newConfig.output_dimensions ?? null)
    ) {
      throw new ChromaValueError("Output dimensions cannot be updated");
    }
  }

  public static validateConfig(config: OCIGenAIConfig): void {
    validateConfigSchema(config, NAME);
  }
}

registerEmbeddingFunction(NAME, OCIGenAIEmbeddingFunction);
