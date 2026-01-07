# frozen_string_literal: true

module Chroma
  DEFAULT_TENANT = "default_tenant"
  DEFAULT_DATABASE = "default_database"

  class Client
    attr_reader :tenant, :database, :transport, :headers

    def initialize(host: "localhost", port: 8000, ssl: false, path: Chroma::HttpTransport::DEFAULT_API_PATH,
                   headers: nil, tenant: nil, database: nil, ssl_verify: true, timeout: nil, use_defaults: true)
      @host = host
      @port = port
      @ssl = ssl
      @path = path

      @transport = Chroma::HttpTransport.new(
        host: host,
        port: port,
        ssl: ssl,
        path: path,
        headers: headers,
        ssl_verify: ssl_verify,
        timeout: timeout,
      )
      @headers = @transport.headers

      env_tenant = ENV["CHROMA_TENANT"]
      env_database = ENV["CHROMA_DATABASE"]

      if use_defaults
        @tenant = tenant || env_tenant || DEFAULT_TENANT
        @database = database || env_database || DEFAULT_DATABASE
      else
        @tenant = tenant || env_tenant
        @database = database || env_database
      end

      register_cloud_api_key_from_headers
    end

    def heartbeat
      response = @transport.request(:get, "/heartbeat")
      response["nanosecond heartbeat"] || response["nanosecond_heartbeat"] || response
    end

    def get_version
      @transport.request(:get, "/version")
    end

    def get_settings
      {
        host: @host,
        port: @port,
        ssl: @ssl,
        path: @path,
        tenant: @tenant,
        database: @database
      }
    end

    def get_user_identity
      @transport.request(:get, "/auth/identity")
    end

    def get_max_batch_size
      @transport.max_batch_size
    end

    def supports_base64_encoding?
      @transport.supports_base64_encoding?
    end

    def reset
      @transport.request(:post, "/reset")
    end

    def set_tenant(tenant, database: nil)
      db = database || @database || DEFAULT_DATABASE
      validate_tenant_database(tenant, db)
      @tenant = tenant
      @database = db
    end

    def set_database(database)
      validate_tenant_database(@tenant || DEFAULT_TENANT, database)
      @database = database
    end

    def create_database(name, tenant: nil)
      tenant = resolve_tenant_name(tenant)
      @transport.request(:post, "/tenants/#{tenant}/databases", json: { "name" => name })
      nil
    end

    def get_database(name, tenant: nil)
      tenant = resolve_tenant_name(tenant)
      @transport.request(:get, "/tenants/#{tenant}/databases/#{name}")
    end

    def delete_database(name, tenant: nil)
      tenant = resolve_tenant_name(tenant)
      @transport.request(:delete, "/tenants/#{tenant}/databases/#{name}")
      nil
    end

    def list_databases(limit: nil, offset: nil, tenant: nil)
      tenant = resolve_tenant_name(tenant)
      @transport.request(:get, "/tenants/#{tenant}/databases", params: { limit: limit, offset: offset })
    end

    def create_tenant(name)
      @transport.request(:post, "/tenants", json: { "name" => name })
      nil
    end

    def get_tenant(name = nil)
      tenant = name || @tenant
      raise ArgumentError, "Tenant name must be provided" if tenant.nil? || tenant.to_s.empty?
      @transport.request(:get, "/tenants/#{tenant}")
    end

    def list_collections(limit: nil, offset: nil)
      path = tenant_database_path
      response = @transport.request(
        :get,
        "/tenants/#{path[:tenant]}/databases/#{path[:database]}/collections",
        params: { limit: limit, offset: offset },
      )
      response.map { |collection| build_collection(collection) }
    end

    def count_collections
      path = tenant_database_path
      @transport.request(
        :get,
        "/tenants/#{path[:tenant]}/databases/#{path[:database]}/collections_count",
      )
    end

    def create_collection(name:, metadata: nil, embedding_function: nil, configuration: nil, schema: nil, get_or_create: false)
      path = tenant_database_path

      payload = { "name" => name, "get_or_create" => get_or_create }
      payload["metadata"] = Chroma::Types::Validation.serialize_metadata(metadata) if metadata

      config_payload = configuration_to_payload(configuration)
      if embedding_function
        config_payload["embedding_function"] = EmbeddingFunctions.prepare_embedding_function_config(embedding_function)
      end
      payload["configuration"] = config_payload unless config_payload.empty?

      payload["schema"] = schema.is_a?(Schema) ? schema.serialize_to_json : schema if schema

      response = @transport.request(
        :post,
        "/tenants/#{path[:tenant]}/databases/#{path[:database]}/collections",
        json: payload,
      )

      build_collection(response, embedding_function: embedding_function, schema_override: schema)
    end

    def get_collection(name: nil, id: nil)
      identifier = id || name
      raise ArgumentError, "Collection name or id must be provided" unless identifier

      path = tenant_database_path
      response = @transport.request(
        :get,
        "/tenants/#{path[:tenant]}/databases/#{path[:database]}/collections/#{identifier}",
      )
      build_collection(response)
    end

    def get_or_create_collection(name:, metadata: nil, embedding_function: nil, configuration: nil, schema: nil)
      create_collection(
        name: name,
        metadata: metadata,
        embedding_function: embedding_function,
        configuration: configuration,
        schema: schema,
        get_or_create: true,
      )
    end

    def delete_collection(name: nil, id: nil)
      identifier = id || name
      raise ArgumentError, "Collection name or id must be provided" unless identifier

      path = tenant_database_path
      @transport.request(
        :delete,
        "/tenants/#{path[:tenant]}/databases/#{path[:database]}/collections/#{identifier}",
      )
      nil
    end

    def update_collection(id:, name: nil, metadata: nil, configuration: nil)
      path = tenant_database_path
      payload = {}
      payload["name"] = name if name
      payload["metadata"] = Chroma::Types::Validation.serialize_metadata(metadata) if metadata
      config_payload = configuration_to_payload(configuration)
      payload["configuration"] = config_payload unless config_payload.empty?

      @transport.request(
        :put,
        "/tenants/#{path[:tenant]}/databases/#{path[:database]}/collections/#{id}",
        json: payload,
      )
      nil
    end

    def tenant_database_path
      resolve_tenant_database
    end

    private

    def resolve_tenant_database
      return { tenant: @tenant, database: @database } if @tenant && @database

      identity = get_user_identity
      tenant = identity["tenant"]
      databases = Array(identity["databases"]).uniq
      if databases.empty?
        raise Chroma::AuthorizationError, "Your API key does not have access to any databases for tenant #{tenant}"
      end
      if databases.length > 1 || databases[0] == "*"
        raise Chroma::InvalidArgumentError,
              "Your API key is scoped to more than 1 DB. Please provide a DB name to the CloudClient constructor"
      end

      @tenant = tenant
      @database = databases[0]
      { tenant: @tenant, database: @database }
    end

    def resolve_tenant_name(tenant)
      return tenant if tenant
      return @tenant if @tenant

      begin
        identity = get_user_identity
        resolved = identity.is_a?(Hash) ? identity["tenant"] : nil
        @tenant = resolved if resolved
      rescue StandardError
        # ignore and fall back to default tenant
      end

      @tenant || DEFAULT_TENANT
    end

    def configuration_to_payload(configuration)
      return {} if configuration.nil?
      return configuration.to_h if configuration.respond_to?(:to_h)
      configuration
    end

    def build_collection(model, embedding_function: nil, schema_override: nil)
      schema_json = schema_override.is_a?(Schema) ? schema_override : Schema.deserialize_from_json(model["schema"], client: self)
      config_json = model["configuration_json"] || model["configuration"] || {}
      ef_config = config_json["embedding_function"]
      ef_instance = embedding_function || EmbeddingFunctions.build_embedding_function(ef_config, client: self)

      Collection.new(
        client: self,
        model: model,
        embedding_function: ef_instance,
        schema: schema_json,
      )
    end

    def validate_tenant_database(tenant, database)
      get_tenant(tenant)
      get_database(database, tenant: tenant)
    rescue Faraday::ConnectionFailed
      raise ArgumentError, "Could not connect to a Chroma server. Are you sure it is running?"
    rescue Chroma::ChromaError => e
      raise e
    rescue StandardError
      raise ArgumentError, "Could not connect to tenant #{tenant}. Are you sure it exists?"
    end

    def register_cloud_api_key_from_headers
      token = @headers["x-chroma-token"] || @headers["X-Chroma-Token"]
      Chroma::SharedState.register_cloud_api_key(token)
    end
  end

  class HttpClient < Client
  end

  class CloudClient < Client
    def initialize(api_key: nil, headers: nil, tenant: nil, database: nil, ssl_verify: true, timeout: nil)
      config = Chroma.configuration
      api_key ||= config.cloud_api_key
      raise ArgumentError, "CHROMA_API_KEY is required for CloudClient" if api_key.nil? || api_key.empty?

      tenant ||= config.cloud_tenant
      database ||= config.cloud_database

      merged_headers = (headers || {}).dup
      merged_headers["x-chroma-token"] ||= api_key

      super(
        host: config.cloud_host,
        port: config.cloud_port,
        ssl: config.cloud_ssl,
        headers: merged_headers,
        tenant: tenant,
        database: database,
        ssl_verify: ssl_verify,
        timeout: timeout,
        use_defaults: false,
      )
    end
  end
end
