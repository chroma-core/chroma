# frozen_string_literal: true

module Chroma
  module EmbeddingFunctions
    class ChromaCloudSpladeEmbeddingFunction
      NAME = "chroma-cloud-splade"
      DEFAULT_MODEL = "prithivida/Splade_PP_en_v1"

      attr_reader :api_key_env_var, :model, :include_tokens

      def initialize(api_key_env_var: "CHROMA_API_KEY", model: DEFAULT_MODEL, include_tokens: false)
        @api_key_env_var = api_key_env_var
        @api_key = ENV[@api_key_env_var] || Chroma::SharedState.cloud_api_key
        raise ArgumentError, "API key not found in #{@api_key_env_var} or any existing clients" if @api_key.nil? || @api_key.empty?

        @model = model
        @include_tokens = !!include_tokens

        @connection = Faraday.new(url: "https://embed.trychroma.com") do |builder|
          builder.headers["x-chroma-token"] = @api_key
          builder.headers["x-chroma-embedding-model"] = @model
          builder.headers["Content-Type"] = "application/json"
        end
      end

      def call(texts)
        return [] if texts.nil? || texts.empty?

        payload = {
          "texts" => texts,
          "task" => "",
          "target" => "",
          "fetch_tokens" => @include_tokens ? "true" : "false"
        }

        response = @connection.post("/embed_sparse") do |req|
          req.body = JSON.generate(payload)
        end

        parse_response(response)
      end

      def embed_query(texts)
        call(texts)
      end

      def name
        NAME
      end

      def get_config
        {
          "api_key_env_var" => @api_key_env_var,
          "model" => @model,
          "include_tokens" => @include_tokens
        }
      end

      def validate_config(config)
        EmbeddingFunctions.validate_config_schema(config, "chroma-cloud-splade")
      end

      def validate_config_update(old_config, new_config)
        %w[include_tokens model].each do |key|
          next unless new_config.key?(key)
          raise ArgumentError, "Updating '#{key}' is not supported for chroma-cloud-splade" if new_config[key] != old_config[key]
        end
      end

      def self.build_from_config(config, client: nil)
        api_key_env_var = config["api_key_env_var"] || config[:api_key_env_var]
        model = config["model"] || config[:model]
        include_tokens = config.key?("include_tokens") ? config["include_tokens"] : config[:include_tokens]

        raise ArgumentError, "model must be provided in config" if model.nil?
        raise ArgumentError, "api_key_env_var must be provided in config" if api_key_env_var.nil? || api_key_env_var.to_s.empty?

        ChromaCloudSpladeEmbeddingFunction.new(
          api_key_env_var: api_key_env_var,
          model: model,
          include_tokens: include_tokens || false,
        )
      end

      def self.validate_config(config)
        EmbeddingFunctions.validate_config_schema(config, "chroma-cloud-splade")
      end

      private

      def parse_response(response)
        unless response.success?
          raise RuntimeError, "Failed to get embeddings from Chroma Cloud API: HTTP #{response.status} - #{response.body}"
        end
        data = JSON.parse(response.body)
        raw_embeddings = data["embeddings"] || []

        raw_embeddings.map do |embedding|
          if embedding.is_a?(Hash)
            indices = embedding["indices"] || []
            values = embedding["values"] || []
            labels = @include_tokens ? (embedding["labels"] || embedding["tokens"]) : nil
            Types::Validation.normalize_sparse_vector(indices: indices, values: values, labels: labels)
          elsif embedding.is_a?(Types::SparseVector)
            Types::Validation.normalize_sparse_vector(
              indices: embedding.indices,
              values: embedding.values,
              labels: @include_tokens ? embedding.labels : nil,
            )
          else
            raise ArgumentError, "Unexpected sparse embedding format: #{embedding.inspect}"
          end
        end
      rescue JSON::ParserError
        raise RuntimeError, "Invalid JSON response from Chroma Cloud API"
      end
    end

    register_sparse_embedding_function(ChromaCloudSpladeEmbeddingFunction::NAME, ChromaCloudSpladeEmbeddingFunction)
  end
end
