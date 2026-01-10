# frozen_string_literal: true

module Chroma
  module EmbeddingFunctions
    class ChromaCloudQwenEmbeddingFunction
      NAME = "chroma-cloud-qwen"
      DEFAULT_MODEL = "Qwen/Qwen3-Embedding-0.6B"
      DEFAULT_INSTRUCTIONS = {
        "nl_to_code" => {
          "documents" => "",
          "query" => "Given a question about coding, retrieval code or passage that can solve user's question"
        }
      }.freeze

      attr_reader :model, :task, :instructions, :api_key_env_var

      def initialize(model: DEFAULT_MODEL, task: nil, instructions: DEFAULT_INSTRUCTIONS, api_key_env_var: "CHROMA_API_KEY")
        @api_key_env_var = api_key_env_var
        @api_key = ENV[@api_key_env_var] || Chroma::SharedState.cloud_api_key
        raise ArgumentError, "API key not found in #{@api_key_env_var} or any existing clients" if @api_key.nil? || @api_key.empty?

        @model = model
        @task = task
        @instructions = instructions

        @connection = Faraday.new(url: "https://embed.trychroma.com") do |builder|
          builder.headers["x-chroma-token"] = @api_key
          builder.headers["x-chroma-embedding-model"] = @model
          builder.headers["Content-Type"] = "application/json"
        end
      end

      def call(texts)
        return [] if texts.nil? || texts.empty?

        payload = {
          "instructions" => instruction_for("documents"),
          "texts" => texts
        }

        response = @connection.post do |req|
          req.body = JSON.generate(payload)
        end

        parse_response(response)
      end

      def embed_query(texts)
        return [] if texts.nil? || texts.empty?

        payload = {
          "instructions" => instruction_for("query"),
          "texts" => texts
        }

        response = @connection.post do |req|
          req.body = JSON.generate(payload)
        end

        parse_response(response)
      end

      def name
        NAME
      end

      def default_space
        "cosine"
      end

      def supported_spaces
        %w[cosine l2 ip]
      end

      def get_config
        {
          "api_key_env_var" => @api_key_env_var,
          "model" => @model,
          "task" => @task,
          "instructions" => @instructions
        }
      end

      def validate_config(config)
        EmbeddingFunctions.validate_config_schema(config, "chroma-cloud-qwen")
      end

      def self.build_from_config(config, client: nil)
        model = config["model"] || config[:model]
        task = config["task"] || config[:task]
        instructions = config["instructions"] || config[:instructions]
        api_key_env_var = config["api_key_env_var"] || config[:api_key_env_var] || "CHROMA_API_KEY"

        raise ArgumentError, "Config is missing required field 'model'" if model.nil?

        ChromaCloudQwenEmbeddingFunction.new(
          model: model,
          task: task,
          instructions: instructions || DEFAULT_INSTRUCTIONS,
          api_key_env_var: api_key_env_var,
        )
      end

      def self.validate_config(config)
        EmbeddingFunctions.validate_config_schema(config, "chroma-cloud-qwen")
      end

      def validate_config_update(old_config, new_config)
        %w[model task instructions].each do |key|
          next unless new_config.key?(key)
          raise ArgumentError, "The #{key} cannot be changed after initialization." if new_config[key] != old_config[key]
        end
      end

      private

      def instruction_for(target)
        return "" unless @task && @instructions[@task]
        target_instructions = @instructions[@task]
        target_instructions[target] || ""
      end

      def parse_response(response)
        unless response.success?
          raise RuntimeError, "Failed to get embeddings from Chroma Cloud API: HTTP #{response.status} - #{response.body}"
        end
        data = JSON.parse(response.body)
        unless data["embeddings"]
          raise RuntimeError, data["error"] || "Unknown error"
        end
        data["embeddings"]
      rescue JSON::ParserError
        raise RuntimeError, "Invalid JSON response from Chroma Cloud API"
      end
    end

    register_embedding_function(ChromaCloudQwenEmbeddingFunction::NAME, ChromaCloudQwenEmbeddingFunction)
  end
end
