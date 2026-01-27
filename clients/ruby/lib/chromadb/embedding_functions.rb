# frozen_string_literal: true

require "json_schemer"
require "porter2stemmer"

module Chroma
  module EmbeddingFunctions
    KNOWN_DENSE = {}
    KNOWN_SPARSE = {}

    module_function

    def register_embedding_function(name, klass)
      raise ArgumentError, "Embedding function #{name} already registered" if KNOWN_DENSE.key?(name)
      KNOWN_DENSE[name] = klass
    end

    def register_sparse_embedding_function(name, klass)
      raise ArgumentError, "Sparse embedding function #{name} already registered" if KNOWN_SPARSE.key?(name)
      KNOWN_SPARSE[name] = klass
    end

    def resolve_name(fn)
      return nil unless fn
      if fn.respond_to?(:name)
        value = fn.name
        return value if value.is_a?(String)
      end
      nil
    end

    def prepare_embedding_function_config(fn)
      return { "type" => "legacy" } if fn.nil?

      name = resolve_name(fn)
      return { "type" => "legacy" } if name.nil?
      return { "type" => "legacy" } unless fn.respond_to?(:get_config) && fn.class.respond_to?(:build_from_config)

      config = fn.get_config
      if fn.respond_to?(:validate_config)
        fn.validate_config(config)
      elsif fn.class.respond_to?(:validate_config)
        fn.class.validate_config(config)
      end

      { "type" => "known", "name" => name, "config" => config }
    end

    def build_embedding_function(ef_config, client: nil)
      return nil if ef_config.nil?
      return nil unless ef_config.is_a?(Hash)
      return nil unless ef_config["type"] == "known"

      name = ef_config["name"]
      config = ef_config["config"] || {}

      klass = KNOWN_DENSE[name]
      return nil unless klass

      if klass.respond_to?(:build_from_config)
        return klass.build_from_config(config, client: client)
      end

      nil
    end

    def build_sparse_embedding_function(ef_config, client: nil)
      return nil if ef_config.nil?
      return nil unless ef_config.is_a?(Hash)
      return nil unless ef_config["type"] == "known"

      name = ef_config["name"]
      config = ef_config["config"] || {}

      klass = KNOWN_SPARSE[name]
      return nil unless klass

      if klass.respond_to?(:build_from_config)
        return klass.build_from_config(config, client: client)
      end

      nil
    end

    def validate_config_schema(config, schema_name)
      schemer = schema_for(schema_name)
      errors = schemer.validate(config).to_a
      return if errors.empty?

      messages = errors.map { |err| err["message"] }.uniq
      raise ArgumentError, "Invalid configuration for #{schema_name}: #{messages.join('; ')}"
    end

    def schema_for(schema_name)
      @schemers ||= {}
      return @schemers[schema_name] if @schemers[schema_name]

      schema_path = File.expand_path("schemas/#{schema_name}.json", __dir__)
      schema = JSON.parse(File.read(schema_path))
      @schemers[schema_name] = JSONSchemer.schema(schema)
    end
  end

  module SharedState
    @cloud_api_key = nil

    module_function

    def register_cloud_api_key(key)
      @cloud_api_key = key if key && !key.to_s.empty?
    end

    def cloud_api_key
      @cloud_api_key
    end
  end
end

require_relative "embedding_functions/chroma_cloud_qwen"
require_relative "embedding_functions/chroma_cloud_splade"
require_relative "embedding_functions/chroma_bm25"
