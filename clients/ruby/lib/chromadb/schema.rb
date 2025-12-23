# frozen_string_literal: true

module Chroma
  DOCUMENT_KEY = "#document"
  EMBEDDING_KEY = "#embedding"

  STRING_VALUE_NAME = "string"
  FLOAT_LIST_VALUE_NAME = "float_list"
  SPARSE_VECTOR_VALUE_NAME = "sparse_vector"
  INT_VALUE_NAME = "int"
  FLOAT_VALUE_NAME = "float"
  BOOL_VALUE_NAME = "bool"

  FTS_INDEX_NAME = "fts_index"
  STRING_INVERTED_INDEX_NAME = "string_inverted_index"
  VECTOR_INDEX_NAME = "vector_index"
  SPARSE_VECTOR_INDEX_NAME = "sparse_vector_index"
  INT_INVERTED_INDEX_NAME = "int_inverted_index"
  FLOAT_INVERTED_INDEX_NAME = "float_inverted_index"
  BOOL_INVERTED_INDEX_NAME = "bool_inverted_index"

  class FtsIndexConfig
    def type = "FtsIndexConfig"
  end

  class StringInvertedIndexConfig
    def type = "StringInvertedIndexConfig"
  end

  class IntInvertedIndexConfig
    def type = "IntInvertedIndexConfig"
  end

  class FloatInvertedIndexConfig
    def type = "FloatInvertedIndexConfig"
  end

  class BoolInvertedIndexConfig
    def type = "BoolInvertedIndexConfig"
  end

  class VectorIndexConfig
    attr_accessor :space, :embedding_function, :source_key, :hnsw, :spann

    def initialize(space: nil, embedding_function: nil, source_key: nil, hnsw: nil, spann: nil)
      @space = space
      @embedding_function = embedding_function
      @source_key = source_key.respond_to?(:name) ? source_key.name : source_key
      @hnsw = hnsw
      @spann = spann
    end

    def type = "VectorIndexConfig"
  end

  class SparseVectorIndexConfig
    attr_accessor :embedding_function, :source_key, :bm25

    def initialize(embedding_function: nil, source_key: nil, bm25: nil)
      @embedding_function = embedding_function
      @source_key = source_key.respond_to?(:name) ? source_key.name : source_key
      @bm25 = bm25
    end

    def type = "SparseVectorIndexConfig"
  end

  class FtsIndexType
    attr_accessor :enabled, :config

    def initialize(enabled, config)
      @enabled = enabled
      @config = config
    end
  end

  class StringInvertedIndexType
    attr_accessor :enabled, :config

    def initialize(enabled, config)
      @enabled = enabled
      @config = config
    end
  end

  class VectorIndexType
    attr_accessor :enabled, :config

    def initialize(enabled, config)
      @enabled = enabled
      @config = config
    end
  end

  class SparseVectorIndexType
    attr_accessor :enabled, :config

    def initialize(enabled, config)
      @enabled = enabled
      @config = config
    end
  end

  class IntInvertedIndexType
    attr_accessor :enabled, :config

    def initialize(enabled, config)
      @enabled = enabled
      @config = config
    end
  end

  class FloatInvertedIndexType
    attr_accessor :enabled, :config

    def initialize(enabled, config)
      @enabled = enabled
      @config = config
    end
  end

  class BoolInvertedIndexType
    attr_accessor :enabled, :config

    def initialize(enabled, config)
      @enabled = enabled
      @config = config
    end
  end

  class StringValueType
    attr_accessor :fts_index, :string_inverted_index

    def initialize(fts_index = nil, string_inverted_index = nil)
      @fts_index = fts_index
      @string_inverted_index = string_inverted_index
    end
  end

  class FloatListValueType
    attr_accessor :vector_index

    def initialize(vector_index = nil)
      @vector_index = vector_index
    end
  end

  class SparseVectorValueType
    attr_accessor :sparse_vector_index

    def initialize(sparse_vector_index = nil)
      @sparse_vector_index = sparse_vector_index
    end
  end

  class IntValueType
    attr_accessor :int_inverted_index

    def initialize(int_inverted_index = nil)
      @int_inverted_index = int_inverted_index
    end
  end

  class FloatValueType
    attr_accessor :float_inverted_index

    def initialize(float_inverted_index = nil)
      @float_inverted_index = float_inverted_index
    end
  end

  class BoolValueType
    attr_accessor :bool_inverted_index

    def initialize(bool_inverted_index = nil)
      @bool_inverted_index = bool_inverted_index
    end
  end

  class ValueTypes
    attr_accessor :string, :float_list, :sparse_vector, :int_value, :float_value, :boolean

    def initialize
      @string = nil
      @float_list = nil
      @sparse_vector = nil
      @int_value = nil
      @float_value = nil
      @boolean = nil
    end
  end

  class Schema
    attr_accessor :defaults, :keys

    def initialize
      @defaults = ValueTypes.new
      @keys = {}
      initialize_defaults
      initialize_keys
    end

    def create_index(config: nil, key: nil)
      config_provided = !config.nil?
      key_provided = !key.nil?

      if !config_provided && !key_provided
        raise ArgumentError,
              "Cannot enable all index types globally. Must specify either config or key."
      end

      if key_provided && [ EMBEDDING_KEY, DOCUMENT_KEY ].include?(key)
        raise ArgumentError,
              "Cannot create index on special key '#{key}'. These keys are managed automatically by the system."
      end

      if config.is_a?(VectorIndexConfig)
        if !key_provided
          set_vector_index_config(config)
          return self
        end
        raise ArgumentError,
              "Vector index cannot be enabled on specific keys. Use create_index without key to configure globally."
      end

      if config.is_a?(FtsIndexConfig)
        if !key_provided
          set_fts_index_config(config)
          return self
        end
        raise ArgumentError,
              "FTS index cannot be enabled on specific keys. Use create_index without key to configure globally."
      end

      if config.is_a?(SparseVectorIndexConfig) && !key_provided
        raise ArgumentError,
              "Sparse vector index must be created on a specific key. Please specify a key using create_index(config: SparseVectorIndexConfig.new, key: 'your_key')"
      end

      if !config_provided && key_provided
        raise ArgumentError,
              "Cannot enable all index types for key '#{key}'. Please specify a specific index configuration."
      end

      if config_provided && !key_provided
        set_index_in_defaults(config, true)
      elsif config_provided && key_provided
        set_index_for_key(key, config, true)
      end

      self
    end

    def delete_index(config: nil, key: nil)
      config_provided = !config.nil?
      key_provided = !key.nil?

      if !config_provided && !key_provided
        raise ArgumentError,
              "Cannot disable all indexes. Must specify either config or key."
      end

      if key_provided && [ EMBEDDING_KEY, DOCUMENT_KEY ].include?(key)
        raise ArgumentError,
              "Cannot delete index on special key '#{key}'. These keys are managed automatically by the system."
      end

      if config.is_a?(VectorIndexConfig)
        raise ArgumentError, "Deleting vector index is not currently supported."
      end

      if config.is_a?(FtsIndexConfig)
        raise ArgumentError, "Deleting FTS index is not currently supported."
      end

      if config.is_a?(SparseVectorIndexConfig)
        raise ArgumentError, "Deleting sparse vector index is not currently supported."
      end

      if key_provided && !config_provided
        raise ArgumentError,
              "Cannot disable all index types for key '#{key}'. Please specify a specific index configuration."
      end

      if key_provided && config_provided
        set_index_for_key(key, config, false)
      elsif !key_provided && config_provided
        set_index_in_defaults(config, false)
      end

      self
    end

    def serialize_to_json
      defaults = serialize_value_types(@defaults)
      keys = {}
      @keys.each do |key_name, value_types|
        keys[key_name] = serialize_value_types(value_types)
      end
      { "defaults" => defaults, "keys" => keys }
    end

    def self.deserialize_from_json(json, client: nil)
      return nil if json.nil?
      data = json
      instance = allocate
      instance.defaults = deserialize_value_types(data["defaults"] || {}, client: client)
      instance.keys = {}
      (data["keys"] || {}).each do |key_name, value|
        instance.keys[key_name] = deserialize_value_types(value, client: client)
      end
      instance
    end

    def resolve_embedding_function
      override = @keys[EMBEDDING_KEY]&.float_list&.vector_index&.config&.embedding_function
      return override if override

      @defaults.float_list&.vector_index&.config&.embedding_function
    end

    private

    def set_vector_index_config(config)
      defaults_float_list = ensure_float_list_value_type(@defaults)
      current_vector = defaults_float_list.vector_index || VectorIndexType.new(false, VectorIndexConfig.new)
      defaults_float_list.vector_index = VectorIndexType.new(
        current_vector.enabled,
        VectorIndexConfig.new(
          space: config.space,
          embedding_function: config.embedding_function,
          source_key: config.source_key,
          hnsw: deep_clone(config.hnsw),
          spann: deep_clone(config.spann),
        ),
      )

      embedding_value_types = ensure_value_types(@keys[EMBEDDING_KEY])
      @keys[EMBEDDING_KEY] = embedding_value_types
      override_float_list = ensure_float_list_value_type(embedding_value_types)
      current_override = override_float_list.vector_index || VectorIndexType.new(true, VectorIndexConfig.new(source_key: DOCUMENT_KEY))
      preserved_source_key = current_override.config.source_key || DOCUMENT_KEY
      override_float_list.vector_index = VectorIndexType.new(
        current_override.enabled,
        VectorIndexConfig.new(
          space: config.space,
          embedding_function: config.embedding_function,
          source_key: preserved_source_key,
          hnsw: deep_clone(config.hnsw),
          spann: deep_clone(config.spann),
        ),
      )
    end

    def set_fts_index_config(config)
      defaults_string = ensure_string_value_type(@defaults)
      current_defaults = defaults_string.fts_index || FtsIndexType.new(false, FtsIndexConfig.new)
      defaults_string.fts_index = FtsIndexType.new(current_defaults.enabled, config)

      document_value_types = ensure_value_types(@keys[DOCUMENT_KEY])
      @keys[DOCUMENT_KEY] = document_value_types
      override_string = ensure_string_value_type(document_value_types)
      current_override = override_string.fts_index || FtsIndexType.new(true, FtsIndexConfig.new)
      override_string.fts_index = FtsIndexType.new(current_override.enabled, config)
    end

    def set_index_in_defaults(config, enabled)
      case config
      when FtsIndexConfig
        ensure_string_value_type(@defaults).fts_index = FtsIndexType.new(enabled, config)
      when StringInvertedIndexConfig
        ensure_string_value_type(@defaults).string_inverted_index = StringInvertedIndexType.new(enabled, config)
      when VectorIndexConfig
        ensure_float_list_value_type(@defaults).vector_index = VectorIndexType.new(enabled, config)
      when SparseVectorIndexConfig
        ensure_sparse_vector_value_type(@defaults).sparse_vector_index = SparseVectorIndexType.new(enabled, config)
      when IntInvertedIndexConfig
        ensure_int_value_type(@defaults).int_inverted_index = IntInvertedIndexType.new(enabled, config)
      when FloatInvertedIndexConfig
        ensure_float_value_type(@defaults).float_inverted_index = FloatInvertedIndexType.new(enabled, config)
      when BoolInvertedIndexConfig
        ensure_bool_value_type(@defaults).bool_inverted_index = BoolInvertedIndexType.new(enabled, config)
      end
    end

    def set_index_for_key(key, config, enabled)
      if config.is_a?(SparseVectorIndexConfig) && enabled
        validate_single_sparse_vector_index(key)
        validate_sparse_vector_config(config)
      end

      current = @keys[key] = ensure_value_types(@keys[key])

      case config
      when StringInvertedIndexConfig
        ensure_string_value_type(current).string_inverted_index = StringInvertedIndexType.new(enabled, config)
      when FtsIndexConfig
        ensure_string_value_type(current).fts_index = FtsIndexType.new(enabled, config)
      when SparseVectorIndexConfig
        ensure_sparse_vector_value_type(current).sparse_vector_index = SparseVectorIndexType.new(enabled, config)
      when VectorIndexConfig
        ensure_float_list_value_type(current).vector_index = VectorIndexType.new(enabled, config)
      when IntInvertedIndexConfig
        ensure_int_value_type(current).int_inverted_index = IntInvertedIndexType.new(enabled, config)
      when FloatInvertedIndexConfig
        ensure_float_value_type(current).float_inverted_index = FloatInvertedIndexType.new(enabled, config)
      when BoolInvertedIndexConfig
        ensure_bool_value_type(current).bool_inverted_index = BoolInvertedIndexType.new(enabled, config)
      end
    end

    def validate_single_sparse_vector_index(target_key)
      @keys.each do |existing_key, value_types|
        next if existing_key == target_key
        sparse_index = value_types.sparse_vector&.sparse_vector_index
        if sparse_index&.enabled
          raise ArgumentError,
                "Cannot enable sparse vector index on key '#{target_key}'. A sparse vector index is already enabled on key '#{existing_key}'. Only one sparse vector index is allowed per collection."
        end
      end
    end

    def validate_sparse_vector_config(config)
      if config.source_key && config.embedding_function.nil?
        raise ArgumentError,
              "If source_key is provided then embedding_function must also be provided since there is no default embedding function."
      end
    end

    def initialize_defaults
      @defaults.string = StringValueType.new(
        FtsIndexType.new(false, FtsIndexConfig.new),
        StringInvertedIndexType.new(true, StringInvertedIndexConfig.new),
      )

      @defaults.float_list = FloatListValueType.new(
        VectorIndexType.new(false, VectorIndexConfig.new),
      )

      @defaults.sparse_vector = SparseVectorValueType.new(
        SparseVectorIndexType.new(false, SparseVectorIndexConfig.new),
      )

      @defaults.int_value = IntValueType.new(
        IntInvertedIndexType.new(true, IntInvertedIndexConfig.new),
      )

      @defaults.float_value = FloatValueType.new(
        FloatInvertedIndexType.new(true, FloatInvertedIndexConfig.new),
      )

      @defaults.boolean = BoolValueType.new(
        BoolInvertedIndexType.new(true, BoolInvertedIndexConfig.new),
      )
    end

    def initialize_keys
      @keys[DOCUMENT_KEY] = ValueTypes.new
      @keys[DOCUMENT_KEY].string = StringValueType.new(
        FtsIndexType.new(true, FtsIndexConfig.new),
        StringInvertedIndexType.new(false, StringInvertedIndexConfig.new),
      )

      @keys[EMBEDDING_KEY] = ValueTypes.new
      @keys[EMBEDDING_KEY].float_list = FloatListValueType.new(
        VectorIndexType.new(true, VectorIndexConfig.new(source_key: DOCUMENT_KEY)),
      )
    end

    def serialize_value_types(value_types)
      result = {}
      if value_types.string
        serialized = serialize_string_value_type(value_types.string)
        result[STRING_VALUE_NAME] = serialized unless serialized.empty?
      end
      if value_types.float_list
        serialized = serialize_float_list_value_type(value_types.float_list)
        result[FLOAT_LIST_VALUE_NAME] = serialized unless serialized.empty?
      end
      if value_types.sparse_vector
        serialized = serialize_sparse_vector_value_type(value_types.sparse_vector)
        result[SPARSE_VECTOR_VALUE_NAME] = serialized unless serialized.empty?
      end
      if value_types.int_value
        serialized = serialize_int_value_type(value_types.int_value)
        result[INT_VALUE_NAME] = serialized unless serialized.empty?
      end
      if value_types.float_value
        serialized = serialize_float_value_type(value_types.float_value)
        result[FLOAT_VALUE_NAME] = serialized unless serialized.empty?
      end
      if value_types.boolean
        serialized = serialize_bool_value_type(value_types.boolean)
        result[BOOL_VALUE_NAME] = serialized unless serialized.empty?
      end
      result
    end

    def serialize_string_value_type(value_type)
      result = {}
      if value_type.fts_index
        result[FTS_INDEX_NAME] = {
          "enabled" => value_type.fts_index.enabled,
          "config" => {}
        }
      end
      if value_type.string_inverted_index
        result[STRING_INVERTED_INDEX_NAME] = {
          "enabled" => value_type.string_inverted_index.enabled,
          "config" => {}
        }
      end
      result
    end

    def serialize_float_list_value_type(value_type)
      result = {}
      if value_type.vector_index
        result[VECTOR_INDEX_NAME] = {
          "enabled" => value_type.vector_index.enabled,
          "config" => serialize_config(value_type.vector_index.config)
        }
      end
      result
    end

    def serialize_sparse_vector_value_type(value_type)
      result = {}
      if value_type.sparse_vector_index
        result[SPARSE_VECTOR_INDEX_NAME] = {
          "enabled" => value_type.sparse_vector_index.enabled,
          "config" => serialize_config(value_type.sparse_vector_index.config)
        }
      end
      result
    end

    def serialize_int_value_type(value_type)
      result = {}
      if value_type.int_inverted_index
        result[INT_INVERTED_INDEX_NAME] = {
          "enabled" => value_type.int_inverted_index.enabled,
          "config" => {}
        }
      end
      result
    end

    def serialize_float_value_type(value_type)
      result = {}
      if value_type.float_inverted_index
        result[FLOAT_INVERTED_INDEX_NAME] = {
          "enabled" => value_type.float_inverted_index.enabled,
          "config" => {}
        }
      end
      result
    end

    def serialize_bool_value_type(value_type)
      result = {}
      if value_type.bool_inverted_index
        result[BOOL_INVERTED_INDEX_NAME] = {
          "enabled" => value_type.bool_inverted_index.enabled,
          "config" => {}
        }
      end
      result
    end

    def serialize_config(config)
      case config
      when VectorIndexConfig
        serialize_vector_config(config)
      when SparseVectorIndexConfig
        serialize_sparse_vector_config(config)
      else
        {}
      end
    end

    def serialize_vector_config(config)
      serialized = {}
      embedding_function = config.embedding_function
      serialized["embedding_function"] = EmbeddingFunctions.prepare_embedding_function_config(embedding_function)

      resolved_space = config.space
      if resolved_space.nil? && embedding_function&.respond_to?(:default_space)
        resolved_space = embedding_function.default_space
      end
      serialized["space"] = resolved_space if resolved_space
      serialized["source_key"] = config.source_key if config.source_key
      serialized["hnsw"] = deep_clone(config.hnsw) if config.hnsw
      serialized["spann"] = deep_clone(config.spann) if config.spann
      serialized
    end

    def serialize_sparse_vector_config(config)
      serialized = {}
      serialized["embedding_function"] = EmbeddingFunctions.prepare_embedding_function_config(config.embedding_function)
      serialized["source_key"] = config.source_key if config.source_key
      serialized["bm25"] = config.bm25 if [ true, false ].include?(config.bm25)
      serialized
    end

    def self.deserialize_value_types(json, client: nil)
      result = ValueTypes.new

      result.string = deserialize_string_value_type(json[STRING_VALUE_NAME]) if json[STRING_VALUE_NAME]
      result.float_list = deserialize_float_list_value_type(json[FLOAT_LIST_VALUE_NAME], client: client) if json[FLOAT_LIST_VALUE_NAME]
      result.sparse_vector = deserialize_sparse_vector_value_type(json[SPARSE_VECTOR_VALUE_NAME], client: client) if json[SPARSE_VECTOR_VALUE_NAME]
      result.int_value = deserialize_int_value_type(json[INT_VALUE_NAME]) if json[INT_VALUE_NAME]
      result.float_value = deserialize_float_value_type(json[FLOAT_VALUE_NAME]) if json[FLOAT_VALUE_NAME]
      result.boolean = deserialize_bool_value_type(json[BOOL_VALUE_NAME]) if json[BOOL_VALUE_NAME]

      result
    end

    def self.deserialize_string_value_type(json)
      fts_index = nil
      string_index = nil
      if json[FTS_INDEX_NAME]
        cfg = json[FTS_INDEX_NAME]
        fts_index = FtsIndexType.new(cfg["enabled"], FtsIndexConfig.new)
      end
      if json[STRING_INVERTED_INDEX_NAME]
        cfg = json[STRING_INVERTED_INDEX_NAME]
        string_index = StringInvertedIndexType.new(cfg["enabled"], StringInvertedIndexConfig.new)
      end
      StringValueType.new(fts_index, string_index)
    end

    def self.deserialize_float_list_value_type(json, client: nil)
      vector_index = nil
      if json[VECTOR_INDEX_NAME]
        cfg = json[VECTOR_INDEX_NAME]
        config = deserialize_vector_config(cfg["config"], client: client)
        vector_index = VectorIndexType.new(cfg["enabled"], config)
      end
      FloatListValueType.new(vector_index)
    end

    def self.deserialize_sparse_vector_value_type(json, client: nil)
      sparse_index = nil
      if json[SPARSE_VECTOR_INDEX_NAME]
        cfg = json[SPARSE_VECTOR_INDEX_NAME]
        config = deserialize_sparse_vector_config(cfg["config"], client: client)
        sparse_index = SparseVectorIndexType.new(cfg["enabled"], config)
      end
      SparseVectorValueType.new(sparse_index)
    end

    def self.deserialize_int_value_type(json)
      int_index = nil
      if json[INT_INVERTED_INDEX_NAME]
        cfg = json[INT_INVERTED_INDEX_NAME]
        int_index = IntInvertedIndexType.new(cfg["enabled"], IntInvertedIndexConfig.new)
      end
      IntValueType.new(int_index)
    end

    def self.deserialize_float_value_type(json)
      float_index = nil
      if json[FLOAT_INVERTED_INDEX_NAME]
        cfg = json[FLOAT_INVERTED_INDEX_NAME]
        float_index = FloatInvertedIndexType.new(cfg["enabled"], FloatInvertedIndexConfig.new)
      end
      FloatValueType.new(float_index)
    end

    def self.deserialize_bool_value_type(json)
      bool_index = nil
      if json[BOOL_INVERTED_INDEX_NAME]
        cfg = json[BOOL_INVERTED_INDEX_NAME]
        bool_index = BoolInvertedIndexType.new(cfg["enabled"], BoolInvertedIndexConfig.new)
      end
      BoolValueType.new(bool_index)
    end

    def self.deserialize_vector_config(json, client: nil)
      embedding_function = EmbeddingFunctions.build_embedding_function(json["embedding_function"], client: client)
      space = json["space"]
      config = VectorIndexConfig.new(
        space: space,
        embedding_function: embedding_function,
        source_key: json["source_key"],
        hnsw: json["hnsw"],
        spann: json["spann"],
      )
      if config.space.nil? && embedding_function&.respond_to?(:default_space)
        config.space = embedding_function.default_space
      end
      config
    end

    def self.deserialize_sparse_vector_config(json, client: nil)
      embedding_function = EmbeddingFunctions.build_sparse_embedding_function(json["embedding_function"], client: client)
      SparseVectorIndexConfig.new(
        embedding_function: embedding_function,
        source_key: json["source_key"],
        bm25: json["bm25"],
      )
    end

    def deep_clone(value)
      return nil if value.nil?
      Marshal.load(Marshal.dump(value))
    end

    def ensure_value_types(value_types)
      value_types || ValueTypes.new
    end

    def ensure_string_value_type(value_types)
      value_types.string ||= StringValueType.new
      value_types.string
    end

    def ensure_float_list_value_type(value_types)
      value_types.float_list ||= FloatListValueType.new
      value_types.float_list
    end

    def ensure_sparse_vector_value_type(value_types)
      value_types.sparse_vector ||= SparseVectorValueType.new
      value_types.sparse_vector
    end

    def ensure_int_value_type(value_types)
      value_types.int_value ||= IntValueType.new
      value_types.int_value
    end

    def ensure_float_value_type(value_types)
      value_types.float_value ||= FloatValueType.new
      value_types.float_value
    end

    def ensure_bool_value_type(value_types)
      value_types.boolean ||= BoolValueType.new
      value_types.boolean
    end
  end
end
