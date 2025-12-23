# frozen_string_literal: true

module Chroma
  class Collection
    attr_reader :id, :name, :metadata, :tenant, :database, :configuration, :schema

    def initialize(client:, model:, embedding_function: nil, data_loader: nil, schema: nil)
      @client = client
      @embedding_function = embedding_function
      @data_loader = data_loader

      @id = model["id"]
      @name = model["name"]
      @metadata = model["metadata"]
      @tenant = model["tenant"]
      @database = model["database"]
      @configuration = model["configuration_json"] || model["configuration"]
      @schema = schema
    end

    def embedding_function
      @embedding_function || @schema&.resolve_embedding_function
    end

    def count
      path = collection_path
      @client.transport.request(
        :get,
        "/tenants/#{path[:tenant]}/databases/#{path[:database]}/collections/#{path[:collection_id]}/count",
      )
    end

    def add(ids:, embeddings: nil, metadatas: nil, documents: nil, uris: nil)
      record_set = {
        ids: ids,
        embeddings: embeddings,
        metadatas: metadatas,
        documents: documents,
        images: nil,
        uris: uris
      }

      prepared = prepare_records(record_set)
      path = collection_path

      @client.transport.request(
        :post,
        "/tenants/#{path[:tenant]}/databases/#{path[:database]}/collections/#{path[:collection_id]}/add",
        json: {
          "ids" => prepared[:ids],
          "embeddings" => prepared[:embeddings],
          "metadatas" => Types::Validation.serialize_metadatas(prepared[:metadatas]),
          "documents" => prepared[:documents],
          "uris" => prepared[:uris]
        },
      )
      nil
    end

    def get(ids: nil, where: nil, where_document: nil, include: [ "metadatas", "documents" ], limit: nil, offset: nil)
      include = include.dup
      Types::Validation.validate_include(include, disallowed: [ "distances" ])
      Types::Validation.validate_ids(ids) if ids
      Types::Validation.validate_where(where) if where
      Types::Validation.validate_where_document(where_document) if where_document

      if include.include?("data") && @data_loader.nil?
        raise ArgumentError, "You must set a data loader on the collection if loading from URIs."
      end

      request_include = include
      request_include << "uris" if include.include?("data") && !include.include?("uris")

      path = collection_path
      response = @client.transport.request(
        :post,
        "/tenants/#{path[:tenant]}/databases/#{path[:database]}/collections/#{path[:collection_id]}/get",
        json: {
          "ids" => ids,
          "where" => where,
          "where_document" => where_document,
          "include" => request_include,
          "limit" => limit,
          "offset" => offset
        },
      )

      metadatas = Types::Validation.deserialize_metadatas(response["metadatas"])

      data = nil
      if include.include?("data") && @data_loader && response["uris"]
        data = @data_loader.call(response["uris"])
      end

      uris = include.include?("uris") ? response["uris"] : nil

      Types::GetResult.new(
        ids: response["ids"],
        embeddings: response["embeddings"],
        metadatas: metadatas,
        documents: response["documents"],
        uris: uris,
        data: data,
        included: include,
      )
    end

    def peek(limit: 10)
      get(limit: limit)
    end

    def query(query_embeddings: nil, query_texts: nil, query_uris: nil, ids: nil, n_results: 10,
              where: nil, where_document: nil, include: [ "metadatas", "documents", "distances" ])
      record_set = {
        embeddings: query_embeddings,
        documents: query_texts,
        images: nil,
        uris: query_uris
      }

      prepared = prepare_query(record_set, include, ids, where, where_document, n_results)

      path = collection_path
      response = @client.transport.request(
        :post,
        "/tenants/#{path[:tenant]}/databases/#{path[:database]}/collections/#{path[:collection_id]}/query",
        json: {
          "query_embeddings" => prepared[:embeddings],
          "n_results" => n_results,
          "where" => where,
          "where_document" => where_document,
          "include" => prepared[:include],
          "ids" => ids
        },
      )

      metadatas = deserialize_metadata_matrix(response["metadatas"])

      data = nil
      if include.include?("data") && @data_loader && response["uris"]
        data = @data_loader.call(response["uris"])
      end

      uris = include.include?("uris") ? response["uris"] : nil

      Types::QueryResult.new(
        ids: response["ids"],
        embeddings: response["embeddings"],
        metadatas: metadatas,
        documents: response["documents"],
        uris: uris,
        data: data,
        distances: response["distances"],
        included: include,
      )
    end

    def modify(name: nil, metadata: nil, configuration: nil)
      payload = {}
      payload["name"] = name if name
      payload["metadata"] = Types::Validation.serialize_metadata(metadata) if metadata
      config_payload = configuration_to_payload(configuration)
      payload["configuration"] = config_payload unless config_payload.empty?

      path = collection_path
      @client.transport.request(
        :put,
        "/tenants/#{path[:tenant]}/databases/#{path[:database]}/collections/#{path[:collection_id]}",
        json: payload,
      )
      nil
    end

    def fork(name:)
      path = collection_path
      response = @client.transport.request(
        :post,
        "/tenants/#{path[:tenant]}/databases/#{path[:database]}/collections/#{path[:collection_id]}/fork",
        json: { "name" => name },
      )
      @client.send(:build_collection, response)
    end

    def update(ids:, embeddings: nil, metadatas: nil, documents: nil, uris: nil)
      record_set = {
        ids: ids,
        embeddings: embeddings,
        metadatas: metadatas,
        documents: documents,
        images: nil,
        uris: uris
      }

      prepared = prepare_records(record_set, update: true)
      path = collection_path

      @client.transport.request(
        :post,
        "/tenants/#{path[:tenant]}/databases/#{path[:database]}/collections/#{path[:collection_id]}/update",
        json: {
          "ids" => prepared[:ids],
          "embeddings" => prepared[:embeddings],
          "metadatas" => Types::Validation.serialize_metadatas(prepared[:metadatas]),
          "documents" => prepared[:documents],
          "uris" => prepared[:uris]
        },
      )
      nil
    end

    def upsert(ids:, embeddings: nil, metadatas: nil, documents: nil, uris: nil)
      record_set = {
        ids: ids,
        embeddings: embeddings,
        metadatas: metadatas,
        documents: documents,
        images: nil,
        uris: uris
      }

      prepared = prepare_records(record_set)
      path = collection_path

      @client.transport.request(
        :post,
        "/tenants/#{path[:tenant]}/databases/#{path[:database]}/collections/#{path[:collection_id]}/upsert",
        json: {
          "ids" => prepared[:ids],
          "embeddings" => prepared[:embeddings],
          "metadatas" => Types::Validation.serialize_metadatas(prepared[:metadatas]),
          "documents" => prepared[:documents],
          "uris" => prepared[:uris]
        },
      )
      nil
    end

    def delete(ids: nil, where: nil, where_document: nil)
      if ids.nil? && where.nil? && where_document.nil?
        raise ArgumentError, "At least one of ids, where, or where_document must be provided"
      end

      Types::Validation.validate_ids(ids) if ids
      Types::Validation.validate_where(where) if where
      Types::Validation.validate_where_document(where_document) if where_document

      path = collection_path
      @client.transport.request(
        :post,
        "/tenants/#{path[:tenant]}/databases/#{path[:database]}/collections/#{path[:collection_id]}/delete",
        json: { "ids" => ids, "where" => where, "where_document" => where_document },
      )
      nil
    end

    def attach_function(function_id:, name:, output_collection:, params: nil)
      path = collection_path
      response = @client.transport.request(
        :post,
        "/tenants/#{path[:tenant]}/databases/#{path[:database]}/collections/#{path[:collection_id]}/functions/attach",
        json: {
          "function_id" => function_id,
          "name" => name,
          "output_collection" => output_collection,
          "params" => params
        },
      )
      response
    end

    def get_attached_function(name:)
      path = collection_path
      @client.transport.request(
        :get,
        "/tenants/#{path[:tenant]}/databases/#{path[:database]}/collections/#{path[:collection_id]}/functions/#{name}",
      )
    end

    def detach_function(name:)
      path = collection_path
      @client.transport.request(
        :post,
        "/tenants/#{path[:tenant]}/databases/#{path[:database]}/collections/#{path[:collection_id]}/attached_functions/#{name}/detach",
        json: {},
      )
      nil
    end

    def search(searches)
      payloads = (searches.is_a?(Array) ? searches : [ searches ]).map { |item| normalize_search_input(item) }
      embedded_payloads = payloads.map { |payload| embed_search_payload(payload) }

      body = { "searches" => embedded_payloads }
      path = collection_path
      response = @client.transport.request(
        :post,
        "/tenants/#{path[:tenant]}/databases/#{path[:database]}/collections/#{path[:collection_id]}/search",
        json: body,
      )

      Types::SearchResult.new(response)
    end

    private

    def collection_path
      path = @client.tenant_database_path
      { tenant: path[:tenant], database: path[:database], collection_id: @id }
    end

    def embed(texts, is_query)
      ef = embedding_function
      raise ArgumentError, "Embedding function must be defined for operations requiring embeddings." unless ef

      if is_query && ef.respond_to?(:embed_query)
        ef.embed_query(texts)
      else
        ef.call(texts)
      end
    end

    def sparse_embed(sparse_embedding_function, texts, is_query)
      if is_query && sparse_embedding_function.respond_to?(:embed_query)
        sparse_embedding_function.embed_query(texts)
      else
        sparse_embedding_function.call(texts)
      end
    end

    def get_sparse_embedding_targets
      return {} unless @schema

      targets = {}
      @schema.keys.each do |key, value_types|
        sparse_vector = value_types.sparse_vector
        sparse_index = sparse_vector&.sparse_vector_index
        next unless sparse_index&.enabled

        config = sparse_index.config
        next unless config.embedding_function && config.source_key

        targets[key] = config
      end
      targets
    end

    def apply_sparse_embeddings_to_metadatas(metadatas, documents)
      sparse_targets = get_sparse_embedding_targets
      return metadatas if sparse_targets.empty?

      if metadatas.nil?
        return nil unless documents
        metadatas = Array.new(documents.length) { {} }
      end

      updated = metadatas.map { |metadata| metadata.nil? ? {} : metadata.dup }
      documents_list = documents ? documents.dup : nil

      sparse_targets.each do |target_key, config|
        source_key = config.source_key
        embedding_function = config.embedding_function
        next unless source_key && embedding_function

        inputs = []
        positions = []

        if source_key == DOCUMENT_KEY
          next unless documents_list
          updated.each_with_index do |metadata, index|
            next if metadata.key?(target_key)
            doc = documents_list[index]
            if doc.is_a?(String)
              inputs << doc
              positions << index
            end
          end
        else
          updated.each_with_index do |metadata, index|
            next if metadata.key?(target_key)
            source_value = metadata[source_key]
            next unless source_value.is_a?(String)
            inputs << source_value
            positions << index
          end
        end

        next if inputs.empty?

        sparse_embeddings = sparse_embed(embedding_function, inputs, false)
        if sparse_embeddings.length != positions.length
          raise ArgumentError, "Sparse embedding function returned unexpected number of embeddings."
        end

        positions.each_with_index do |position, idx|
          updated[position][target_key] = sparse_embeddings[idx]
        end
      end

      updated.map { |metadata| metadata.empty? ? nil : metadata }
    end

    def prepare_records(record_set, update: false)
      normalized = Types::Validation.normalize_insert_record_set(
        ids: record_set[:ids],
        embeddings: record_set[:embeddings],
        metadatas: record_set[:metadatas],
        documents: record_set[:documents],
        images: record_set[:images],
        uris: record_set[:uris],
      )

      Types::Validation.validate_insert_record_set(normalized)
      Types::Validation.validate_record_set_contains_any(normalized, %i[ids])

      max_batch_size = @client.get_max_batch_size
      if max_batch_size && max_batch_size > 0
        Types::Validation.validate_batch([ normalized[:ids] ], { max_batch_size: max_batch_size })
      end

      if normalized[:embeddings].nil?
        if update
          if normalized[:documents] || normalized[:images]
            Types::Validation.validate_record_set_for_embedding(normalized, embeddable_fields: %i[documents images])
            normalized[:embeddings] = embed(normalized[:documents] || [], false)
          end
        else
          Types::Validation.validate_record_set_for_embedding(normalized)
          normalized[:embeddings] = embed(normalized[:documents] || [], false)
        end
      end

      normalized[:metadatas] = apply_sparse_embeddings_to_metadatas(normalized[:metadatas], normalized[:documents])

      if @client.supports_base64_encoding? && normalized[:embeddings]
        normalized[:embeddings] = Types::Encoding.embeddings_to_base64_strings(normalized[:embeddings])
      end

      normalized
    end

    def prepare_query(record_set, include, ids, where, where_document, n_results)
      normalized = Types::Validation.normalize_base_record_set(
        embeddings: record_set[:embeddings],
        documents: record_set[:documents],
        images: record_set[:images],
        uris: record_set[:uris],
      )

      Types::Validation.validate_base_record_set(normalized)
      Types::Validation.validate_include(include)
      Types::Validation.validate_ids(ids) if ids
      Types::Validation.validate_where(where) if where
      Types::Validation.validate_where_document(where_document) if where_document
      Types::Validation.validate_n_results(n_results) if n_results

      if normalized[:embeddings].nil?
        Types::Validation.validate_record_set_for_embedding(normalized)
        normalized[:embeddings] = embed(normalized[:documents] || [], true)
      end

      request_include = include.dup
      request_include << "uris" if include.include?("data") && !include.include?("uris")

      normalized[:include] = request_include
      normalized
    end

    def deserialize_metadata_matrix(matrix)
      return nil if matrix.nil?
      matrix.map do |row|
        row&.map { |metadata| Types::Validation.deserialize_metadata(metadata) }
      end
    end

    def configuration_to_payload(configuration)
      return {} if configuration.nil?
      return configuration.to_h if configuration.respond_to?(:to_h)
      configuration
    end

    def normalize_search_input(item)
      return item.to_h if item.is_a?(Chroma::Search::Search)
      if item.is_a?(Hash)
        has_limit = item.key?("limit") || item.key?(:limit)
        has_select = item.key?("select") || item.key?(:select)
        if has_limit && has_select
          return stringify_keys(item)
        end
        return Chroma::Search::Search.new(
          where: item[:where] || item["where"],
          rank: item[:rank] || item["rank"],
          limit: item[:limit] || item["limit"],
          select: item[:select] || item["select"],
        ).to_h
      end

      raise ArgumentError, "Unsupported search input"
    end

    def embed_search_payload(payload)
      return payload unless payload["rank"]
      embedded_rank = embed_rank_literal(payload["rank"])
      return payload unless embedded_rank.is_a?(Hash)
      payload.merge("rank" => embedded_rank)
    end

    def embed_rank_literal(rank)
      return rank if rank.nil?
      if rank.is_a?(Array)
        return rank.map { |item| embed_rank_literal(item) }
      end
      return rank unless rank.is_a?(Hash)

      rank.each_with_object({}) do |(key, value), acc|
        if key == "$knn" && value.is_a?(Hash)
          acc[key] = embed_knn_literal(value)
        else
          acc[key] = embed_rank_literal(value)
        end
      end
    end

    def embed_knn_literal(knn)
      query_value = knn["query"] || knn[:query]
      return knn if !query_value.is_a?(String)

      key_value = knn["key"] || knn[:key]
      key = key_value || EMBEDDING_KEY

      if key == EMBEDDING_KEY
        embeddings = embed([ query_value ], true)
        raise ArgumentError, "Embedding function returned unexpected number of embeddings." unless embeddings.length == 1
        return knn.merge("query" => embeddings[0])
      end

      raise ArgumentError,
            "Cannot embed string query for key '#{key}': schema is not available. Provide an embedded vector or configure an embedding function." unless @schema

      value_types = @schema.keys[key]
      raise ArgumentError,
            "Cannot embed string query for key '#{key}': key not found in schema. Provide an embedded vector or configure an embedding function." unless value_types

      sparse_index = value_types.sparse_vector&.sparse_vector_index
      if sparse_index&.enabled && sparse_index.config.embedding_function
        sparse_embeddings = sparse_embed(sparse_index.config.embedding_function, [ query_value ], true)
        raise ArgumentError, "Sparse embedding function returned unexpected number of embeddings." unless sparse_embeddings.length == 1
        embedded = sparse_embeddings[0]
        return knn.merge("query" => { "indices" => embedded.indices, "values" => embedded.values })
      end

      vector_index = value_types.float_list&.vector_index
      if vector_index&.enabled && vector_index.config.embedding_function
        ef = vector_index.config.embedding_function
        embeddings = if ef.respond_to?(:embed_query)
          ef.embed_query([ query_value ])
        else
          ef.call([ query_value ])
        end
        raise ArgumentError, "Embedding function returned unexpected number of embeddings." unless embeddings.length == 1
        return knn.merge("query" => embeddings[0])
      end

      raise ArgumentError,
            "Cannot embed string query for key '#{key}': no embedding function configured. Provide an embedded vector or configure an embedding function."
    end

    def stringify_keys(hash)
      hash.each_with_object({}) do |(key, value), acc|
        acc[key.to_s] = value
      end
    end
  end
end
