# frozen_string_literal: true

require "set"

module Chroma
  module Types
    module Validation
      INCLUDE_OPTIONS = %w[documents embeddings metadatas distances uris data].freeze

      module_function

      def maybe_cast_one_to_many(target)
        return nil if target.nil?
        return target if target.is_a?(Array)
        [ target ]
      end

      def normalize_embeddings(target)
        return nil if target.nil?
        return [ target ] if target.is_a?(Array) && !target.empty? && target.all? { |v| v.is_a?(Numeric) }
        return target if target.is_a?(Array)

        raise ArgumentError, "Expected embeddings to be an Array, got #{target.class}"
      end

      def normalize_metadata(metadata)
        return nil if metadata.nil?
        unless metadata.is_a?(Hash)
          raise ArgumentError, "Expected metadata to be a Hash, got #{metadata.class}"
        end
        normalized = {}
        metadata.each do |key, value|
          if value.is_a?(Hash) && value[TYPE_KEY] == SPARSE_VECTOR_TYPE_VALUE
            normalized[key] = SparseVector.from_h(value)
          else
            normalized[key] = value
          end
        end
        normalized
      end

      def normalize_metadatas(metadatas)
        return nil if metadatas.nil?
        return [ normalize_metadata(metadatas) ] if metadatas.is_a?(Hash)
        unless metadatas.is_a?(Array)
          raise ArgumentError, "Expected metadatas to be an Array, got #{metadatas.class}"
        end
        metadatas.map { |metadata| normalize_metadata(metadata) }
      end

      def serialize_metadata(metadata)
        return nil if metadata.nil?
        unless metadata.is_a?(Hash)
          raise ArgumentError, "Expected metadata to be a Hash, got #{metadata.class}"
        end
        serialized = {}
        metadata.each do |key, value|
          if value.is_a?(SparseVector)
            serialized[key] = value.to_h
          else
            serialized[key] = value
          end
        end
        serialized
      end

      def serialize_metadatas(metadatas)
        return nil if metadatas.nil?
        unless metadatas.is_a?(Array)
          raise ArgumentError, "Expected metadatas to be an Array, got #{metadatas.class}"
        end
        metadatas.map { |metadata| metadata.nil? ? nil : serialize_metadata(metadata) }
      end

      def deserialize_metadata(metadata)
        return nil if metadata.nil?
        normalize_metadata(metadata)
      end

      def deserialize_metadatas(metadatas)
        return nil if metadatas.nil?
        normalize_metadatas(metadatas)
      end

      def validate_ids(ids)
        unless ids.is_a?(Array)
          raise ArgumentError, "Expected IDs to be a list, got #{ids.class} as IDs"
        end
        raise ArgumentError, "Expected IDs to be a non-empty list, got #{ids.length} IDs" if ids.empty?

        seen = {}
        dups = Set.new
        ids.each do |id|
          raise ArgumentError, "Expected ID to be a String, got #{id.inspect}" unless id.is_a?(String)
          if seen[id]
            dups.add(id)
          else
            seen[id] = true
          end
        end

        return ids if dups.empty?

        n_dups = dups.length
        if n_dups < 10
          example_string = dups.to_a.join(", ")
          message = "Expected IDs to be unique, found duplicates of: #{example_string}"
        else
          examples = dups.to_a
          example_string = "#{examples.first(5).join(', ')}, ..., #{examples.last(5).join(', ')}"
          message = "Expected IDs to be unique, found #{n_dups} duplicated IDs: #{example_string}"
        end
        raise Chroma::DuplicateIDError, message
      end

      def validate_metadata(metadata)
        return metadata if metadata.nil?
        unless metadata.is_a?(Hash)
          raise ArgumentError, "Expected metadata to be a Hash or nil, got #{metadata.class} as metadata"
        end
        raise ArgumentError, "Expected metadata to be a non-empty Hash, got #{metadata.length} metadata attributes" if metadata.empty?

        metadata.each do |key, value|
          if key == META_KEY_CHROMA_DOCUMENT
            raise ArgumentError, "Expected metadata to not contain the reserved key #{META_KEY_CHROMA_DOCUMENT}"
          end
          raise TypeError, "Expected metadata key to be a String, got #{key.inspect}" unless key.is_a?(String)
          if value.is_a?(SparseVector)
            next
          end
          unless value.nil? || value.is_a?(String) || value.is_a?(Numeric) || value.is_a?(TrueClass) || value.is_a?(FalseClass)
            raise ArgumentError,
                  "Expected metadata value to be a String, Numeric, Boolean, SparseVector, or nil, got #{value.inspect} which is a #{value.class}"
          end
        end
        metadata
      end

      def validate_update_metadata(metadata)
        return metadata if metadata.nil?
        unless metadata.is_a?(Hash)
          raise ArgumentError, "Expected metadata to be a Hash or nil, got #{metadata.class}"
        end
        raise ArgumentError, "Expected metadata to be a non-empty Hash, got #{metadata.inspect}" if metadata.empty?

        metadata.each do |key, value|
          raise ArgumentError, "Expected metadata key to be a String, got #{key.inspect}" unless key.is_a?(String)
          if value.is_a?(SparseVector)
            next
          end
          unless value.nil? || value.is_a?(String) || value.is_a?(Numeric) || value.is_a?(TrueClass) || value.is_a?(FalseClass)
            raise ArgumentError,
                  "Expected metadata value to be a String, Numeric, Boolean, SparseVector, or nil, got #{value.inspect}"
          end
        end
        metadata
      end

      def validate_metadatas(metadatas)
        unless metadatas.is_a?(Array)
          raise ArgumentError, "Expected metadatas to be a list, got #{metadatas.inspect}"
        end
        metadatas.each { |metadata| validate_metadata(metadata) }
        metadatas
      end

      def validate_where(where)
        unless where.is_a?(Hash)
          raise ArgumentError, "Expected where to be a Hash, got #{where.inspect}"
        end
        raise ArgumentError, "Expected where to have exactly one operator, got #{where.inspect}" if where.length != 1

        where.each do |key, value|
          raise ArgumentError, "Expected where key to be a String, got #{key.inspect}" unless key.is_a?(String)

          if [ "$and", "$or", "$in", "$nin" ].include?(key)
            # handled below
          elsif !(value.is_a?(String) || value.is_a?(Numeric) || value.is_a?(Hash))
            raise ArgumentError,
                  "Expected where value to be a String, Numeric, or operator expression, got #{value.inspect}"
          end

          if key == "$and" || key == "$or"
            unless value.is_a?(Array)
              raise ArgumentError,
                    "Expected where value for #{key} to be a list of where expressions, got #{value.inspect}"
            end
            if value.length <= 1
              raise ArgumentError,
                    "Expected where value for #{key} to be a list with at least two expressions, got #{value.inspect}"
            end
            value.each { |expr| validate_where(expr) }
          elsif value.is_a?(Hash)
            if value.length != 1
              raise ArgumentError,
                    "Expected where operator expression to have exactly one operator, got #{value.inspect}"
            end
            operator, operand = value.first
            unless [ "$eq", "$ne", "$gt", "$gte", "$lt", "$lte", "$in", "$nin", "$contains", "$not_contains", "$regex", "$not_regex" ].include?(operator)
              raise ArgumentError,
                    "Expected where operator to be one of $eq, $ne, $gt, $gte, $lt, $lte, $in, $nin, $contains, $not_contains, $regex, $not_regex, got #{operator}"
            end
            if [ "$in", "$nin" ].include?(operator)
              unless operand.is_a?(Array)
                raise ArgumentError,
                      "Expected where operand for #{operator} to be a list, got #{operand.inspect}"
              end
              if operand.empty?
                raise ArgumentError,
                      "Expected where operand for #{operator} to be a non-empty list"
              end
              operand.each do |item|
                unless item.is_a?(String) || item.is_a?(Numeric) || item.is_a?(TrueClass) || item.is_a?(FalseClass)
                  raise ArgumentError,
                        "Expected where list items to be String, Numeric, or Boolean, got #{item.inspect}"
                end
              end
            elsif [ "$contains", "$not_contains", "$regex", "$not_regex" ].include?(operator)
              unless operand.is_a?(String)
                raise ArgumentError,
                      "Expected where operand for #{operator} to be a String, got #{operand.inspect}"
              end
              raise ArgumentError, "Expected where operand for #{operator} to be a non-empty String" if operand.empty?
            else
              unless operand.is_a?(String) || operand.is_a?(Numeric) || operand.is_a?(TrueClass) || operand.is_a?(FalseClass)
                raise ArgumentError,
                      "Expected where operand for #{operator} to be String, Numeric, or Boolean, got #{operand.inspect}"
              end
            end
          end
        end
      end

      def validate_where_document(where_document)
        unless where_document.is_a?(Hash)
          raise ArgumentError, "Expected where document to be a Hash, got #{where_document.inspect}"
        end
        raise ArgumentError, "Expected where document to have exactly one operator, got #{where_document.inspect}" if where_document.length != 1

        where_document.each do |operator, operand|
          raise ArgumentError, "Expected where document key to be a String, got #{operator.inspect}" unless operator.is_a?(String)

          unless [ "$contains", "$not_contains", "$regex", "$not_regex", "$and", "$or" ].include?(operator)
            raise ArgumentError,
                  "Expected where document operator to be one of $contains, $not_contains, $regex, $not_regex, $and, $or, got #{operator}"
          end

          if operator == "$and" || operator == "$or"
            unless operand.is_a?(Array)
              raise ArgumentError,
                    "Expected document value for #{operator} to be a list of where document expressions, got #{operand.inspect}"
            end
            if operand.length <= 1
              raise ArgumentError,
                    "Expected document value for #{operator} to be a list with at least two where document expressions, got #{operand.inspect}"
            end
            operand.each { |expr| validate_where_document(expr) }
          else
            unless operand.is_a?(String)
              raise ArgumentError,
                    "Expected where document operand value for operator #{operator} to be a String, got #{operand.inspect}"
            end
            raise ArgumentError,
                  "Expected where document operand value for operator #{operator} to be a non-empty String" if operand.empty?
          end
        end
      end

      def validate_include(include, disallowed: nil)
        unless include.is_a?(Array)
          raise ArgumentError, "Expected include to be a list, got #{include.inspect}"
        end
        include.each do |item|
          unless item.is_a?(String)
            raise ArgumentError, "Expected include item to be a String, got #{item.inspect}"
          end
          unless INCLUDE_OPTIONS.include?(item)
            raise ArgumentError, "Expected include item to be one of #{INCLUDE_OPTIONS.join(', ')}, got #{item}"
          end
          if disallowed && disallowed.include?(item)
            raise ArgumentError, "Include item cannot be one of #{disallowed.join(', ')}, got #{item}"
          end
        end
      end

      def validate_n_results(n_results)
        unless n_results.is_a?(Integer)
          raise ArgumentError, "Expected requested number of results to be an Integer, got #{n_results.inspect}"
        end
        if n_results <= 0
          raise ArgumentError, "Number of requested results #{n_results} cannot be negative or zero."
        end
        n_results
      end

      def validate_embeddings(embeddings)
        unless embeddings.is_a?(Array)
          raise ArgumentError, "Expected embeddings to be a list, got #{embeddings.class}"
        end
        raise ArgumentError,
              "Expected embeddings to be a list with at least one item, got #{embeddings.length} embeddings" if embeddings.empty?

        embeddings.each_with_index do |embedding, idx|
          unless embedding.is_a?(Array)
            raise ArgumentError,
                  "Expected embedding at position #{idx} to be an Array, got #{embedding.class}"
          end
          raise ArgumentError, "Expected embedding at position #{idx} to be non-empty" if embedding.empty?
          embedding.each do |value|
            unless value.is_a?(Numeric)
              raise ArgumentError,
                    "Expected each value in the embedding to be Numeric, got #{value.inspect}"
            end
          end
        end
        embeddings
      end

      def validate_sparse_vectors(vectors)
        unless vectors.is_a?(Array)
          raise ArgumentError,
                "Expected sparse vectors to be a list, got #{vectors.class}"
        end
        raise ArgumentError,
              "Expected sparse vectors to be a non-empty list, got #{vectors.length} sparse vectors" if vectors.empty?
        vectors.each_with_index do |vector, i|
          unless vector.is_a?(SparseVector)
            raise ArgumentError,
                  "Expected SparseVector instance at position #{i}, got #{vector.class}"
          end
        end
        vectors
      end

      def validate_documents(documents, nullable: false)
        unless documents.is_a?(Array)
          raise ArgumentError, "Expected documents to be a list, got #{documents.class}"
        end
        raise ArgumentError,
              "Expected documents to be a non-empty list, got #{documents.length} documents" if documents.empty?
        documents.each do |doc|
          next if nullable && doc.nil?
          unless doc.is_a?(String)
            raise ArgumentError, "Expected document to be a String, got #{doc.inspect}"
          end
        end
      end

      def validate_images(images)
        unless images.is_a?(Array)
          raise ArgumentError, "Expected images to be a list, got #{images.class}"
        end
        raise ArgumentError,
              "Expected images to be a non-empty list, got #{images.length} images" if images.empty?
        images.each do |img|
          unless img.is_a?(Array)
            raise ArgumentError, "Expected image to be an Array, got #{img.inspect}"
          end
        end
      end

      def validate_base_record_set(record_set)
        validate_record_set_length_consistency(record_set)

        validate_embeddings(record_set[:embeddings]) if record_set[:embeddings]
        validate_documents(record_set[:documents], nullable: !record_set[:embeddings].nil?) if record_set[:documents]
        validate_images(record_set[:images]) if record_set[:images]
      end

      def validate_insert_record_set(record_set)
        validate_record_set_length_consistency(record_set)
        validate_base_record_set(record_set)

        validate_ids(record_set[:ids])
        validate_metadatas(record_set[:metadatas]) if record_set[:metadatas]
      end

      def validate_record_set_length_consistency(record_set)
        lengths = record_set.values.compact.map(&:length)
        if lengths.empty?
          raise ArgumentError,
                "At least one of #{record_set.keys.join(', ')} must be provided"
        end

        zero_lengths = record_set.select { |_k, v| !v.nil? && v.length == 0 }.keys
        raise ArgumentError, "Non-empty lists are required for #{zero_lengths}" unless zero_lengths.empty?

        if lengths.uniq.length > 1
          error_str = record_set.filter { |_k, v| !v.nil? }.map { |k, v| "#{k}: #{v.length}" }.join(", ")
          raise ArgumentError, "Unequal lengths for fields: #{error_str}"
        end
      end

      def validate_record_set_for_embedding(record_set, embeddable_fields: nil)
        raise ArgumentError, "Attempting to embed a record that already has embeddings." if record_set[:embeddings]
        embeddable_fields ||= default_embeddable_record_set_fields
        validate_record_set_contains_one(record_set, embeddable_fields)
      end

      def validate_record_set_contains_any(record_set, contains_any)
        validate_record_set_contains(record_set, contains_any)
        unless contains_any.any? { |field| !record_set[field].nil? }
          raise ArgumentError, "At least one of #{contains_any.join(', ')} must be provided"
        end
      end

      def validate_record_set_contains_one(record_set, contains_one)
        validate_record_set_contains(record_set, contains_one)
        count = contains_one.count { |field| !record_set[field].nil? }
        unless count == 1
          raise ArgumentError, "Exactly one of #{contains_one.join(', ')} must be provided"
        end
      end

      def validate_record_set_contains(record_set, contains)
        contains.each do |field|
          next if record_set.key?(field)
          raise ArgumentError,
                "Invalid field in contains: #{contains.join(', ')}, available fields: #{record_set.keys.join(', ')}"
        end
      end

      def default_embeddable_record_set_fields
        %i[documents images uris].freeze
      end

      def normalize_base_record_set(embeddings: nil, documents: nil, images: nil, uris: nil)
        {
          embeddings: normalize_embeddings(embeddings),
          documents: maybe_cast_one_to_many(documents),
          images: maybe_cast_one_to_many(images),
          uris: maybe_cast_one_to_many(uris)
        }
      end

      def normalize_insert_record_set(ids:, embeddings:, metadatas: nil, documents: nil, images: nil, uris: nil)
        base_record_set = normalize_base_record_set(
          embeddings: embeddings,
          documents: documents,
          images: images,
          uris: uris,
        )

        {
          ids: maybe_cast_one_to_many(ids),
          metadatas: normalize_metadatas(metadatas),
          embeddings: base_record_set[:embeddings],
          documents: base_record_set[:documents],
          images: base_record_set[:images],
          uris: base_record_set[:uris]
        }
      end

      def validate_batch(batch, limits)
        ids = batch[0]
        max_batch = limits[:max_batch_size]
        if ids.length > max_batch
          raise ArgumentError,
                "Batch size #{ids.length} exceeds maximum batch size #{max_batch}"
        end
      end

      def normalize_sparse_vector(indices:, values:, labels: nil)
        return SparseVector.new(indices: [], values: [], labels: nil) if indices.empty?

        if labels
          triples = indices.zip(values, labels).sort_by { |pair| pair[0] }
          sorted_indices, sorted_values, sorted_labels = triples.transpose
          SparseVector.new(
            indices: sorted_indices,
            values: sorted_values,
            labels: sorted_labels,
          )
        else
          pairs = indices.zip(values).sort_by { |pair| pair[0] }
          sorted_indices, sorted_values = pairs.transpose
          SparseVector.new(indices: sorted_indices, values: sorted_values, labels: nil)
        end
      end
    end

    module Encoding
      F32_MAX = 3.402823466e+38
      F32_MIN = -3.402823466e+38

      module_function

      def to_f32(value)
        return Float::NAN if value.respond_to?(:nan?) && value.nan?
        return Float::INFINITY if value > F32_MAX
        return -Float::INFINITY if value < F32_MIN
        value.to_f
      end

      def pack_embedding_safely(embedding)
        embedding.pack("e*")
      rescue RangeError
        embedding.map { |value| to_f32(value) }.pack("e*")
      end

      def embeddings_to_base64_strings(embeddings)
        return nil if embeddings.nil?
        embeddings.map do |embedding|
          next nil if embedding.nil?
          Base64.strict_encode64(pack_embedding_safely(embedding))
        end
      end

      def base64_strings_to_embeddings(b64_strings)
        return nil if b64_strings.nil?
        b64_strings.map do |b64|
          next nil if b64.nil?
          bytes = Base64.decode64(b64)
          bytes.unpack("e*")
        end
      end
    end
  end
end
