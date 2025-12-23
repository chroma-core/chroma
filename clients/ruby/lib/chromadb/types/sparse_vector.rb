# frozen_string_literal: true

module Chroma
  module Types
    class SparseVector
      attr_reader :indices, :values, :labels

      def initialize(indices:, values:, labels: nil)
        @indices = indices
        @values = values
        @labels = labels
        validate!
      end

      def to_h
        result = {
          TYPE_KEY => SPARSE_VECTOR_TYPE_VALUE,
          "indices" => @indices,
          "values" => @values
        }
        result["tokens"] = @labels if @labels
        result
      end

      def self.from_h(data)
        unless data.is_a?(Hash) && data[TYPE_KEY] == SPARSE_VECTOR_TYPE_VALUE
          raise ArgumentError,
                "Expected #{TYPE_KEY}='#{SPARSE_VECTOR_TYPE_VALUE}', got #{data[TYPE_KEY].inspect}"
        end
        new(indices: data.fetch("indices"), values: data.fetch("values"), labels: data["tokens"])
      end

      private

      def validate!
        unless @indices.is_a?(Array)
          raise ArgumentError, "Expected SparseVector indices to be an Array, got #{@indices.class}"
        end
        unless @values.is_a?(Array)
          raise ArgumentError, "Expected SparseVector values to be an Array, got #{@values.class}"
        end
        if @indices.length != @values.length
          raise ArgumentError,
                "SparseVector indices and values must have the same length, got #{@indices.length} indices and #{@values.length} values"
        end

        if @labels
          unless @labels.is_a?(Array)
            raise ArgumentError, "Expected SparseVector labels to be an Array, got #{@labels.class}"
          end
          if @labels.length != @indices.length
            raise ArgumentError,
                  "SparseVector labels must match indices length, got #{@labels.length} labels and #{@indices.length} indices"
          end
        end

        @indices.each_with_index do |idx, i|
          unless idx.is_a?(Integer)
            raise ArgumentError,
                  "SparseVector indices must be integers, got #{idx.inspect} at position #{i}"
          end
          if idx.negative?
            raise ArgumentError,
                  "SparseVector indices must be non-negative, got #{idx} at position #{i}"
          end
        end

        @values.each_with_index do |val, i|
          unless val.is_a?(Numeric)
            raise ArgumentError,
                  "SparseVector values must be numeric, got #{val.inspect} at position #{i}"
          end
        end

        if @indices.length > 1
          @indices.each_cons(2).with_index do |(prev, curr), i|
            if curr <= prev
              raise ArgumentError,
                    "SparseVector indices must be strictly ascending, found indices[#{i + 1}]=#{curr} <= indices[#{i}]=#{prev}"
            end
          end
        end
      end
    end
  end
end
