# frozen_string_literal: true

module Chroma
  module Search
    class Select
      def initialize(keys = [])
        unique = []
        seen = {}
        Array(keys).each do |key|
          normalized = key.respond_to?(:name) ? key.name : key
          unless normalized.is_a?(String)
            raise TypeError, "Select keys must be strings or Key instances"
          end
          next if seen[normalized]
          seen[normalized] = true
          unique << normalized
        end
        @keys = unique
      end

      def self.from(input)
        return Select.new(input.values) if input.is_a?(Select)
        return Select.new if input.nil?

        if input.is_a?(Hash) && (input.key?(:keys) || input.key?("keys"))
          keys = input[:keys] || input["keys"] || []
          return Select.new(keys)
        end

        if input.is_a?(String)
          return Select.new([ input ])
        end

        if input.respond_to?(:each)
          return Select.new(input)
        end

        raise TypeError, "Unsupported select input"
      end

      def self.all
        Select.new([ K::DOCUMENT, K::EMBEDDING, K::METADATA, K::SCORE ])
      end

      def values
        @keys.dup
      end

      def to_h
        { "keys" => values }
      end
    end
  end
end
