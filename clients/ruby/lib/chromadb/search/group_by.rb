# frozen_string_literal: true

require "set"

module Chroma
  module Search
    module GroupByHelpers
      module_function

      def normalize_keys(keys, label)
        raise ArgumentError, "#{label} cannot be empty" if keys.nil?

        values = if keys.is_a?(Array) || keys.is_a?(Set)
          keys.to_a
        else
          [ keys ]
        end

        raise ArgumentError, "#{label} cannot be empty" if values.empty?

        values.map do |value|
          normalized = value.respond_to?(:name) ? value.name : value
          unless normalized.is_a?(String)
            raise TypeError, "#{label} must be strings or Key instances"
          end
          normalized
        end
      end

      def normalize_k(value, label)
        unless value.is_a?(Integer) && value.positive?
          raise TypeError, "#{label} must be a positive integer"
        end
        value
      end

      def fetch_value(hash, key)
        hash[key] || hash[key.to_s]
      end
    end
    private_constant :GroupByHelpers

    class Aggregate
      def to_h
        raise NotImplementedError
      end

      def self.from(input)
        return input if input.is_a?(Aggregate)
        unless input.is_a?(Hash)
          raise TypeError, "Aggregate input must be an Aggregate or Hash"
        end
        raise ArgumentError, "Aggregate hash must contain exactly one operator" unless input.length == 1

        operator, config = input.first
        operator = operator.to_s

        case operator
        when "$min_k"
          MinK.from_config(config)
        when "$max_k"
          MaxK.from_config(config)
        else
          raise ArgumentError, "Unknown aggregate operator: #{operator}"
        end
      end
    end

    class MinK < Aggregate
      attr_reader :keys, :k

      def initialize(keys:, k:)
        @keys = GroupByHelpers.normalize_keys(keys, "MinK keys")
        @k = GroupByHelpers.normalize_k(k, "MinK k")
      end

      def to_h
        { "$min_k" => { "keys" => @keys.dup, "k" => @k } }
      end

      def self.from_config(config)
        unless config.is_a?(Hash)
          raise TypeError, "$min_k requires a Hash"
        end

        keys = GroupByHelpers.fetch_value(config, :keys)
        k = GroupByHelpers.fetch_value(config, :k)
        raise ArgumentError, "$min_k requires 'keys' field" if keys.nil?
        raise ArgumentError, "$min_k requires 'k' field" if k.nil?

        new(keys: keys, k: k)
      end
    end

    class MaxK < Aggregate
      attr_reader :keys, :k

      def initialize(keys:, k:)
        @keys = GroupByHelpers.normalize_keys(keys, "MaxK keys")
        @k = GroupByHelpers.normalize_k(k, "MaxK k")
      end

      def to_h
        { "$max_k" => { "keys" => @keys.dup, "k" => @k } }
      end

      def self.from_config(config)
        unless config.is_a?(Hash)
          raise TypeError, "$max_k requires a Hash"
        end

        keys = GroupByHelpers.fetch_value(config, :keys)
        k = GroupByHelpers.fetch_value(config, :k)
        raise ArgumentError, "$max_k requires 'keys' field" if keys.nil?
        raise ArgumentError, "$max_k requires 'k' field" if k.nil?

        new(keys: keys, k: k)
      end
    end

    class GroupBy
      attr_reader :keys, :aggregate

      def initialize(keys: nil, aggregate: nil)
        if keys.nil? && aggregate.nil?
          @keys = []
          @aggregate = nil
          return
        end

        raise ArgumentError, "GroupBy requires 'keys' field" if keys.nil?
        raise ArgumentError, "GroupBy requires 'aggregate' field" if aggregate.nil?

        @keys = GroupByHelpers.normalize_keys(keys, "GroupBy keys")
        @aggregate = Aggregate.from(aggregate)
      end

      def self.from(input)
        return input if input.is_a?(GroupBy)
        return nil if input.nil?
        unless input.is_a?(Hash)
          raise TypeError, "GroupBy input must be a GroupBy or Hash"
        end

        return GroupBy.new if input.empty?

        keys = GroupByHelpers.fetch_value(input, :keys)
        aggregate = GroupByHelpers.fetch_value(input, :aggregate)

        GroupBy.new(keys: keys, aggregate: aggregate)
      end

      def empty?
        @keys.empty? || @aggregate.nil?
      end

      def to_h
        return {} if empty?

        {
          "keys" => @keys.dup,
          "aggregate" => @aggregate.to_h
        }
      end
    end
  end
end
