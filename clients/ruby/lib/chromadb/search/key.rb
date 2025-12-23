# frozen_string_literal: true

module Chroma
  module Search
    class Key
      attr_reader :name

      def initialize(name)
        @name = name
      end

      def eq(value)
        WhereExpression.create_comparison(@name, "$eq", value)
      end

      def ne(value)
        WhereExpression.create_comparison(@name, "$ne", value)
      end

      def gt(value)
        WhereExpression.create_comparison(@name, "$gt", value)
      end

      def gte(value)
        WhereExpression.create_comparison(@name, "$gte", value)
      end

      def lt(value)
        WhereExpression.create_comparison(@name, "$lt", value)
      end

      def lte(value)
        WhereExpression.create_comparison(@name, "$lte", value)
      end

      def is_in(values)
        array = iterable_to_array(values)
        assert_non_empty(array, "$in requires at least one value")
        WhereExpression.create_comparison(@name, "$in", array)
      end

      def not_in(values)
        array = iterable_to_array(values)
        assert_non_empty(array, "$nin requires at least one value")
        WhereExpression.create_comparison(@name, "$nin", array)
      end

      def contains(value)
        raise TypeError, "$contains requires a string value" unless value.is_a?(String)
        WhereExpression.create_comparison(@name, "$contains", value)
      end

      def not_contains(value)
        raise TypeError, "$not_contains requires a string value" unless value.is_a?(String)
        WhereExpression.create_comparison(@name, "$not_contains", value)
      end

      def regex(pattern)
        raise TypeError, "$regex requires a string pattern" unless pattern.is_a?(String)
        WhereExpression.create_comparison(@name, "$regex", pattern)
      end

      def not_regex(pattern)
        raise TypeError, "$not_regex requires a string pattern" unless pattern.is_a?(String)
        WhereExpression.create_comparison(@name, "$not_regex", pattern)
      end

      private

      def iterable_to_array(values)
        return values.to_a if values.respond_to?(:to_a)
        Array(values)
      end

      def assert_non_empty(array, message)
        raise ArgumentError, message if array.empty?
      end
    end

    module K
      ID = Key.new("#id")
      DOCUMENT = Key.new("#document")
      EMBEDDING = Key.new("#embedding")
      METADATA = Key.new("#metadata")
      SCORE = Key.new("#score")

      module_function

      def [](name)
        Key.new(name)
      end
    end
  end
end
