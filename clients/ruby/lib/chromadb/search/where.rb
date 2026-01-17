# frozen_string_literal: true

module Chroma
  module Search
    class WhereExpression
      def and(other)
        target = self.class.from(other)
        return self unless target
        AndWhere.combine(self, target)
      end

      def or(other)
        target = self.class.from(other)
        return self unless target
        OrWhere.combine(self, target)
      end

      def to_h
        raise NotImplementedError
      end

      def self.from(input)
        return input if input.is_a?(WhereExpression)
        return nil if input.nil?
        unless input.is_a?(Hash)
          raise TypeError, "Where input must be a WhereExpression or Hash"
        end
        parse_where_hash(input)
      end

      def self.create_comparison(key, operator, value)
        ComparisonWhere.new(key, operator, value)
      end

      def self.parse_where_hash(data)
        if data.key?("$and")
          raise ArgumentError, "$and cannot be combined with other keys" if data.length != 1
          raw = data["$and"]
          unless raw.is_a?(Array) && !raw.empty?
            raise TypeError, "$and must be a non-empty array"
          end
          conditions = raw.map.with_index do |item, index|
            expr = from(item)
            raise TypeError, "Invalid where clause at index #{index}" if expr.nil?
            expr
          end
          return conditions[0] if conditions.length == 1

          return conditions.drop(1).reduce(conditions[0]) { |acc, cond| AndWhere.combine(acc, cond) }
        end

        if data.key?("$or")
          raise ArgumentError, "$or cannot be combined with other keys" if data.length != 1
          raw = data["$or"]
          unless raw.is_a?(Array) && !raw.empty?
            raise TypeError, "$or must be a non-empty array"
          end
          conditions = raw.map.with_index do |item, index|
            expr = from(item)
            raise TypeError, "Invalid where clause at index #{index}" if expr.nil?
            expr
          end
          return conditions[0] if conditions.length == 1

          return conditions.drop(1).reduce(conditions[0]) { |acc, cond| OrWhere.combine(acc, cond) }
        end

        entries = data.to_a
        if entries.length != 1
          raise ArgumentError, "Where hash must contain exactly one field"
        end
        field, value = entries[0]
        unless value.is_a?(Hash)
          return ComparisonWhere.new(field, "$eq", value)
        end

        operator_entries = value.to_a
        if operator_entries.length != 1
          raise ArgumentError, "Operator hash for field '#{field}' must contain exactly one operator"
        end

        operator, operand = operator_entries[0]
        unless %w[$eq $ne $gt $gte $lt $lte $in $nin $contains $not_contains $regex $not_regex].include?(operator)
          raise ArgumentError, "Unsupported where operator: #{operator}"
        end

        ComparisonWhere.new(field, operator, operand)
      end
    end

    class AndWhere < WhereExpression
      def initialize(conditions)
        @conditions = conditions
      end

      def to_h
        { "$and" => @conditions.map(&:to_h) }
      end

      def operands
        @conditions.dup
      end

      def self.combine(left, right)
        flattened = []
        [ left, right ].each do |expr|
          if expr.is_a?(AndWhere)
            flattened.concat(expr.operands)
          else
            flattened << expr
          end
        end
        return flattened[0] if flattened.length == 1
        AndWhere.new(flattened)
      end
    end

    class OrWhere < WhereExpression
      def initialize(conditions)
        @conditions = conditions
      end

      def to_h
        { "$or" => @conditions.map(&:to_h) }
      end

      def operands
        @conditions.dup
      end

      def self.combine(left, right)
        flattened = []
        [ left, right ].each do |expr|
          if expr.is_a?(OrWhere)
            flattened.concat(expr.operands)
          else
            flattened << expr
          end
        end
        return flattened[0] if flattened.length == 1
        OrWhere.new(flattened)
      end
    end

    class ComparisonWhere < WhereExpression
      def initialize(key, operator, value)
        @key = key
        @operator = operator
        @value = value
      end

      def to_h
        { @key => { @operator => @value } }
      end
    end
  end
end
