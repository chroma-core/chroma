# frozen_string_literal: true

module Chroma
  module Search
    class RankExpression
      def to_h
        raise NotImplementedError
      end

      def add(*others)
        return self if others.empty?
        expressions = [ self ] + others.map.with_index { |item, idx| ::Chroma::Search.require_rank(item, "add operand #{idx}") }
        SumRankExpression.create(expressions)
      end

      def subtract(other)
        SubRankExpression.new(self, ::Chroma::Search.require_rank(other, "subtract operand"))
      end

      def multiply(*others)
        return self if others.empty?
        expressions = [ self ] + others.map.with_index { |item, idx| ::Chroma::Search.require_rank(item, "multiply operand #{idx}") }
        MulRankExpression.create(expressions)
      end

      def divide(other)
        DivRankExpression.new(self, ::Chroma::Search.require_rank(other, "divide operand"))
      end

      def negate
        multiply(-1)
      end

      def abs
        AbsRankExpression.new(self)
      end

      def exp
        ExpRankExpression.new(self)
      end

      def log
        LogRankExpression.new(self)
      end

      def max(*others)
        return self if others.empty?
        expressions = [ self ] + others.map.with_index { |item, idx| ::Chroma::Search.require_rank(item, "max operand #{idx}") }
        MaxRankExpression.create(expressions)
      end

      def min(*others)
        return self if others.empty?
        expressions = [ self ] + others.map.with_index { |item, idx| ::Chroma::Search.require_rank(item, "min operand #{idx}") }
        MinRankExpression.create(expressions)
      end

      def self.from(input)
        return input if input.is_a?(RankExpression)
        return nil if input.nil?
        if input.is_a?(Numeric)
          return ValueRankExpression.new(::Chroma::Search.require_number(input, "Val requires a numeric value"))
        end
        if input.is_a?(Hash)
          return RawRankExpression.new(input)
        end
        raise TypeError, "Rank input must be a RankExpression, number, or Hash"
      end
    end

    class RawRankExpression < RankExpression
      def initialize(raw)
        @raw = raw
      end

      def to_h
        deep_clone(@raw)
      end

      private

      def deep_clone(value)
        Marshal.load(Marshal.dump(value))
      end
    end

    class ValueRankExpression < RankExpression
      def initialize(value)
        @value = value
      end

      def to_h
        { "$val" => @value }
      end
    end

    class SumRankExpression < RankExpression
      def initialize(ranks)
        @ranks = ranks
      end

      def self.create(ranks)
        flattened = []
        ranks.each do |rank|
          if rank.is_a?(SumRankExpression)
            flattened.concat(rank.operands)
          else
            flattened << rank
          end
        end
        return flattened[0] if flattened.length == 1
        SumRankExpression.new(flattened)
      end

      def operands
        @ranks.dup
      end

      def to_h
        { "$sum" => @ranks.map(&:to_h) }
      end
    end

    class SubRankExpression < RankExpression
      def initialize(left, right)
        @left = left
        @right = right
      end

      def to_h
        { "$sub" => { "left" => @left.to_h, "right" => @right.to_h } }
      end
    end

    class MulRankExpression < RankExpression
      def initialize(ranks)
        @ranks = ranks
      end

      def self.create(ranks)
        flattened = []
        ranks.each do |rank|
          if rank.is_a?(MulRankExpression)
            flattened.concat(rank.operands)
          else
            flattened << rank
          end
        end
        return flattened[0] if flattened.length == 1
        MulRankExpression.new(flattened)
      end

      def operands
        @ranks.dup
      end

      def to_h
        { "$mul" => @ranks.map(&:to_h) }
      end
    end

    class DivRankExpression < RankExpression
      def initialize(left, right)
        @left = left
        @right = right
      end

      def to_h
        { "$div" => { "left" => @left.to_h, "right" => @right.to_h } }
      end
    end

    class AbsRankExpression < RankExpression
      def initialize(operand)
        @operand = operand
      end

      def to_h
        { "$abs" => @operand.to_h }
      end
    end

    class ExpRankExpression < RankExpression
      def initialize(operand)
        @operand = operand
      end

      def to_h
        { "$exp" => @operand.to_h }
      end
    end

    class LogRankExpression < RankExpression
      def initialize(operand)
        @operand = operand
      end

      def to_h
        { "$log" => @operand.to_h }
      end
    end

    class MaxRankExpression < RankExpression
      def initialize(ranks)
        @ranks = ranks
      end

      def self.create(ranks)
        flattened = []
        ranks.each do |rank|
          if rank.is_a?(MaxRankExpression)
            flattened.concat(rank.operands)
          else
            flattened << rank
          end
        end
        return flattened[0] if flattened.length == 1
        MaxRankExpression.new(flattened)
      end

      def operands
        @ranks.dup
      end

      def to_h
        { "$max" => @ranks.map(&:to_h) }
      end
    end

    class MinRankExpression < RankExpression
      def initialize(ranks)
        @ranks = ranks
      end

      def self.create(ranks)
        flattened = []
        ranks.each do |rank|
          if rank.is_a?(MinRankExpression)
            flattened.concat(rank.operands)
          else
            flattened << rank
          end
        end
        return flattened[0] if flattened.length == 1
        MinRankExpression.new(flattened)
      end

      def operands
        @ranks.dup
      end

      def to_h
        { "$min" => @ranks.map(&:to_h) }
      end
    end

    class KnnRankExpression < RankExpression
      def initialize(config)
        @config = config
      end

      def to_h
        base = {
          "query" => @config[:query],
          "key" => @config[:key],
          "limit" => @config[:limit]
        }
        base["default"] = @config[:default_value] if @config.key?(:default_value)
        base["return_rank"] = true if @config[:return_rank]
        { "$knn" => base }
      end
    end

    def self.Val(value)
      ValueRankExpression.new(require_number(value, "Val requires a numeric value"))
    end

    def self.Knn(options)
      KnnRankExpression.new(normalize_knn_options(options))
    end

    def self.Rrf(ranks:, k: 60, weights: nil, normalize: false)
      unless k.is_a?(Integer) && k > 0
        raise TypeError, "Rrf k must be a positive integer"
      end
      unless ranks.is_a?(Array) && !ranks.empty?
        raise TypeError, "Rrf requires at least one rank expression"
      end

      expressions = ranks.map.with_index { |rank, index| require_rank(rank, "ranks[#{index}]") }

      weight_values = weights ? weights.dup : Array.new(expressions.length, 1)
      if weight_values.length != expressions.length
        raise ArgumentError, "Number of weights must match number of ranks"
      end
      if weight_values.any? { |value| !value.is_a?(Numeric) || value.negative? }
        raise TypeError, "Weights must be non-negative numbers"
      end

      if normalize
        total = weight_values.reduce(0.0) { |sum, value| sum + value }
        raise ArgumentError, "Weights must sum to a positive value when normalize=true" if total <= 0
        weight_values = weight_values.map { |value| value / total }
      end

      terms = expressions.map.with_index do |rank, index|
        weight = weight_values[index]
        numerator = Val(weight)
        denominator = rank.add(k)
        numerator.divide(denominator)
      end

      fused = terms.reduce { |acc, term| acc.add(term) }
      fused.negate
    end

    def self.Sum(*inputs)
      raise ArgumentError, "Sum requires at least one rank expression" if inputs.empty?
      expressions = inputs.map.with_index { |rank, index| require_rank(rank, "Sum operand #{index}") }
      SumRankExpression.create(expressions)
    end

    def self.Sub(left, right)
      SubRankExpression.new(require_rank(left, "Sub left"), require_rank(right, "Sub right"))
    end

    def self.Mul(*inputs)
      raise ArgumentError, "Mul requires at least one rank expression" if inputs.empty?
      expressions = inputs.map.with_index { |rank, index| require_rank(rank, "Mul operand #{index}") }
      MulRankExpression.create(expressions)
    end

    def self.Div(left, right)
      DivRankExpression.new(require_rank(left, "Div left"), require_rank(right, "Div right"))
    end

    def self.Abs(input)
      require_rank(input, "Abs").abs
    end

    def self.Exp(input)
      require_rank(input, "Exp").exp
    end

    def self.Log(input)
      require_rank(input, "Log").log
    end

    def self.Max(*inputs)
      raise ArgumentError, "Max requires at least one rank expression" if inputs.empty?
      expressions = inputs.map.with_index { |rank, index| require_rank(rank, "Max operand #{index}") }
      MaxRankExpression.create(expressions)
    end

    def self.Min(*inputs)
      raise ArgumentError, "Min requires at least one rank expression" if inputs.empty?
      expressions = inputs.map.with_index { |rank, index| require_rank(rank, "Min operand #{index}") }
      MinRankExpression.create(expressions)
    end

    def self.require_rank(input, context)
      result = RankExpression.from(input)
      raise TypeError, "#{context} must be a rank expression" unless result
      result
    end

    def self.require_number(value, message)
      unless value.is_a?(Numeric) && value.finite?
        raise TypeError, message
      end
      value.to_f
    end

    def self.normalize_knn_options(options)
      limit = options[:limit] || options["limit"] || 128
      unless limit.is_a?(Integer) && limit > 0
        raise TypeError, "Knn limit must be a positive integer"
      end

      query_input = options[:query] || options["query"]
      query = if query_input.is_a?(String)
        query_input
      elsif query_input.is_a?(Chroma::Types::SparseVector)
        { "indices" => query_input.indices.dup, "values" => query_input.values.dup }
      elsif query_input.is_a?(Hash) && query_input.key?(:indices) && query_input.key?(:values)
        { "indices" => query_input[:indices].dup, "values" => query_input[:values].dup }
      else
        normalize_dense_vector(query_input)
      end

      key_input = options[:key] || options["key"]
      key = key_input.respond_to?(:name) ? key_input.name : key_input
      key ||= "#embedding"
      raise TypeError, "Knn key must be a String or Key instance" unless key.is_a?(String)

      default_value = options.key?(:default) ? options[:default] : options["default"]
      if !default_value.nil?
        default_value = require_number(default_value, "Knn default must be a number")
        raise TypeError, "Knn default must be a finite number" unless default_value.finite?
      end

      return_rank = options.key?(:returnRank) ? options[:returnRank] : options["returnRank"]
      return_rank = options.key?(:return_rank) ? options[:return_rank] : return_rank

      {
        query: query.is_a?(Hash) ? Marshal.load(Marshal.dump(query)) : query,
        key: key,
        limit: limit,
        return_rank: !!return_rank
      }.tap do |config|
        config[:default_value] = default_value unless default_value.nil?
      end
    end

    def self.normalize_dense_vector(vector)
      array = vector.is_a?(Array) ? vector.dup : vector.to_a
      array.map do |value|
        unless value.is_a?(Numeric) && value.finite?
          raise TypeError, "Dense query vector values must be finite numbers"
        end
        value.to_f
      end
    end
  end
end
