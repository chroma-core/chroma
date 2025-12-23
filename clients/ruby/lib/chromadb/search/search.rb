# frozen_string_literal: true

require "set"
module Chroma
  module Search
    class Search
      attr_reader :where_clause, :rank_expression, :limit_config, :select_config

      def initialize(where: nil, rank: nil, limit: nil, select: nil)
        @where_clause = WhereExpression.from(where) if where
        @rank_expression = RankExpression.from(rank) if rank
        @limit_config = Limit.from(limit)
        @select_config = Select.from(select)
      end

      def where(where = nil)
        clone_with(where: WhereExpression.from(where))
      end

      def rank(rank = nil)
        clone_with(rank: RankExpression.from(rank))
      end

      def limit(limit = nil, offset = nil)
        if limit.is_a?(Numeric)
          clone_with(limit: Limit.from(limit.to_i, offset))
        else
          clone_with(limit: Limit.from(limit))
        end
      end

      def select(*keys)
        if keys.length == 1 && (keys[0].is_a?(Array) || keys[0].is_a?(Set))
          return clone_with(select: Select.from(keys[0]))
        end
        if keys.length == 1 && keys[0].is_a?(Select)
          return clone_with(select: Select.from(keys[0]))
        end
        if keys.length == 1 && keys[0].is_a?(Hash) && (keys[0].key?(:keys) || keys[0].key?("keys"))
          return clone_with(select: Select.from(keys[0]))
        end

        clone_with(select: Select.from(keys))
      end

      def select_all
        clone_with(select: Select.all)
      end

      def to_h
        payload = {
          "limit" => @limit_config.to_h,
          "select" => @select_config.to_h
        }

        payload["filter"] = @where_clause.to_h if @where_clause
        payload["rank"] = @rank_expression.to_h if @rank_expression
        payload
      end

      private

      def clone_with(where: @where_clause, rank: @rank_expression, limit: @limit_config, select: @select_config)
        instance = self.class.allocate
        instance.instance_variable_set(:@where_clause, where)
        instance.instance_variable_set(:@rank_expression, rank)
        instance.instance_variable_set(:@limit_config, limit)
        instance.instance_variable_set(:@select_config, select)
        instance
      end
    end
  end
end
