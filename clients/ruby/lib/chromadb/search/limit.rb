# frozen_string_literal: true

module Chroma
  module Search
    class Limit
      attr_reader :offset, :limit

      def initialize(offset: 0, limit: nil)
        unless offset.is_a?(Integer) && offset >= 0
          raise TypeError, "Limit offset must be a non-negative integer"
        end
        if !limit.nil?
          unless limit.is_a?(Integer) && limit > 0
            raise TypeError, "Limit must be a positive integer when provided"
          end
        end
        @offset = offset
        @limit = limit
      end

      def self.from(input, offset_override = nil)
        return Limit.new(offset: input.offset, limit: input.limit) if input.is_a?(Limit)
        if input.is_a?(Numeric)
          return Limit.new(limit: input.to_i, offset: offset_override || 0)
        end
        return Limit.new if input.nil?
        if input.is_a?(Hash)
          return Limit.new(offset: input[:offset] || input["offset"] || 0,
                           limit: input[:limit] || input["limit"])
        end
        raise TypeError, "Invalid limit input"
      end

      def to_h
        result = { "offset" => @offset }
        result["limit"] = @limit if @limit
        result
      end
    end
  end
end
