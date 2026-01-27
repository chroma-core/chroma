# frozen_string_literal: true

module Chroma
  module Types
    class GetResult
      attr_reader :ids, :embeddings, :metadatas, :documents, :uris, :data, :included

      def initialize(ids:, embeddings:, metadatas:, documents:, uris:, data:, included: nil)
        @ids = ids
        @embeddings = embeddings
        @metadatas = metadatas
        @documents = documents
        @uris = uris
        @data = data
        @included = included
      end

      def to_h
        {
          ids: @ids,
          embeddings: @embeddings,
          metadatas: @metadatas,
          documents: @documents,
          uris: @uris,
          data: @data,
          included: @included
        }
      end
    end

    class QueryResult < GetResult
      attr_reader :distances

      def initialize(ids:, embeddings:, metadatas:, documents:, uris:, data:, distances:, included: nil)
        super(ids: ids, embeddings: embeddings, metadatas: metadatas, documents: documents, uris: uris, data: data, included: included)
        @distances = distances
      end

      def to_h
        super.merge(distances: @distances)
      end
    end

    class SearchResult
      attr_reader :ids, :documents, :embeddings, :metadatas, :scores, :select

      def initialize(response)
        @ids = response.fetch("ids")
        payload_count = @ids.length

        @documents = normalize_payload_array(response["documents"], payload_count)
        @embeddings = normalize_payload_array(response["embeddings"], payload_count)
        raw_metadatas = normalize_payload_array(response["metadatas"], payload_count)
        @metadatas = raw_metadatas.map { |payload| payload ? Types::Validation.deserialize_metadatas(payload) : nil }
        @scores = normalize_payload_array(response["scores"], payload_count)
        @select = response["select"] || []
      end

      def rows
        results = []
        @ids.each_with_index do |id_batch, batch_index|
          docs = @documents[batch_index] || []
          embeds = @embeddings[batch_index] || []
          metas = @metadatas[batch_index] || []
          scores = @scores[batch_index] || []

          batch_rows = id_batch.map.with_index do |id, row_index|
            row = { id: id }
            doc = docs[row_index]
            row[:document] = doc unless doc.nil?
            emb = embeds[row_index]
            row[:embedding] = emb unless emb.nil?
            meta = metas[row_index]
            row[:metadata] = meta unless meta.nil?
            score = scores[row_index]
            row[:score] = score unless score.nil?
            row
          end
          results << batch_rows
        end
        results
      end

      private

      def normalize_payload_array(payload, count)
        return Array.new(count) { nil } if payload.nil?
        return payload.map { |item| item ? item.dup : nil } if payload.length == count

        result = payload.map { |item| item ? item.dup : nil }
        result.fill(nil, result.length...count)
        result
      end
    end
  end
end
