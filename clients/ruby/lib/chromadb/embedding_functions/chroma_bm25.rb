# frozen_string_literal: true

require "set"
module Chroma
  module EmbeddingFunctions
    class Murmur3AbsHasher
      def initialize(seed = 0)
        @seed = seed
      end

      def hash(token)
        unsigned = murmur3(token)
        signed = unsigned >= 0x80000000 ? unsigned - 0x1_0000_0000 : unsigned
        signed.abs
      end

      private

      def murmur3(key)
        bytes = key.to_s.b.bytes
        h1 = @seed & 0xffffffff
        c1 = 0xcc9e2d51
        c2 = 0x1b873593
        length = bytes.length
        i = 0
        rounded = length - (length & 3)

        while i < rounded
          k1 = (bytes[i] & 0xff) |
                ((bytes[i + 1] & 0xff) << 8) |
                ((bytes[i + 2] & 0xff) << 16) |
                ((bytes[i + 3] & 0xff) << 24)
          i += 4

          k1 = imul(k1, c1)
          k1 = rotl32(k1, 15)
          k1 = imul(k1, c2)

          h1 ^= k1
          h1 = rotl32(h1, 13)
          h1 = (imul(h1, 5) + 0xe6546b64) & 0xffffffff
        end

        k1 = 0
        case length & 3
        when 3
          k1 ^= (bytes[i + 2] & 0xff) << 16
          k1 ^= (bytes[i + 1] & 0xff) << 8
          k1 ^= (bytes[i] & 0xff)
          k1 = imul(k1, c1)
          k1 = rotl32(k1, 15)
          k1 = imul(k1, c2)
          h1 ^= k1
        when 2
          k1 ^= (bytes[i + 1] & 0xff) << 8
          k1 ^= (bytes[i] & 0xff)
          k1 = imul(k1, c1)
          k1 = rotl32(k1, 15)
          k1 = imul(k1, c2)
          h1 ^= k1
        when 1
          k1 ^= (bytes[i] & 0xff)
          k1 = imul(k1, c1)
          k1 = rotl32(k1, 15)
          k1 = imul(k1, c2)
          h1 ^= k1
        end

        h1 ^= length
        h1 ^= (h1 >> 16)
        h1 = imul(h1, 0x85ebca6b)
        h1 ^= (h1 >> 13)
        h1 = imul(h1, 0xc2b2ae35)
        h1 ^= (h1 >> 16)
        h1 & 0xffffffff
      end

      def imul(a, b)
        ((a & 0xffffffff) * (b & 0xffffffff)) & 0xffffffff
      end

      def rotl32(x, r)
        ((x << r) | (x >> (32 - r))) & 0xffffffff
      end
    end

    class Bm25Tokenizer
      def initialize(stemmer, stopwords, token_max_length)
        @stemmer = stemmer
        @stopwords = stopwords.map { |word| word.to_s.downcase }.to_set
        @token_max_length = token_max_length
      end

      def tokenize(text)
        cleaned = remove_non_alphanumeric(text)
        raw_tokens = simple_tokenize(cleaned)
        tokens = []
        raw_tokens.each do |token|
          next if token.empty?
          next if @stopwords.include?(token)
          next if token.length > @token_max_length

          stemmed = stem(token).strip
          tokens << stemmed unless stemmed.empty?
        end
        tokens
      end

      private

      def remove_non_alphanumeric(text)
        text.to_s.gsub(/[^\p{L}\p{N}_\s]+/u, " ")
      end

      def simple_tokenize(text)
        text.downcase.split(/\s+/)
      end

      def stem(token)
        if @stemmer.respond_to?(:stem)
          @stemmer.stem(token)
        elsif @stemmer.respond_to?(:stem_word)
          @stemmer.stem_word(token)
        elsif token.respond_to?(:porter2_stem)
          token.porter2_stem
        else
          token
        end
      end
    end

    class Porter2StemmerAdapter
      def stem(token)
        token.to_s.porter2_stem
      end
    end

    class HashedToken
      attr_reader :hash, :label

      def initialize(hash, label)
        @hash = hash
        @label = label
      end

      def eql?(other)
        other.is_a?(HashedToken) && other.hash == @hash
      end

      def ==(other)
        eql?(other)
      end

      def hash
        @hash
      end
    end

    class ChromaBm25EmbeddingFunction
      NAME = "chroma_bm25"

      DEFAULT_K = 1.2
      DEFAULT_B = 0.75
      DEFAULT_AVG_DOC_LENGTH = 256.0
      DEFAULT_TOKEN_MAX_LENGTH = 40

      DEFAULT_CHROMA_BM25_STOPWORDS = [
        "a",
        "about",
        "above",
        "after",
        "again",
        "against",
        "ain",
        "all",
        "am",
        "an",
        "and",
        "any",
        "are",
        "aren",
        "aren't",
        "as",
        "at",
        "be",
        "because",
        "been",
        "before",
        "being",
        "below",
        "between",
        "both",
        "but",
        "by",
        "can",
        "couldn",
        "couldn't",
        "d",
        "did",
        "didn",
        "didn't",
        "do",
        "does",
        "doesn",
        "doesn't",
        "doing",
        "don",
        "don't",
        "down",
        "during",
        "each",
        "few",
        "for",
        "from",
        "further",
        "had",
        "hadn",
        "hadn't",
        "has",
        "hasn",
        "hasn't",
        "have",
        "haven",
        "haven't",
        "having",
        "he",
        "her",
        "here",
        "hers",
        "herself",
        "him",
        "himself",
        "his",
        "how",
        "i",
        "if",
        "in",
        "into",
        "is",
        "isn",
        "isn't",
        "it",
        "it's",
        "its",
        "itself",
        "just",
        "ll",
        "m",
        "ma",
        "me",
        "mightn",
        "mightn't",
        "more",
        "most",
        "mustn",
        "mustn't",
        "my",
        "myself",
        "needn",
        "needn't",
        "no",
        "nor",
        "not",
        "now",
        "o",
        "of",
        "off",
        "on",
        "once",
        "only",
        "or",
        "other",
        "our",
        "ours",
        "ourselves",
        "out",
        "over",
        "own",
        "re",
        "s",
        "same",
        "shan",
        "shan't",
        "she",
        "she's",
        "should",
        "should've",
        "shouldn",
        "shouldn't",
        "so",
        "some",
        "such",
        "t",
        "than",
        "that",
        "that'll",
        "the",
        "their",
        "theirs",
        "them",
        "themselves",
        "then",
        "there",
        "these",
        "they",
        "this",
        "those",
        "through",
        "to",
        "too",
        "under",
        "until",
        "up",
        "ve",
        "very",
        "was",
        "wasn",
        "wasn't",
        "we",
        "were",
        "weren",
        "weren't",
        "what",
        "when",
        "where",
        "which",
        "while",
        "who",
        "whom",
        "why",
        "will",
        "with",
        "won",
        "won't",
        "wouldn",
        "wouldn't",
        "y",
        "you",
        "you'd",
        "you'll",
        "you're",
        "you've",
        "your",
        "yours",
        "yourself",
        "yourselves"
      ].freeze

      attr_reader :k, :b, :avg_doc_length, :token_max_length, :stopwords, :include_tokens

      def initialize(k: DEFAULT_K, b: DEFAULT_B, avg_doc_length: DEFAULT_AVG_DOC_LENGTH, token_max_length: DEFAULT_TOKEN_MAX_LENGTH, stopwords: nil, include_tokens: false)
        @k = k.to_f
        @b = b.to_f
        @avg_doc_length = avg_doc_length.to_f
        @token_max_length = token_max_length.to_i
        @include_tokens = !!include_tokens

        if stopwords
          @stopwords = stopwords.map(&:to_s)
          @stopword_list = @stopwords
        else
          @stopwords = nil
          @stopword_list = DEFAULT_CHROMA_BM25_STOPWORDS
        end

        @hasher = Murmur3AbsHasher.new
        @stemmer = Porter2StemmerAdapter.new
      end

      def call(documents)
        return [] if documents.nil? || documents.empty?

        documents.map { |doc| encode(doc.to_s) }
      end

      def embed_query(documents)
        call(documents)
      end

      def name
        NAME
      end

      def get_config
        config = {
          "k" => @k,
          "b" => @b,
          "avg_doc_length" => @avg_doc_length,
          "token_max_length" => @token_max_length,
          "include_tokens" => @include_tokens
        }
        config["stopwords"] = @stopwords.dup if @stopwords
        config
      end

      def validate_config(config)
        EmbeddingFunctions.validate_config_schema(config, "chroma_bm25")
      end

      def validate_config_update(old_config, new_config)
        mutable_keys = %w[k b avg_doc_length token_max_length stopwords include_tokens]
        new_config.each_key do |key|
          next if mutable_keys.include?(key)
          raise ArgumentError, "Updating '#{key}' is not supported for #{NAME}"
        end
      end

      def self.build_from_config(config, client: nil)
        new(
          k: config["k"] || DEFAULT_K,
          b: config["b"] || DEFAULT_B,
          avg_doc_length: config["avg_doc_length"] || DEFAULT_AVG_DOC_LENGTH,
          token_max_length: config["token_max_length"] || DEFAULT_TOKEN_MAX_LENGTH,
          stopwords: config["stopwords"],
          include_tokens: config.fetch("include_tokens", false),
        )
      end

      def self.validate_config(config)
        EmbeddingFunctions.validate_config_schema(config, "chroma_bm25")
      end

      private

      def encode(text)
        tokenizer = Bm25Tokenizer.new(@stemmer, @stopword_list, @token_max_length)
        tokens = tokenizer.tokenize(text)
        return Types::SparseVector.new(indices: [], values: [], labels: nil) if tokens.empty?

        doc_len = tokens.length.to_f
        counts = Hash.new(0)

        tokens.each do |token|
          token_key = HashedToken.new(@hasher.hash(token), @include_tokens ? token : nil)
          counts[token_key] += 1
        end

        sorted_keys = counts.keys.sort_by(&:hash)
        indices = []
        values = []
        labels = @include_tokens ? [] : nil

        sorted_keys.each do |key|
          tf = counts[key].to_f
          denominator = tf + @k * (1 - @b + (@b * doc_len) / @avg_doc_length)
          score = tf * (@k + 1) / denominator
          indices << key.hash
          values << score
          labels << key.label if labels && key.label
        end

        Types::SparseVector.new(indices: indices, values: values, labels: labels)
      end
    end

    register_sparse_embedding_function(ChromaBm25EmbeddingFunction::NAME, ChromaBm25EmbeddingFunction)
    register_sparse_embedding_function("chroma-bm25", ChromaBm25EmbeddingFunction)
  end
end
