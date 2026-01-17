# frozen_string_literal: true

require_relative "types/sparse_vector"
require_relative "types/validation"
require_relative "types/results"

module Chroma
  module Types
    TYPE_KEY = "#type"
    SPARSE_VECTOR_TYPE_VALUE = "sparse_vector"
    META_KEY_CHROMA_DOCUMENT = "chroma:document"
  end
end
