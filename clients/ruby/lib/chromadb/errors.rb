# frozen_string_literal: true

module Chroma
  class ChromaError < StandardError
    attr_accessor :trace_id

    def self.name
      "ChromaError"
    end
  end

  class InvalidDimensionError < ChromaError
    def self.name
      "InvalidDimension"
    end
  end

  class IDAlreadyExistsError < ChromaError
    def self.name
      "IDAlreadyExists"
    end
  end

  class ChromaAuthError < ChromaError
    def self.name
      "AuthError"
    end
  end

  class DuplicateIDError < ChromaError
    def self.name
      "DuplicateID"
    end
  end

  class InvalidArgumentError < ChromaError
    def self.name
      "InvalidArgument"
    end
  end

  class InvalidUUIDError < ChromaError
    def self.name
      "InvalidUUID"
    end
  end

  class InvalidHTTPVersionError < ChromaError
    def self.name
      "InvalidHTTPVersion"
    end
  end

  class AuthorizationError < ChromaError
    def self.name
      "AuthorizationError"
    end
  end

  class NotFoundError < ChromaError
    def self.name
      "NotFoundError"
    end
  end

  class UniqueConstraintError < ChromaError
    def self.name
      "UniqueConstraintError"
    end
  end

  class BatchSizeExceededError < ChromaError
    def self.name
      "BatchSizeExceededError"
    end
  end

  class VersionMismatchError < ChromaError
    def self.name
      "VersionMismatchError"
    end
  end

  class InternalError < ChromaError
    def self.name
      "InternalError"
    end
  end

  class RateLimitError < ChromaError
    def self.name
      "RateLimitError"
    end
  end

  class QuotaError < ChromaError
    def self.name
      "QuotaError"
    end
  end

  ERROR_TYPES = {
    "InvalidDimension" => InvalidDimensionError,
    "InvalidArgumentError" => InvalidArgumentError,
    "IDAlreadyExists" => IDAlreadyExistsError,
    "DuplicateID" => DuplicateIDError,
    "InvalidUUID" => InvalidUUIDError,
    "InvalidHTTPVersion" => InvalidHTTPVersionError,
    "AuthorizationError" => AuthorizationError,
    "NotFoundError" => NotFoundError,
    "BatchSizeExceededError" => BatchSizeExceededError,
    "VersionMismatchError" => VersionMismatchError,
    "RateLimitError" => RateLimitError,
    "AuthError" => ChromaAuthError,
    "UniqueConstraintError" => UniqueConstraintError,
    "QuotaError" => QuotaError,
    "InternalError" => InternalError,
    "ChromaError" => ChromaError
  }.freeze
end
