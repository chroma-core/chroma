# frozen_string_literal: true

openapi_root = File.expand_path("openapi/lib", __dir__)
$LOAD_PATH << openapi_root unless $LOAD_PATH.include?(openapi_root)

require_relative "openapi/lib/chromadb"
