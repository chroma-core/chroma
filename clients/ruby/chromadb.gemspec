# frozen_string_literal: true

require_relative "lib/chromadb/version"

Gem::Specification.new do |spec|
  spec.name = "chromadb"
  spec.version = Chroma::VERSION
  spec.authors = [ "Chroma Core" ]
  spec.email = [ "support@trychroma.com" ]

  spec.summary = "Chroma Ruby client"
  spec.description = "Ruby client for Chroma's HTTP API (dense + sparse embeddings)."
  spec.homepage = "https://github.com/chroma-core/chroma"
  spec.license = "Apache-2.0"

  spec.files = Dir.glob("{lib,bin,README.md}/**/*", File::FNM_DOTMATCH).reject { |f| File.directory?(f) }
  spec.require_paths = [ "lib" ]

  spec.required_ruby_version = ">= 3.0"

  spec.add_dependency "faraday", "~> 2.0"
  spec.add_dependency "base64", "~> 0.2"
  spec.add_dependency "json_schemer", "~> 0.2"
  spec.add_dependency "porter2stemmer", "~> 1.0"

  spec.add_development_dependency "rspec", "~> 3.13"
  spec.add_development_dependency "rubocop-rails-omakase", "~> 1.1"
  spec.add_development_dependency "webmock", "~> 3.23"
end
