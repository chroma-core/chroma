# frozen_string_literal: true

require "spec_helper"

RSpec.describe Chroma::Types::Validation do
  it "rejects duplicate IDs" do
    expect do
      described_class.validate_ids([ "a", "b", "a" ])
    end.to raise_error(Chroma::DuplicateIDError)
  end

  it "validates include options" do
    expect do
      described_class.validate_include([ "documents", "embeddings" ])
    end.not_to raise_error

    expect do
      described_class.validate_include([ "invalid" ])
    end.to raise_error(ArgumentError)
  end

  it "enforces max batch size limits" do
    expect do
      described_class.validate_batch([ %w[a b c] ], { max_batch_size: 2 })
    end.to raise_error(ArgumentError, /exceeds maximum batch size/)
  end

  it "validates embeddings" do
    expect do
      described_class.validate_embeddings([ [ 0.1, 0.2 ], [ 0.3, 0.4 ] ])
    end.not_to raise_error

    expect do
      described_class.validate_embeddings([ [ "bad" ] ])
    end.to raise_error(ArgumentError, /Numeric/)
  end

  it "validates metadata values" do
    expect do
      described_class.validate_metadatas([ { "topic" => "hello" } ])
    end.not_to raise_error

    expect do
      described_class.validate_metadatas([ { "bad" => [] } ])
    end.to raise_error(ArgumentError)
  end

  it "validates where clauses" do
    expect do
      described_class.validate_where({ "topic" => { "$eq" => "hello" } })
    end.not_to raise_error

    expect do
      described_class.validate_where({ "$and" => [ { "a" => 1 } ] })
    end.to raise_error(ArgumentError)
  end

  it "validates where_document clauses" do
    expect do
      described_class.validate_where_document({ "$contains" => "hello" })
    end.not_to raise_error

    expect do
      described_class.validate_where_document({ "$eq" => 1 })
    end.to raise_error(ArgumentError)
  end
end
