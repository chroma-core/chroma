# frozen_string_literal: true

require "spec_helper"

RSpec.describe Chroma::Types::SparseVector do
  it "serializes and deserializes sparse vectors" do
    vector = described_class.new(indices: [ 1, 3 ], values: [ 0.5, 1.25 ], labels: [ "foo", "bar" ])
    hash = vector.to_h

    expect(hash["#type"]).to eq("sparse_vector")
    expect(hash["indices"]).to eq([ 1, 3 ])
    expect(hash["values"]).to eq([ 0.5, 1.25 ])
    expect(hash["tokens"]).to eq([ "foo", "bar" ])

    restored = described_class.from_h(hash)
    expect(restored.indices).to eq([ 1, 3 ])
    expect(restored.values).to eq([ 0.5, 1.25 ])
    expect(restored.labels).to eq([ "foo", "bar" ])
  end

  it "rejects unsorted indices" do
    expect do
      described_class.new(indices: [ 2, 1 ], values: [ 0.5, 0.6 ])
    end.to raise_error(ArgumentError, /strictly ascending/)
  end
end

RSpec.describe Chroma::Types::Validation do
  it "serializes and deserializes metadata with sparse vectors" do
    vector = Chroma::Types::SparseVector.new(indices: [ 2 ], values: [ 0.25 ])
    metadata = { "sparse" => vector, "tag" => "alpha" }

    serialized = described_class.serialize_metadata(metadata)
    expect(serialized["sparse"]).to be_a(Hash)
    expect(serialized["sparse"]["#type"]).to eq("sparse_vector")

    restored = described_class.deserialize_metadata(serialized)
    expect(restored["sparse"]).to be_a(Chroma::Types::SparseVector)
    expect(restored["tag"]).to eq("alpha")
  end
end
