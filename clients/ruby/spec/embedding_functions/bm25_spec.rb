# frozen_string_literal: true

require "spec_helper"

RSpec.describe Chroma::EmbeddingFunctions::ChromaBm25EmbeddingFunction do
  let(:embedder) { described_class.new }

  it "matches comprehensive tokenization expectations" do
    embedding = embedder.call([ "Usain Bolt's top speed reached ~27.8 mph (44.72 km/h)" ]).first

    expected_indices = [
      230246813, 395514983, 458027949, 488165615, 729632045, 734978415,
      997512866, 1114505193, 1381820790, 1501587190, 1649421877, 1837285388
    ]
    expected_value = 1.6391153

    expect(embedding.indices).to eq(expected_indices)
    embedding.values.each do |value|
      expect(value).to be_within(1e-5).of(expected_value)
    end
  end

  it "matches rust compatibility tokenization" do
    embedding = embedder.call([
      "The   space-time   continuum   WARPS   near   massive   objects..."
    ]).first

    expected_indices = [
      90097469, 519064992, 737893654, 1110755108, 1950894484, 2031641008,
      2058513491
    ]
    expected_value = 1.660867

    expect(embedding.indices).to eq(expected_indices)
    embedding.values.each do |value|
      expect(value).to be_within(1e-5).of(expected_value)
    end
  end

  it "round trips config" do
    config = embedder.get_config
    expect(config["k"]).to eq(1.2)
    expect(config["b"]).to eq(0.75)
    expect(config["avg_doc_length"]).to eq(256.0)
    expect(config["token_max_length"]).to eq(40)

    custom = described_class.build_from_config(config.merge("stopwords" => described_class::DEFAULT_CHROMA_BM25_STOPWORDS[0, 10]))
    rebuilt = custom.get_config
    expect(rebuilt["stopwords"]).to eq(described_class::DEFAULT_CHROMA_BM25_STOPWORDS[0, 10])
  end
end
