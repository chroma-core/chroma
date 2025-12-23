# frozen_string_literal: true

require "spec_helper"

RSpec.describe "Chroma cloud embedding functions" do
  before do
    ENV["CHROMA_API_KEY"] = "test-token"
  end

  after do
    ENV.delete("CHROMA_API_KEY")
  end

  it "parses Qwen embeddings" do
    stub_request(:post, "https://embed.trychroma.com/")
      .to_return(
        status: 200,
        body: JSON.generate({ "embeddings" => [ [ 0.1, 0.2 ], [ 0.3, 0.4 ] ] }),
        headers: { "Content-Type" => "application/json" },
      )

    ef = Chroma::EmbeddingFunctions::ChromaCloudQwenEmbeddingFunction.new(
      model: "Qwen/Qwen3-Embedding-0.6B",
      task: "nl_to_code",
    )

    embeddings = ef.call([ "doc1", "doc2" ])
    expect(embeddings).to eq([ [ 0.1, 0.2 ], [ 0.3, 0.4 ] ])
  end

  it "parses Splade sparse embeddings with tokens" do
    stub_request(:post, "https://embed.trychroma.com/embed_sparse")
      .to_return(
        status: 200,
        body: JSON.generate({
          "embeddings" => [
            { "indices" => [ 2, 5 ], "values" => [ 0.5, 1.0 ], "tokens" => [ "alpha", "beta" ] }
          ]
        }),
        headers: { "Content-Type" => "application/json" },
      )

    ef = Chroma::EmbeddingFunctions::ChromaCloudSpladeEmbeddingFunction.new(include_tokens: true)
    embeddings = ef.call([ "doc" ])

    expect(embeddings.length).to eq(1)
    embedding = embeddings.first
    expect(embedding).to be_a(Chroma::Types::SparseVector)
    expect(embedding.indices).to eq([ 2, 5 ])
    expect(embedding.values).to eq([ 0.5, 1.0 ])
    expect(embedding.labels).to eq([ "alpha", "beta" ])
  end
end
