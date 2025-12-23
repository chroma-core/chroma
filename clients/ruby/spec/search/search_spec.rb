# frozen_string_literal: true

require "spec_helper"

RSpec.describe Chroma::Search::Search do
  it "builds a search payload" do
    where = Chroma::Search::K["type"].eq("doc")
    rank = Chroma::Search.Knn(query: [ 0.1, 0.2 ], key: "#embedding", limit: 5)

    search = described_class.new(where: where, rank: rank).limit(5).select_all
    payload = search.to_h

    expect(payload["limit"]).to eq({ "offset" => 0, "limit" => 5 })
    expect(payload["select"]).to eq({ "keys" => [ "#document", "#embedding", "#metadata", "#score" ] })
    expect(payload["filter"]).to eq({ "type" => { "$eq" => "doc" } })
    expect(payload["rank"]).to eq({ "$knn" => { "query" => [ 0.1, 0.2 ], "key" => "#embedding", "limit" => 5 } })
  end
end
