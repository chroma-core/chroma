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

  it "includes group_by in the payload" do
    group_by = Chroma::Search::GroupBy.new(
      keys: Chroma::Search::K["category"],
      aggregate: Chroma::Search::MinK.new(keys: Chroma::Search::K::SCORE, k: 3),
    )

    search = described_class.new.group_by(group_by).limit(5).select("title")
    payload = search.to_h

    expect(payload["group_by"]).to eq({
      "keys" => [ "category" ],
      "aggregate" => { "$min_k" => { "keys" => [ "#score" ], "k" => 3 } }
    })
    expect(payload["select"]).to eq({ "keys" => [ "title" ] })
  end
end
