# frozen_string_literal: true

require "spec_helper"

RSpec.describe Chroma::Search::GroupBy do
  it "serializes group_by with aggregates" do
    group_by = described_class.new(
      keys: Chroma::Search::K["category"],
      aggregate: Chroma::Search::MinK.new(keys: Chroma::Search::K::SCORE, k: 2),
    )

    expect(group_by.to_h).to eq({
      "keys" => [ "category" ],
      "aggregate" => { "$min_k" => { "keys" => [ "#score" ], "k" => 2 } }
    })
  end

  it "accepts hash inputs" do
    group_by = described_class.from({
      "keys" => [ "category" ],
      "aggregate" => { "$max_k" => { "keys" => [ "#score" ], "k" => 1 } }
    })

    expect(group_by.to_h).to eq({
      "keys" => [ "category" ],
      "aggregate" => { "$max_k" => { "keys" => [ "#score" ], "k" => 1 } }
    })
  end

  it "validates required fields" do
    expect do
      described_class.from({ "keys" => [ "category" ] })
    end.to raise_error(ArgumentError, /aggregate/)

    expect do
      described_class.from({ "aggregate" => { "$min_k" => { "keys" => [ "#score" ], "k" => 1 } } })
    end.to raise_error(ArgumentError, /keys/)
  end

  it "rejects invalid aggregates" do
    expect do
      Chroma::Search::MinK.new(keys: [], k: 1)
    end.to raise_error(ArgumentError, /cannot be empty/)

    expect do
      Chroma::Search::MaxK.new(keys: [ "#score" ], k: 0)
    end.to raise_error(TypeError, /positive integer/)
  end
end
