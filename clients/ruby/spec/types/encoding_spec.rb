# frozen_string_literal: true

require "spec_helper"

RSpec.describe Chroma::Types::Encoding do
  it "round trips embeddings through base64" do
    embeddings = [ [ 0.1, 1.25, -3.5 ] ]
    encoded = described_class.embeddings_to_base64_strings(embeddings)
    decoded = described_class.base64_strings_to_embeddings(encoded)

    expect(decoded.length).to eq(1)
    expect(decoded[0].length).to eq(3)
    expect(decoded[0][0]).to be_within(1e-6).of(0.1)
    expect(decoded[0][1]).to be_within(1e-6).of(1.25)
    expect(decoded[0][2]).to be_within(1e-6).of(-3.5)
  end
end
