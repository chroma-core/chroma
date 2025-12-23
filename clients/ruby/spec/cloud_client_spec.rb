# frozen_string_literal: true

require "spec_helper"

RSpec.describe Chroma::CloudClient do
  before do
    ENV["CHROMA_API_KEY"] = "test-token"
    ENV["CHROMA_TENANT"] = "tenant-123"
    ENV["CHROMA_DATABASE"] = "db-123"
  end

  after do
    ENV.delete("CHROMA_API_KEY")
    ENV.delete("CHROMA_TENANT")
    ENV.delete("CHROMA_DATABASE")
  end

  it "uses the API key header and env tenant for admin endpoints" do
    stub_request(:get, "https://api.trychroma.com:443/api/v2/tenants/tenant-123/databases")
      .with(headers: { "x-chroma-token" => "test-token" })
      .to_return(
        status: 200,
        body: JSON.generate([]),
        headers: { "Content-Type" => "application/json" },
      )

    client = described_class.new
    expect(client.list_databases).to eq([])
  end
end
