# frozen_string_literal: true

require "spec_helper"
require "securerandom"

RSpec.describe "Chroma Ruby cloud integration", :cloud do
  before(:all) do
    @skip_cloud = ENV["CHROMA_CLOUD_INTEGRATION_TESTS"] != "1"
    return if @skip_cloud

    @api_key = ENV["CHROMA_API_KEY"] || ENV["RUBY_INTEGRATION_TEST_CHROMA_API_KEY"]
    if @api_key.nil? || @api_key.empty?
      raise ArgumentError,
            "CHROMA_API_KEY or RUBY_INTEGRATION_TEST_CHROMA_API_KEY must be set for cloud integration tests"
    end

    @original_api_key = ENV["CHROMA_API_KEY"]
    @api_key_modified = false
    if @original_api_key.nil? || @original_api_key.empty?
      ENV["CHROMA_API_KEY"] = @api_key
      @api_key_modified = true
    end

    Chroma.reset_configuration!
    Chroma.configure do |config|
      config.cloud_api_key = @api_key
    end

    @cloud_config = Chroma.configuration
    @cloud_host = @cloud_config.cloud_host
    @cloud_port = @cloud_config.cloud_port
    @cloud_ssl = @cloud_config.cloud_ssl
    @collection_names = []

    @webmock_overridden = true
    WebMock.allow_net_connect!

    @cloud_client = Chroma::CloudClient.new

    identity = @cloud_client.get_user_identity
    tenant = identity.is_a?(Hash) ? identity["tenant"] : nil
    if tenant.nil? || tenant.empty?
      raise ArgumentError, "Cloud identity did not include a tenant"
    end
    @tenant = tenant
    Chroma.configure do |config|
      config.cloud_tenant = @tenant
    end

    @admin_client = Chroma::AdminClient.new(
      host: @cloud_host,
      port: @cloud_port,
      ssl: @cloud_ssl,
      headers: { "x-chroma-token" => @api_key },
      tenant: @tenant
    )

    @database = "ruby_cloud_it_#{SecureRandom.hex(6)}"
    @admin_client.create_database(@database, tenant: @tenant)

    Chroma.configure do |config|
      config.cloud_database = @database
    end
    @cloud_db_client = Chroma::CloudClient.new
  end

  before do
    skip "Set CHROMA_CLOUD_INTEGRATION_TESTS=1 to run cloud integration tests" if @skip_cloud
  end

  after(:all) do
    return if @skip_cloud

    @collection_names&.each do |name|
      begin
        @cloud_db_client&.delete_collection(name: name)
      rescue StandardError
        # best-effort cleanup
      end
    end

    if @database && @tenant
      begin
        @admin_client&.delete_database(@database, tenant: @tenant)
      rescue StandardError
        # best-effort cleanup
      end
    end

    WebMock.disable_net_connect!(allow_localhost: true) if @webmock_overridden

    if @api_key_modified
      if @original_api_key.nil? || @original_api_key.empty?
        ENV.delete("CHROMA_API_KEY")
      else
        ENV["CHROMA_API_KEY"] = @original_api_key
      end
    end

    Chroma.reset_configuration!
  end

  it "exercises cloud workflows with dense and sparse embeddings, search, and fork" do
    dense_ef = Chroma::EmbeddingFunctions::ChromaCloudQwenEmbeddingFunction.new
    sparse_ef = Chroma::EmbeddingFunctions::ChromaCloudSpladeEmbeddingFunction.new

    schema = Chroma::Schema.new
    schema.create_index(config: Chroma::VectorIndexConfig.new(embedding_function: dense_ef))
    schema.create_index(
      config: Chroma::SparseVectorIndexConfig.new(
        embedding_function: sparse_ef,
        source_key: Chroma::DOCUMENT_KEY
      ),
      key: "sparse_embedding"
    )

    collection_name = "ruby_cloud_coll_#{SecureRandom.hex(6)}"
    @collection_names << collection_name
    collection = @cloud_db_client.create_collection(name: collection_name, schema: schema)

    ids = %w[alpha beta gamma]
    documents = [ "alpha document", "beta document", "gamma document" ]
    metadatas = [
      { "category" => "alpha", "kind" => "test" },
      { "category" => "beta", "kind" => "test" },
      { "category" => "gamma", "kind" => "test" }
    ]

    collection.add(ids: ids, documents: documents, metadatas: metadatas)

    get_result = collection.get(ids: [ "alpha" ], include: [ "documents", "metadatas" ])
    expect(get_result.ids.flatten).to include("alpha")
    metadata = get_result.metadatas.flatten.compact.first
    expect(metadata["category"]).to eq("alpha")
    expect(metadata["sparse_embedding"]).to be_a(Chroma::Types::SparseVector)

    query_result = collection.query(
      query_texts: [ "alpha" ],
      n_results: 2,
      include: [ "documents", "metadatas", "distances" ]
    )
    expect(query_result.ids.first).not_to be_empty
    expect(query_result.documents.first).not_to be_empty

    search = Chroma::Search::Search.new
      .where(Chroma::Search::K["category"].eq("alpha"))
      .rank(Chroma::Search.Knn(query: "alpha", key: "sparse_embedding", limit: 2))
      .limit(2)
      .select_all

    search_result = collection.search(search)
    expect(search_result.ids.first).not_to be_empty
    expect(search_result.scores.first.first).not_to be_nil

    limit = 2
    rank_limit = [ limit * 5, limit, 128 ].max
    dense_weight = 1.0
    sparse_weight = 1.0
    rrf_k = 60

    dense_knn = Chroma::Search.Knn(
      query: "alpha",
      key: Chroma::Search::K::EMBEDDING,
      limit: rank_limit,
      return_rank: true,
    )
    sparse_knn = Chroma::Search.Knn(
      query: "alpha",
      key: "sparse_embedding",
      limit: rank_limit,
      return_rank: true,
    )

    rrf_rank = Chroma::Search.Rrf(
      ranks: [ dense_knn, sparse_knn ],
      k: rrf_k,
      weights: [ dense_weight, sparse_weight ],
    )

    rrf_search = Chroma::Search::Search.new
      .rank(rrf_rank)
      .limit([ limit * 3, limit ].max)
      .select(Chroma::Search::K::DOCUMENT, Chroma::Search::K::SCORE, Chroma::Search::K::METADATA)

    rrf_result = collection.search(rrf_search)
    expect(rrf_result.ids.first).not_to be_empty
    expect(rrf_result.scores.first.first).to be_a(Numeric)

    fork_name = "ruby_cloud_fork_#{SecureRandom.hex(6)}"
    @collection_names << fork_name
    forked = collection.fork(name: fork_name)
    expect(forked).to be_a(Chroma::Collection)
    expect(forked.count).to eq(collection.count)

    fork_only_id = "fork_only_#{SecureRandom.hex(4)}"
    forked.add(
      ids: [ fork_only_id ],
      documents: [ "fork-only document" ],
      metadatas: [ { "category" => "fork", "kind" => "fork" } ],
    )

    fork_get = forked.get(ids: [ fork_only_id ], include: [ "documents", "metadatas" ])
    expect(fork_get.ids.flatten).to include(fork_only_id)

    original_get = collection.get(ids: [ fork_only_id ], include: [ "documents", "metadatas" ])
    expect(original_get.ids.flatten).not_to include(fork_only_id)
  end
end
