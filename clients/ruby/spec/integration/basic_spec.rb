# frozen_string_literal: true

require "spec_helper"
require "securerandom"

RSpec.describe "Chroma Ruby integration" do
  before(:all) do
    host = ENV.fetch("CHROMA_SERVER_HOST", nil)
    skip("CHROMA_SERVER_HOST not set") unless host

    @host = host
    @port = Integer(ENV.fetch("CHROMA_SERVER_HTTP_PORT", "8000"))
    @client = Chroma::HttpClient.new(host: @host, port: @port)
    @admin = Chroma::AdminClient.new(host: @host, port: @port)
  end

  it "runs basic collection CRUD" do
    collection_name = "ruby_test_#{SecureRandom.hex(4)}"
    collection = @client.get_or_create_collection(name: collection_name)

    ids = [ "a", "b" ]
    embeddings = [ [ 0.1, 0.2, 0.3 ], [ 0.2, 0.1, 0.0 ] ]
    documents = [ "hello", "world" ]
    metadatas = [ { "topic" => "greeting" }, { "topic" => "farewell" } ]

    collection.add(ids: ids, embeddings: embeddings, documents: documents, metadatas: metadatas)

    get_result = collection.get(ids: [ "a" ], include: [ "documents", "metadatas" ])
    expect(get_result.ids.flatten).to include("a")

    query_result = collection.query(query_embeddings: [ [ 0.1, 0.2, 0.3 ] ], n_results: 1)
    expect(query_result.ids.first).to include("a").or include("b")

    filtered = collection.get(where: { "topic" => "greeting" }, include: [ "documents", "metadatas" ])
    expect(filtered.ids.flatten).to include("a")

    collection.update(ids: [ "a" ], embeddings: [ [ 0.0, 0.0, 0.0 ] ], metadatas: [ { "stage" => "updated" } ])
    collection.upsert(ids: [ "c" ], embeddings: [ [ 0.9, 0.9, 0.9 ] ], documents: [ "new" ])

    collection.delete(ids: [ "b" ])
  end

  it "supports admin database operations" do
    db_name = "ruby_db_#{SecureRandom.hex(4)}"
    @admin.create_database(db_name, tenant: Chroma::DEFAULT_TENANT)

    dbs = @admin.list_databases(tenant: Chroma::DEFAULT_TENANT)
    expect(dbs.map { |db| db["name"] }).to include(db_name)

    @admin.delete_database(db_name, tenant: Chroma::DEFAULT_TENANT)
  end

  it "supports function attach/detach when configured" do
    function_id = ENV.fetch("CHROMA_FUNCTION_ID", nil)
    skip("CHROMA_FUNCTION_ID not set") unless function_id

    base_collection = @client.get_or_create_collection(name: "ruby_fn_base_#{SecureRandom.hex(4)}")
    output_collection_name = "ruby_fn_output_#{SecureRandom.hex(4)}"
    @client.get_or_create_collection(name: output_collection_name)

    attached_name = "ruby_fn_#{SecureRandom.hex(4)}"

    begin
      response = base_collection.attach_function(
        function_id: function_id,
        name: attached_name,
        output_collection: output_collection_name,
      )
      expect(response).to be_a(Hash)

      attached = base_collection.get_attached_function(name: attached_name)
      expect(attached).to be_a(Hash)
    ensure
      begin
        base_collection.detach_function(name: attached_name)
      rescue Chroma::ChromaError
        # best-effort cleanup for servers without function support
      end
      @client.delete_collection(name: output_collection_name)
      @client.delete_collection(name: base_collection.name)
    end
  end
end
