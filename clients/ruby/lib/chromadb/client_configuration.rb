# frozen_string_literal: true

module Chroma
  class ClientConfiguration
    attr_accessor :cloud_api_key, :cloud_host, :cloud_port, :cloud_ssl, :cloud_tenant, :cloud_database

    def initialize
      @cloud_api_key = ENV["CHROMA_API_KEY"]
      @cloud_host = ENV.fetch("CHROMA_CLOUD_HOST", "api.trychroma.com")
      @cloud_port = normalize_port(ENV.fetch("CHROMA_CLOUD_PORT", "443"))
      @cloud_ssl = ENV.fetch("CHROMA_CLOUD_SSL", "true").downcase != "false"
      @cloud_tenant = ENV["CHROMA_TENANT"]
      @cloud_database = ENV["CHROMA_DATABASE"]
    end

    private

    def normalize_port(value)
      Integer(value)
    rescue ArgumentError, TypeError
      443
    end
  end

  class << self
    def configuration
      @configuration ||= ClientConfiguration.new
    end

    def configure
      yield(configuration) if block_given?
      if defined?(Chroma::SharedState)
        SharedState.register_cloud_api_key(configuration.cloud_api_key)
      end
      configuration
    end

    def reset_configuration!
      @configuration = ClientConfiguration.new
      if defined?(Chroma::SharedState)
        SharedState.register_cloud_api_key(@configuration.cloud_api_key)
      end
      @configuration
    end
  end
end
