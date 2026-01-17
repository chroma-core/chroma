# frozen_string_literal: true

require "faraday"
require "uri"

module Chroma
  class HttpTransport
    DEFAULT_API_PATH = "/api/v2"

    attr_reader :base_url, :headers

    def initialize(host:, port: 8000, ssl: false, path: DEFAULT_API_PATH, headers: nil, ssl_verify: true, timeout: nil)
      @base_url = self.class.resolve_url(
        host: host,
        port: port,
        ssl: ssl,
        default_api_path: path,
      )
      @headers = (headers || {}).dup
      @headers["Content-Type"] ||= "application/json"
      @headers["User-Agent"] ||= "Chroma Ruby Client v#{Chroma::VERSION} (https://github.com/chroma-core/chroma)"
      @preflight_checks = nil

      @connection = Faraday.new(url: @base_url, ssl: { verify: ssl_verify }) do |builder|
        if timeout
          builder.options.timeout = timeout
          builder.options.open_timeout = timeout
        end
      end
    end

    def request(method, path, params: nil, json: nil)
      normalized_path = path.start_with?("/") ? path[1..] : path
      response = @connection.run_request(method.to_sym, normalized_path, nil, @headers) do |req|
        req.params.update(clean_params(params)) if params
        req.body = JSON.generate(json) if json
      end

      raise_chroma_error(response) unless response.success?

      parse_response(response.body)
    end

    def get_pre_flight_checks
      @preflight_checks ||= request(:get, "/pre-flight-checks")
    end

    def supports_base64_encoding?
      checks = get_pre_flight_checks
      checks.is_a?(Hash) && checks["supports_base64_encoding"] == true
    end

    def max_batch_size
      checks = get_pre_flight_checks
      return -1 unless checks.is_a?(Hash)
      checks.fetch("max_batch_size", -1)
    end

    def request_headers
      @headers.dup
    end

    def self.resolve_url(host:, port:, ssl:, default_api_path: DEFAULT_API_PATH)
      validate_host(host)

      skip_port = host.start_with?("http://", "https://")
      parsed = URI.parse(host.start_with?("http") ? host : "http://#{host}")

      scheme = if host.start_with?("http")
        parsed.scheme
      else
        ssl ? "https" : "http"
      end

      netloc = parsed.host || parsed.path
      if skip_port && parsed.port
        netloc = "#{netloc}:#{parsed.port}"
      end
      port_value = skip_port ? parsed.port : port
      path = parsed.path
      path = default_api_path if path.nil? || path.empty? || path == netloc

      trimmed_path = path.end_with?("/") ? path.chomp("/") : path
      if default_api_path && !default_api_path.empty? && !trimmed_path.end_with?(default_api_path)
        path = trimmed_path + default_api_path
      else
        path = trimmed_path
      end

      normalized_path = path.start_with?("/") ? path : "/#{path}"
      normalized_path = normalized_path.gsub(%r{//+}, "/")

      port_segment = skip_port ? "" : ":#{port_value}"
      "#{scheme}://#{netloc}#{port_segment}#{normalized_path}"
    end

    def self.validate_host(host)
      parsed = URI.parse(host)
      if host.include?("/") && parsed.scheme.nil?
        raise ArgumentError,
              "Invalid URL. Seems that you are trying to pass URL as a host but without specifying the protocol. Please add http:// or https:// to the host."
      end
      if host.include?("/") && !%w[http https].include?(parsed.scheme)
        raise ArgumentError, "Invalid URL. Unrecognized protocol - #{parsed.scheme}."
      end
    end

    private

    def clean_params(params)
      params.reject { |_k, v| v.nil? }
    end

    def parse_response(body)
      return nil if body.nil? || body.to_s.strip.empty?
      JSON.parse(body)
    rescue JSON::ParserError
      body
    end

    def raise_chroma_error(response)
      body = response.body
      trace_id = response.headers["chroma-trace-id"]

      begin
        data = JSON.parse(body)
        if data.is_a?(Hash) && data["error"]
          error_class = Chroma::ERROR_TYPES[data["error"]] || Chroma::ChromaError
          error = error_class.new(data["message"])
          error.trace_id = trace_id if error.respond_to?(:trace_id=)
          raise error
        end
      rescue JSON::ParserError
        # fall through
      end

      message = body.to_s
      message = "#{message} (trace ID: #{trace_id})" if trace_id
      raise Chroma::ChromaError, message
    end
  end
end
