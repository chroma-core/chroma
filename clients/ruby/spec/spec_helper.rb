# frozen_string_literal: true

require "bundler/setup"
require "chromadb"
require "webmock/rspec"

WebMock.disable_net_connect!(allow_localhost: true)

RSpec.configure do |config|
  config.order = :random
end
