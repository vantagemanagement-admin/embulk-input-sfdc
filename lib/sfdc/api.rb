require "httpclient"
require "uri"

module Sfdc
  class Api
    DEFAULT_HEADER = {:Accept => 'application/json; charset=UTF-8'.freeze}.freeze

    attr_reader :client

    def self.setup(login_url, config)
      api = new(login_url)

      token = api.authentication(config)
      api.set_latest_version(token)
      api
    end

    def initialize(login_url)
      @login_url = login_url
      @client = HTTPClient.new
    end

    def authentication(config)
      params = {
        :grant_type => 'password',
        :client_id => config[:client_id],
        :client_secret => config[:client_secret],
        :username => config[:username],
        :password => config[:password] + config[:security_token]
      }

      oauth_response = @client.post(@login_url + "services/oauth2/token", params, :Accept => 'application/json; charset=UTF-8')
      oauth = JSON.parse(oauth_response.body)

      client.base_url = oauth["instance_url"]

      oauth["access_token"]
    end

    def set_latest_version(access_token)
      versions_response = @client.get("/services/data")
      # Use latest version always
      version_url = JSON.parse(versions_response.body).last["url"]

      client.base_url = URI.join(@client.base_url, "/", version_url).to_s
      client.default_header = { "Authorization" => "Bearer #{access_token}" }

      self
    end

    def get_metadata(sobject_name)
      sobject_metadata = client.get("/sobjects/#{sobject_name}/describe", :Accept => 'application/json; charset=UTF-8')
      JSON.parse(sobject_metadata.body)
    end

    def search(soql)
      JSON.parse(client.get("/query/?q=#{soql}", DEFAULT_HEADER).body)
    end
  end
end
