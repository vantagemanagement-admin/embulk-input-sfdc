require "httpclient"
require "pathname"

module Embulk
  module Input
    module Sfdc
      class Api
        DEFAULT_LOGIN_URL = "https://login.salesforce.com".freeze

        attr_reader :client

        def setup(login_url, config)
          token = authentication(login_url, config)
          set_latest_version(token)
          self
        end

        def initialize
          @version_path = ""
          @client = HTTPClient.new
          @client.default_header = {Accept: 'application/json; charset=UTF-8'}
        end

        def get(path, parameters={})
          # TODO: Use this method by #get_metadata and #search
          # TODO: error handling
          JSON.parse(client.get(path, parameters).body)
        end

        def get_metadata(sobject_name)
          sobject_metadata = client.get(@version_path.join("sobjects/#{sobject_name}/describe").to_s)
          JSON.parse(sobject_metadata.body)
        end

        def search(soql)
          JSON.parse(client.get(@version_path.join("query").to_s, {q: soql}).body)
        end

        private

        def authentication(login_url, _config)
          # NOTE: At SfdcInputPlugin#init, we use Symbol as each key
          #       for task (Hash), but at SfdcInputPlugin#run, task
          #       has them as String...:(
          #       So, I translate keys from String to Symbol
          config = {}
          _config.each { |key, value| config[key.to_sym] = value }

          params = {
            grant_type: 'password',
            client_id: config[:client_id],
            client_secret: config[:client_secret],
            username: config[:username],
            password: config[:password] + config[:security_token]
          }

          oauth_response = @client.post(URI.join(login_url, "services/oauth2/token").to_s, params)
          oauth = JSON.parse(oauth_response.body)

          client.base_url = oauth["instance_url"]

          oauth["access_token"]
        end

        def set_latest_version(access_token)
          versions_response = @client.get("/services/data")
          # Use latest version always
          @version_path = Pathname.new(JSON.parse(versions_response.body).last["url"])

          client.default_header = client.default_header.merge(Authorization: "Bearer #{access_token}")

          self
        end
      end
    end
  end
end
