require "httpclient"
require "pathname"

module Embulk
  module Input
    module Sfdc
      class ApiError < StandardError; end
      class InternalServerError < StandardError; end

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
          response = client.get(path, parameters)
          body = JSON.parse(response.body)

          handle_error(body, response.status_code)
          body
        end

        def get_metadata(sobject_name)
          get(@version_path.join("sobjects/#{sobject_name}/describe").to_s)
        end

        def search(soql)
          get(@version_path.join("query").to_s, {q: soql})
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
          handle_error(oauth, oauth_response.status_code)

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

        def handle_error(body, status_code)
          # NOTE: Sometimes (on error) body is Array
          body = body.first if body.is_a? Array

          case status_code
          when 400..499
            message = "StatusCode: #{status_code}"

            message << ": #{body['errorCode']}" if body["errorCode"]
            message << ": #{body['message']}" if body["message"]
            raise Sfdc::ApiError, message
          when 500..599
            raise Sfdc::InternalServerError, "Force.com REST API returns 500 (Internal Server Error). Please contact customer support of Force.com."
          end
        end
      end
    end
  end
end
