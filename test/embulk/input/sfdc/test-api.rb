require "embulk/input/sfdc/api"

module Embulk
  module Input
    module Sfdc
      class ApiTest < Test::Unit::TestCase
        def setup
          @api = Sfdc::Api.new(login_url)
        end

        def test_initialize
          assert_true(@api.client.is_a?(HTTPClient))
        end

        def test_setup
          any_instance_of(Sfdc::Api) do |klass|
            mock(klass).authentication(config) { "access_token" }
            mock(klass).set_latest_version("access_token") { klass }
          end

          assert_true(Sfdc::Api.setup("login_url", config).instance_of?(Sfdc::Api))
        end

        def test_authentication
          stub(@api).set_latest_version("access_token") { @api }

          mock(@api.client).post("#{login_url}/services/oauth2/token", params, Sfdc::Api::DEFAULT_HEADER) do |res|
            mock(res).body { authentication_response }
          end

          access_token = @api.authentication(config)

          assert_equal("access_token", access_token)
          assert_equal(instance_url, @api.client.base_url)
        end

        def test_set_latest_version
          stub(@api).authentication(config) do
            @api.client.base_url = instance_url
            "access_token"
          end

          mock(@api.client).get("/services/data") do |res|
            mock(res).body do
              [
                {"label"=>"first", "url"=>"/services/data/v1.0", "version"=>"1.0"},
                {"label"=>"second", "url"=>version_url, "version"=>"2.0"}].to_json
            end
          end

          access_token = @api.authentication(config)

          @api.set_latest_version(access_token)
          assert_equal(instance_url + version_url, @api.client.base_url)
        end

        def test_get_metadata
          setup_api_stub

          Sfdc::Api.setup(login_url, config)

          metadata = {"metadata" => "is here"}
          mock(@api.client).get("/sobjects/custom__c/describe", nil, Sfdc::Api::DEFAULT_HEADER) do |res|
            mock(res).body do
              metadata.to_json
            end
          end

          assert_equal(metadata, @api.get_metadata("custom__c"))
        end

        def test_search
          setup_api_stub

          Sfdc::Api.setup(login_url, config)

          hit_object = {"Name" => "object1"}
          objects = [hit_object, {"Name" => "object2"}]
          soql = "SELECT name FROM custom__c WHERE Name='object1'"

          mock(@api.client).get("/query", {q: soql}, Sfdc::Api::DEFAULT_HEADER) do |res|
            mock(res).body { hit_object.to_json }
          end

          assert_equal(hit_object, @api.search(soql))
        end

        private

        def login_url
          "https://login-sfdc.com"
        end

        def config
          {
            client_id: "client_id",
            client_secret: "client_secret",
            username: "username",
            password: "password",
            security_token: "security_token",
          }
        end

        def params
          {
            grant_type: "password",
            client_id: config[:client_id],
            client_secret: config[:client_secret],
            username: config[:username],
            password: config[:password] + config[:security_token]
          }
        end

        def authentication_response
          {
            "instance_url" => instance_url,
            "access_token" => "access_token"
          }.to_json
        end

        def instance_url
          "https://instance-url.com"
        end

        def version_url
          "/services/data/v2.0"
        end

        def setup_api_stub
          stub(Sfdc::Api).setup(login_url, config) do
            @api.client.base_url = URI.join(instance_url, version_url).to_s
            @api.client.default_header = {"Authorization" => "Bearer access_token"}
            @api
          end
        end
      end
    end
  end
end
