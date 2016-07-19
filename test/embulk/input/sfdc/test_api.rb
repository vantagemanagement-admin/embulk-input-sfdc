require "override_assert_raise"
require "embulk/input/sfdc_api/api"

module Embulk
  module Input
    module SfdcApi
      class ApiTest < Test::Unit::TestCase
        include OverrideAssertRaise

        def setup
          @api = SfdcApi::Api.new
        end

        def test_initialize_client
          assert_true(@api.client.is_a?(HTTPClient))
        end

        def test_initialize_default_header
          assert_equal({Accept: 'application/json; charset=UTF-8'}, @api.client.default_header)
        end

        class SetupTest < self
          def test_returning_value
            mock(@api).authentication(login_url, config) { "access_token" }
            mock(@api).set_latest_version("access_token") { @api }

            assert_true(@api.setup(login_url, config).instance_of?(SfdcApi::Api))
          end

          def test_instance_url
            mock(@api.client).post("#{login_url}/services/oauth2/token", params) do |res|
              mock(res).status_code { 200 }
              mock(res).body { authentication_response }
            end
            mock(@api).set_latest_version("access_token") { @api }

            @api.setup(login_url, config)

            assert_equal(instance_url, @api.client.base_url)
          end

          def test_version_path
            stub(@api).authentication(login_url, config) do
              @api.client.base_url = instance_url
              "access_token"
            end

            mock(@api.client).get("/services/data") do |res|
              mock(res).body do
                [
                  {"label"=>"first", "url"=>"/services/data/v1.0", "version"=>"1.0"},
                  {"label"=>"second", "url"=>version_path, "version"=>"2.0"}].to_json
              end
            end

            @api.setup(login_url, config)

            assert_equal(version_path, @api.instance_variable_get(:@version_path))
          end
        end

        def test_get_metadata
          setup_api_stub

          @api.setup(login_url, config)

          metadata = {"metadata" => "is here"}
          mock(@api).get(version_path.join("sobjects/custom__c/describe").to_s) do
            metadata
          end

          assert_equal(metadata, @api.get_metadata("custom__c"))
        end

        def test_search
          setup_api_stub

          @api.setup(login_url, config)

          hit_object = {"Name" => "object1"}
          objects = [hit_object, {"Name" => "object2"}]
          soql = "SELECT name FROM custom__c WHERE Name='object1'"

          mock(@api).get(version_path.join("query").to_s, {q: soql}) do
            hit_object
          end

          assert_equal(hit_object, @api.search(soql))
        end

        class GetTest < self
          def setup
            super
          end

          def test_success
            result = {"statusCode" => "OK"}
            path = "success"
            mock(@api.client).get(path, {}) do |res|
              mock(res).status_code { 200 }
              mock(res).body { result.to_json }
            end

            assert_equal(result, @api.get(path))
          end

          def test_success_with_parameters
            result = {"statusCode" => "OK"}
            path = "success"
            parameters = {"parameter" => "is OK"}

            mock(@api.client).get(path, parameters) do |res|
              mock(res).status_code { 200 }
              mock(res).body { result.to_json }
            end

            assert_equal(result, @api.get(path, parameters))
          end

          data do
            [
              ["is json response", {"errorCode" => "NotFound", "message" => "error"}.to_json],
              ["is not json response", "<!doctype> this is not json"]
            ]
          end
          def test_failure_with_404(response_body)
            path = "failure"
            parameters = {"parameter" => "is not OK"}

            mock(@api.client).get(path, parameters) do |res|
              mock(res).status_code { 404 }
              stub(res).body { response_body }
            end

            assert_raise(Embulk::ConfigError) do
              @api.get(path, parameters)
            end
          end

          data do
            [
              [
                "INVALID_QUERY_LOCATOR",
                {
                  "errorCode" => "INVALID_QUERY_LOCATOR",
                  "message" => "Unable to find query cursor data for this QueryLocator (org xxxxxxxxxxxxxxxxxx, path c/xxxxxxxxxxxxxxxxxx/3/foo/100000.cursor), please retry your query.",
                }.to_json
              ],
              [
                "QUERY_TIMEOUT",
                {
                  "errorCode" => "QUERY_TIMEOUT",
                  "message" => "Your query request was running for too long.",
                }.to_json
              ],
            ]
          end
          def test_failure_with_retriable_4xx(response_body)
            path = "failure"
            parameters = {"parameter" => "is not OK"}
            response = JSON.parse(response_body)

            mock(@api.client).get(path, parameters) do |res|
              mock(res).status_code { 400 }
              stub(res).body { response_body }
            end

            begin
              @api.get(path, parameters)
              assert false
            rescue => e
              assert_equal RuntimeError, e.class
              assert_match response["errorCode"], e.message
              assert_match response["message"], e.message
            end
          end

          data do
            [
              ["is json response", {"errorCode" => "InternalError", "message" => "error"}.to_json],
              ["is not json response", "<!doctype> this is not json"]
            ]
          end
          def test_failure_with_500(response_body)
            path = "failure"
            parameters = {"parameter" => "is not OK"}

            mock(@api.client).get(path, parameters) do |res|
              mock(res).status_code { 500 }
              stub(res).body { response_body }
            end

            assert_raise(SfdcApi::InternalServerError) do
              @api.get(path, parameters)
            end
          end
        end

        private

        def login_url
          "https://login-sfdc.example.com"
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
          "https://instance-url.example.com"
        end

        def version_path
          Pathname.new("/services/data/v2.0")
        end

        def setup_api_stub
          stub(@api).setup(login_url, config) do
            @api.client.base_url = instance_url
            @api.instance_variable_set(:@version_path, version_path)
            @api.client.default_header = {Accept: 'application/json; charset=UTF-8', Authorization: "Bearer access_token"}
            @api
          end
        end
      end
    end
  end
end
