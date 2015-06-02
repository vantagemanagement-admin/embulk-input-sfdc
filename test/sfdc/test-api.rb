require "sfdc/api"

class SfdcApiTest < Test::Unit::TestCase
  def test_setup
    config = {}

    any_instance_of(Sfdc::Api) do |klass|
      mock(klass).authentication(config) { "access_token" }
      mock(klass).set_latest_version("access_token") { klass }
    end

    Sfdc::Api.setup("login_url", config)
  end

  def test_initialize
    api = Sfdc::Api.new("login_url")

    assert_true(api.client.is_a?(HTTPClient))
  end
end
