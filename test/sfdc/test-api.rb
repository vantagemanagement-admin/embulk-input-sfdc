require "sfdc/api"

class SfdcApiTest < Test::Unit::TestCase
  def test_initialize
    api = Sfdc::Api.new("login_url")

    assert_true(api.client.is_a?(HTTPClient))
  end
end
