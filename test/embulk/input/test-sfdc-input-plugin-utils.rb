require "prepare-embulk"
require "embulk/input/sfdc-input-plugin-utils"

class EmbulkInputPluginUtilsTest < Test::Unit::TestCase
  def test_guess_columns
    records = [
      {
        "key" => "FOO",
        "count" => 3,
        "editable" => true,
        "created" => "2015-03-01T00:12:00"
      }
    ]

    expected = [
      {name: "key", type: :string},
      {name: "count", type: :long},
      {name: "editable", type: :boolean},
      {name: "created", type: :timestamp, format: "%Y-%m-%dT%H:%M:%S"}
    ]

    actual = Embulk::Input::SfdcInputPluginUtils.guess_columns(records)
    assert_equal(expected, actual)
  end
end
