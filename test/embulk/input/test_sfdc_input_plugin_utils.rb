require "prepare_embulk"
require "embulk/input/sfdc_input_plugin_utils"

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

  def test_extract_records
    json = [
      {
        "attributes" => {
          "type" => "manyo__c",
          "url" => "/services/data/v2.0/sobjects/manyo__c/a0028000002prfUAAQ"
        },
       "Id" => "a0028000002prfUAAQ",
       "Name" => "cat",
       "renban__c" => "201506-1078"
      },
      {
        "attributes" => {
          "type" => "manyo__c",
          "url" => "/services/data/v2.0/sobjects/manyo__c/a0028000002prg8AAA"
        },
        "Id" => "a0028000002prg8AAA",
        "Name" => "cat5",
        "renban__c" => "201506-1079"
      },
    ]

    expected = [
      {
       "Id" => "a0028000002prfUAAQ",
       "Name" => "cat",
       "renban__c" => "201506-1078"
      },
      {
        "Id" => "a0028000002prg8AAA",
        "Name" => "cat5",
        "renban__c" => "201506-1079"
      },
    ]

    actual = Embulk::Input::SfdcInputPluginUtils.extract_records(json)

    assert_equal(expected, actual)
  end

  data do
    {
      "nil" => [nil, [nil, :string]],
      "integer_to_string" => ["123", [123, :string]],
      "integer_to_long" => [123, [123, :long]],
      "integer_to_double" => [123.0, [123, :double]],
      "string_to_timestamp" => [Time.new(2015, 3, 1, 0, 12, 0), ["2015-03-01T00:12:00", :timestamp]],
      "integer_to_boolean" => [true, [123, :boolean]],
    }
  end

  def test_cast(data)
    expected, actual = data
    value, type = actual

    casted_value = Embulk::Input::SfdcInputPluginUtils.cast(value, type)

    assert_equal(expected, casted_value)
  end
end
