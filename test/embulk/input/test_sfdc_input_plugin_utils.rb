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

    sobjects = {
      "records" => records
    }

    expected = [
      {name: "key", type: :string},
      {name: "count", type: :long},
      {name: "editable", type: :boolean},
      {name: "created", type: :timestamp, format: "%Y-%m-%dT%H:%M:%S"}
    ]

    actual = Embulk::Input::SfdcInputPluginUtils.guess_columns(sobjects)
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

  def test_build_soql
    metadata = {
      "fields" => [
        {"name" => "foo"},
        {"name" => "bar"}
      ]
    }
    target = "Foo__c"
    soql = Embulk::Input::SfdcInputPluginUtils.build_soql(target, metadata)
    assert_equal("SELECT foo,bar FROM #{target}", soql)
  end
end
