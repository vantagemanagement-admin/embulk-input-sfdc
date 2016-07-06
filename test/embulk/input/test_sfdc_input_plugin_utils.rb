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
       "renban__c" => "201506-1078",
       "poetry__r" => {
         "attributes" => {
           "type" => "poetry__c",
           "url"  => "/services/data/v2.0/sobjects/poetry__c/b0028000002prfUAAQ"
         },
         "Id" => "b0028000002prfUAAQ",
         "Name" => "dog",
         "kajin__r" => {
           "attributes" => {
             "type" => "kajin__c",
             "url"  => "/services/data/v2.0/sobjects/kajin__c/d0028000002prfUAAQ"
           },
           "Id" => "d0028000002prfUAAQ",
           "Name" => "semimaru",
         }
       },
       "waka__c" => {
         "totalSize" => 1,
         "done" => true,
         "records" => [
           {
             "attributes" => {
               "type" => "waka__c",
               "url"  => "/services/data/v2.0/sobjects/waka__c/c0028000002prfUAAQ"
             },
             "Id" => "c0028000002prfUAAQ",
             "Name" => "pony"
           }
         ]
       }
      },
      {
        "attributes" => {
          "type" => "manyo__c",
          "url" => "/services/data/v2.0/sobjects/manyo__c/a0028000002prg8AAA"
        },
        "Id" => "a0028000002prg8AAA",
        "Name" => "cat5",
        "renban__c" => "201506-1079",
        "poetry__r" => {
          "attributes" => {
            "type" => "poetry__c",
            "url"  => "/services/data/v2.0/sobjects/poetry__c/b0028000002prg8AAA"
          },
          "Id" => "b0028000002prg8AAA",
          "Name" => "dog5",
          "kajin__r" => {
           "attributes" => {
             "type" => "kajin__c",
             "url"  => "/services/data/v2.0/sobjects/kajin__c/d0028000002prg8AAA"
           },
           "Id" => "d0028000002prg8AAA",
           "Name" => "semimaru5",
         }
        },
        "waka__c" => {
          "totalSize" => 1,
          "done" => true,
          "records" => [
            {
              "attributes" => {
                "type" => "waka__c",
                "url"  => "/services/data/v2.0/sobjects/waka__c/c0028000002prg8AAA"
              },
              "Id" => "c0028000002prg8AAA",
              "Name" => "pony5"
            }
          ]
        }
      }
    ]

    expected = [
      {
       "Id" => "a0028000002prfUAAQ",
       "Name" => "cat",
       "renban__c" => "201506-1078",
       "poetry__r.Id" => "b0028000002prfUAAQ",
       "poetry__r.Name" => "dog",
       "poetry__r.kajin__r.Id" => "d0028000002prfUAAQ",
       "poetry__r.kajin__r.Name" => "semimaru",
       "waka__c" => {
         "totalSize" => 1,
         "done" => true,
         "records" => [
           {
             "attributes" => {
               "type" => "waka__c",
               "url"  => "/services/data/v2.0/sobjects/waka__c/c0028000002prfUAAQ"
             },
             "Id" => "c0028000002prfUAAQ",
             "Name" => "pony"
           }
         ]
       }
      },
      {
        "Id" => "a0028000002prg8AAA",
        "Name" => "cat5",
        "renban__c" => "201506-1079",
        "poetry__r.Id" => "b0028000002prg8AAA",
        "poetry__r.Name" => "dog5",
        "poetry__r.kajin__r.Id" => "d0028000002prg8AAA",
        "poetry__r.kajin__r.Name" => "semimaru5",
        "waka__c" => {
          "totalSize" => 1,
          "done" => true,
          "records" => [
            {
              "attributes" => {
                "type" => "waka__c",
                "url"  => "/services/data/v2.0/sobjects/waka__c/c0028000002prg8AAA"
              },
              "Id" => "c0028000002prg8AAA",
              "Name" => "pony5"
            }
          ]
        }
      }
    ]

    actual = Embulk::Input::SfdcInputPluginUtils.extract_records(json)

    assert_equal(expected, actual)
  end

  def test_extract_parent_elements
    key = "poetry__r"

    elements = {
      "attributes" => {
        "type" => "poetry__c",
        "url"  => "/services/data/v2.0/sobjects/poetry__c/b0028000002prg8AAA"
      },
      "Id" => "b0028000002prg8AAA",
      "Name" => "dog5",
      "kajin__r" => {
        "attributes" => {
          "type" => "kajin__c",
          "url"  => "/services/data/v2.0/sobjects/kajin__c/d0028000002prg8AAA"
        },
        "Id" => "d0028000002prg8AAA",
        "Name" => "semimaru5"
      }
    }

    expected = {
      "poetry__r.Id" => "b0028000002prg8AAA",
      "poetry__r.Name" => "dog5",
      "poetry__r.kajin__r.Id" => "d0028000002prg8AAA",
      "poetry__r.kajin__r.Name" => "semimaru5"
    }

    actual = Embulk::Input::SfdcInputPluginUtils.extract_parent_elements(key, elements)

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
