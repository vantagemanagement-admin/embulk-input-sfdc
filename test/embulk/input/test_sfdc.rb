require "prepare_embulk"
require "embulk/input/sfdc"

module Embulk
  module Input
    class SfdcInputPluginTest < Test::Unit::TestCase
      def test_run
        @api = Sfdc::Api.new(login_url)
        mock(Sfdc::Api).setup(login_url, config) do
          @api.client.base_url = instance_url
          @api.instance_variable_set(:@version_url, version_url)
          @api.client.default_header = {Accept: 'application/json; charset=UTF-8', Authorization: "Bearer access_token"}
          @api
        end

        mock(@api).search(soql) do
          {
            "totalSize" => 5,
            "done" => false,
            "nextRecordsUrl" => next_records_url,
            "records" => records_with_attributes[0..3],
          }
        end

        mock(@api).get(next_records_url) do
          {
            "totalSize" => 5,
            "done" => true,
            "records" => records_with_attributes[4..5],
          }
        end

        page_builder = Object.new # add mock later
        casted_records.each do |record|
          mock(page_builder).add(record.values)
        end
        mock(page_builder).finish()

        next_commit_diff = Embulk::Input::SfdcInputPlugin.new(task, nil, nil, page_builder).run

        assert_equal({}, next_commit_diff)
      end

      private

      def task
        {
          "login_url" => login_url,
          "config" => config,
          "soql" => soql,
          "schema" => [
            {"name" => "Id", "type" => "string"},
            {"name" => "IsDeleted", "type" => "boolean"},
            {"name" => "Name", "type" => "string"},
            {"name" => "CreatedDate", "type" => "timestamp", "format" => "%Y-%m-%dT%H:%M:%S.%L%z"},
          ]
        }
      end

      def login_url
        "https://login-sfdc.com"
      end

      def config
        {
          "client_id" => "client_id",
          "client_secret" => "client_secret",
          "username" => "username",
          "password" => "passowrd",
          "security_token" => "security_token",
        }
      end

      def soql
        "SELECT Id, IsDeleted, Name, CreatedDate from manyo__c"
      end

      def instance_url
        "https://instance-url.com"
      end

      def version_url
        Pathname.new("/services/data/v2.0")
      end

      def next_records_url
        "https://next-records-hoge.com"
      end

      def records_with_attributes
        records.map.with_index do |records, i|
          {
            "attributes" => {
              "type" => "manyo__c",
              "url" => "#{version_url.to_s}/sobjects/manyo__c/a002800000#{i}prfUAAQ"
            }
          }.merge(records)
        end
      end

      def records
        @records ||= (0..5).map do |i|
          {
           "Id" => "a002800000#{i}prfUAAQ",
           "IsDeleted" => false,
           "Name" => "owl#{i}",
           "CreatedDate" => "2015-06-03T05:42:02.000+0000",
          }
        end
      end

      def casted_records
        records.map do |record|
          casted_record = {}
          record.map do |(key, value)|
            # NOTE: records includs not String value("CreatedDate") so
            #       it should be casted, but other values will be included,
            #       you should use 'case' sentence.
            value = Time.parse(value) if key == "CreatedDate"
            casted_record[key] = value
          end

          casted_record
        end
      end
    end
  end
end
