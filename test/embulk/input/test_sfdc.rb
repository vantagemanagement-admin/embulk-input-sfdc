require "prepare_embulk"
require "embulk/input/sfdc"
require "embulk/data_source"

module Embulk
  module Input
    class SfdcInputPluginTest < Test::Unit::TestCase
      def test_run
        any_instance_of(Sfdc::Api) do |klass|
          mock(klass).setup(login_url, config) do
            api = Sfdc::Api.new
            api.client.base_url = instance_url
            api.instance_variable_set(:@version_path, version_path)
            api.client.default_header = {Accept: 'application/json; charset=UTF-8', Authorization: "Bearer access_token"}
            api
          end

          mock(klass).search(soql) do
            {
              "totalSize" => 5,
              "done" => false,
              "nextRecordsUrl" => next_records_url,
              "records" => records_with_attributes[0..3],
            }
          end

          mock(klass).get(next_records_url) do
            {
              "totalSize" => 5,
              "done" => true,
              "records" => records_with_attributes[4..5],
            }
          end
        end

        page_builder = Object.new # add mock later
        casted_records.each do |record|
          mock(page_builder).add(record.values)
        end
        mock(page_builder).finish()

        next_commit_diff = Embulk::Input::SfdcInputPlugin.new(task, nil, nil, page_builder).run

        assert_equal({}, next_commit_diff)
      end

      def test_transaction
        control = proc {} # dummy
        task = {
          login_url: config["login_url"],
          soql: config["soql"],
          config: Embulk::Input::SfdcInputPlugin.embulk_config_to_hash(embulk_config),
          schema: config["columns"],
        }
        columns = task[:schema].map do |col|
          Embulk::Column.new(nil, col["name"], col["type"].to_sym, col["format"])
        end

        mock(Embulk::Input::SfdcInputPlugin).resume(task, columns, 1, &control)
        Embulk::Input::SfdcInputPlugin.transaction(embulk_config, &control)
      end

      def test_resume
        called = false
        control = proc { called = true}

        Embulk::Input::SfdcInputPlugin.resume({dummy: :task}, {dummy: :columns}, 1, &control)
        assert_true(called)
      end

      class GuessTest < self
        def setup
          super
          @api = api
          @config = embulk_config
          any_instance_of(Sfdc::Api) do |klass|
            stub(klass).setup(login_url, Embulk::Input::SfdcInputPlugin.embulk_config_to_hash(@config)) { @api }
          end
        end

        def test_guess
          mock(@api).get_metadata(@config.param("target", :string)) { metadata }
          soql = SfdcInputPluginUtils.build_soql(metadata, config["target"])
          mock(@api).search("#{soql} LIMIT 5") { sobjects }

          result = Embulk::Input::SfdcInputPlugin.guess(@config)
          assert_equal(soql, result["soql"])
          assert_equal(SfdcInputPluginUtils.guess_columns(sobjects), result["columns"])
        end

        def test_guess_unsearchable_target
          mock(@api).get_metadata(@config.param("target", :string)) { metadata.reject{|k,v| k == "queryable"} }

          assert_raise do
            Embulk::Input::SfdcInputPlugin.guess(@config)
          end
        end

        private

        def api
          Sfdc::Api.new
        end

        def metadata
          {
            "fields" => [
              {"name" => "foo"},
              {"name" => "bar"},
            ],
            "queryable" => "1",
            "searchable" => "1",
          }
        end

        def sobjects
          {
            "records" => [
              {
                "title" => "foobar",
                "id" => "id1",
              },
              {
                "title" => "hoge",
                "id" => "id2",
              }
            ]
          }
        end
      end

      def test_searchable_target?
        assert_true Embulk::Input::SfdcInputPlugin.searchable_target?({"queryable" => true, "searchable" => true})
        assert_false Embulk::Input::SfdcInputPlugin.searchable_target?({})
        assert_false Embulk::Input::SfdcInputPlugin.searchable_target?({"searchable" => true})
        assert_false Embulk::Input::SfdcInputPlugin.searchable_target?({"queryable" => true})
      end

      def test_embulk_config_to_hash
        base_hash = {
          "client_id" => "client_id",
          "client_secret" => "client_secret",
          "username" => "username",
          "password" => "passowrd",
          "security_token" => "security_token",
        }
        embulk_config = Embulk::DataSource[*base_hash.to_a.flatten]
        actual = Embulk::Input::SfdcInputPlugin.embulk_config_to_hash(embulk_config)
        expect = base_hash.inject({}) do |result, (k,v)|
          result[k.to_sym] = v # key is Symbol, not String
          result
        end
        assert_equal(expect, actual)
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
          "login_url" => login_url,
          "target" => "dummy",
          "soql" => "SELECT 1",
          "columns" => [
            {"name" => "foo", "type" => "string"},
            {"name" => "bar", "type" => "integer"},
          ]
        }
      end

      def embulk_config
        Embulk::DataSource[*config.to_a.flatten(1)]
      end

      def soql
        "SELECT Id, IsDeleted, Name, CreatedDate from manyo__c"
      end

      def instance_url
        "https://instance-url.com"
      end

      def version_path
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
              "url" => "#{version_path.to_s}/sobjects/manyo__c/a002800000#{i}prfUAAQ"
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
