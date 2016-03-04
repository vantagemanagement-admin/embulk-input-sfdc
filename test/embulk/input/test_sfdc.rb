require "prepare_embulk"
require "output_capture"
require "override_assert_raise"
require "embulk/input/sfdc"
require "embulk/data_source"

module Embulk
  module Input
    class SfdcInputPluginTest < Test::Unit::TestCase
      include OutputCapture
      include OverrideAssertRaise

      class ScheduledExecutionTest < self
        setup do
          @api = SfdcApi::Api.new
          any_instance_of(SfdcApi::Api) do |klass|
            stub(klass).setup { @api }
          end
          @page_builder = Object.new
          stub(Embulk.logger).debug {}
        end

        def test_without_last_fetched
          t = task.dup
          t.delete(:continue_from)
          @plugin = Sfdc.new(t, nil, nil, @page_builder)

          stub(@api).search { response }
          stub(@plugin).add_next_records {}
          stub(@page_builder).finish
          mock(@page_builder).add(anything).times(response["records"].length)
          mock(Embulk.logger).warn.never

          @plugin.run
        end

        def test_with_last_fetched
          t = task.merge(continue_from: response["records"][1]["LastModifiedDate"])
          @plugin = Sfdc.new(t, nil, nil, @page_builder)

          stub(@api).search { response }
          stub(@plugin).add_next_records {}
          stub(@page_builder).finish

          # NOTE: record 1,2 will be skipped, only add 3
          mock(Embulk.logger).warn(/Skipped/).times(2)
          mock(@page_builder).add(anything).times(1)

          @plugin.run
        end

        def test_without_last_modified_date_record
          t = task.merge(continue_from: response["records"][1]["LastModifiedDate"])
          @plugin = Sfdc.new(t, nil, nil, @page_builder)

          records = response.dup
          records["records"].each{|r| r.delete("LastModifiedDate") } 
          stub(@api).search { records }
          stub(@plugin).add_next_records {}
          stub(@page_builder).finish

          # NOTE: doesn't check LastModifiedDate
          mock(@page_builder).add(anything).times(3)

          @plugin.run
        end

        def response
          {
            "records" => [
              {
                "Id" => "1",
                "LastModifiedDate" => "2000-01-01 00:00:00+09:00"
              },
              {
                "Id" => "2",
                "LastModifiedDate" => "2000-01-02 00:00:00+09:00"
              },
              {
                "Id" => "3",
                "LastModifiedDate" => "2000-01-03 00:00:00+09:00"
              },
            ]
          }
        end
      end

      class RunTest < self
        setup :setup_plugin

        def test_run_through
          stub(@plugin).preview? { false }

          mock(@api).search(task["soql"]) { sfdc_response }
          mock(@plugin).add_records(sfdc_response["records"])
          mock(@plugin).add_next_records(sfdc_response, 1)
          mock(@page_builder).finish
          silence { @plugin.run }
        end

        def test_preview_through
          stub(@plugin).preview? { true }

          mock(@api).search(task["soql"] + " LIMIT #{Sfdc::PREVIEW_RECORDS_COUNT}") { sfdc_response }
          mock(@plugin).add_records(sfdc_response["records"])
          mock(@plugin).add_next_records(sfdc_response, 1)
          mock(@page_builder).finish
          silence { @plugin.run }
        end

        class TestAddRecords < self
          def test_page_builder_add_with_formatted_record
            formatted_records.each do |values|
              mock(@page_builder).add(values)
            end

            stub(@api).search(task["soql"]) { sfdc_response }
            stub(@plugin).add_next_records(sfdc_response, 1)
            stub(@page_builder).finish
            silence { @plugin.run }
          end

          def test_page_builder_add_called_records_count_times
            mock(@page_builder).add(anything).times(sfdc_response["records"].length)

            stub(@api).search(task["soql"]) { sfdc_response }
            stub(@plugin).add_next_records(sfdc_response, 1)
            stub(@page_builder).finish
            silence { @plugin.run }
          end

          def test_invalid_date
            invalid_schema_task = {
              "login_url" => login_url,
              "config" => config,
              "soql" => soql,
              "schema" => [
                {"name" => "Id", "type" => "string"},
                {"name" => "IsDeleted", "type" => "boolean"},
                {"name" => "Name", "type" => "string"},
                {"name" => "CreatedDate", "type" => "timestamp", "format" => "%Y-%m-%dT%H:%M:%S.%L%z"},
                {"name" => "InvalidTimestamp", "type" => "timestamp", "format" => "%Y-%m-%dT%H:%M:%S.%L%z"},
              ]
            }

            @plugin = Sfdc.new(invalid_schema_task, nil, nil, @page_builder)
            stub(Embulk).logger { ::Logger.new(File::NULL) }

            response = {
              "records" => [
                {
                  "Id" => "a00280000010prfUAAQ",
                  "IsDeleted" => false,
                  "Name" => "owl10",
                  "CreatedDate" => "2015-06-03T05:42:02.000+0000",
                  "InvalidTimestamp" => {"hash" => "NO TIME"},
                }
              ]
            }

            stub(@api).search(invalid_schema_task["soql"]) { response }

            assert_raise(Embulk::ConfigError) do
              @plugin.run
            end
          end
        end

        # following tests direct call `add_next_records` method, don't test via `run` method.
        # because `mock(@plugin).add_next_records` completely replace that method implementation
        # so can't mock/stub `add_next_records` for recursive call testing
        class TestAddNextRecords < self
          setup :setup_plugin

          def test_no_next
            actual = @plugin.send(:add_next_records, {"done" => true}, 1)
            assert_nil(actual)
          end

          class TestAddNextRecordsHasNext < self
            def test_api_get_called
              mock(@api).get(first_response["nextRecordsUrl"]) { second_response }
              stub(@plugin).add_records(second_response["records"])

              @plugin.send(:add_next_records, first_response, 1)
            end

            def test_add_records_with_second_response
              stub(@api).get(first_response["nextRecordsUrl"]) { second_response }
              mock(@plugin).add_records(second_response["records"])

              @plugin.send(:add_next_records, first_response, 1)
            end
          end

          private

          def first_response
            {
              "done" => false,
              "nextRecordsUrl" => "http://dummy.example.com/next",
            }
          end

          def second_response
            {
              "done" => true,
              "records" => ["hi"],
            }
          end
        end

        private

        def sfdc_response
          {
            "records" => records_with_attributes
          }
        end
      end

      def test_transaction
        control = proc {} # dummy
        task = {
          login_url: config["login_url"],
          soql: config["soql"],
          config: Sfdc.embulk_config_to_hash(embulk_config),
          schema: config["columns"],
          retry_limit: 5,
          retry_initial_wait_sec: 1,
          continue_from: nil,
          incremental: true,
        }
        columns = task[:schema].map do |col|
          Column.new(nil, col["name"], col["type"].to_sym, col["format"])
        end

        mock(Sfdc).resume(task, columns, 1, &control)
        Sfdc.transaction(embulk_config, &control)
      end

      def test_resume_task_reports
        task_reports = [1, 2, 3]
        control = proc { task_reports }

        assert_equal task_reports.first, Sfdc.resume({dummy: :task}, {dummy: :columns}, 1, &control)
      end

      class GuessTest < self
        def setup
          super
          @api = SfdcApi::Api.new
          @config = embulk_config
          any_instance_of(SfdcApi::Api) do |klass|
            stub(klass).setup(login_url, Sfdc.embulk_config_to_hash(@config)) { @api }
          end
        end

        def test_guess
          mock(@api).get_metadata(@config.param("target", :string)) { metadata }
          soql = SfdcInputPluginUtils.build_soql(config["target"], metadata)
          mock(@api).search("#{soql} LIMIT #{Sfdc::GUESS_RECORDS_COUNT}") { sobjects }

          result = Sfdc.guess(@config)
          assert_equal(soql, result["soql"])
          assert_equal(SfdcInputPluginUtils.guess_columns(sobjects), result["columns"])
        end

        def test_guess_unsearchable_target
          mock(@api).get_metadata(@config.param("target", :string)) { metadata.reject{|k,v| k == "queryable"} }

          assert_raise do
            Sfdc.guess(@config)
          end
        end

        private

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


      data do
        {
          "queryable and searchable" => [true, {"queryable" => true, "searchable" => true}],
          "queryable" => [true, {"queryable" => true}],
          "searchable" => [false, {"searchable" => true}],
          "none" => [false, {}],
        }
      end
      def test_searchable_target?(data)
        expected, actual = data
        assert_equal(expected, Sfdc.searchable_target?(actual))
      end

      def test_embulk_config_to_hash
        base_hash = {
          "client_id" => "client_id",
          "client_secret" => "client_secret",
          "username" => "username",
          "password" => "passowrd",
          "security_token" => "security_token",
        }
        embulk_config = DataSource[*base_hash.to_a.flatten]
        actual = Sfdc.embulk_config_to_hash(embulk_config)
        expect = base_hash.inject({}) do |result, (k,v)|
          result[k.to_sym] = v # key is Symbol, not String
          result
        end
        assert_equal(expect, actual)
      end

      private

      def setup_plugin
        @api = SfdcApi::Api.new
        any_instance_of(SfdcApi::Api) do |klass|
          stub(klass).setup { @api }
        end
        @page_builder = Object.new
        @plugin = Sfdc.new(task, nil, nil, @page_builder)
        stub(Embulk).logger { ::Logger.new(File::NULL) }
      end

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
        "https://login-sfdc.example.com"
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
        DataSource[*config.to_a.flatten(1)]
      end

      def soql
        "SELECT Id, IsDeleted, Name, CreatedDate FROM manyo__c"
      end

      def instance_url
        "https://instance-url.example.com"
      end

      def version_path
        Pathname.new("/services/data/v2.0")
      end

      def next_records_url
        "https://next-records-hoge.example.com"
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

      def formatted_records
        SfdcInputPluginUtils.extract_records(records_with_attributes).map do |record|
          task["schema"].collect do |column|
            if column["name"] == "CreatedDate"
              Time.parse(record["CreatedDate"])
            else
              record[column["name"]]
            end
          end
        end
      end
    end
  end
end
