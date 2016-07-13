require "perfect_retry"
require "embulk/input/sfdc_api/api"
require "embulk/input/sfdc_input_plugin_utils"

module Embulk
  module Input

    class Sfdc < InputPlugin
      Plugin.register_input("sfdc", self)

      GUESS_RECORDS_COUNT = 30
      PREVIEW_RECORDS_COUNT = 15
      MAX_FETCHABLE_COUNT = 2000

      def self.transaction(config, &control)
        task = {}

        task[:login_url] = config.param("login_url", :string, default: SfdcApi::Api::DEFAULT_LOGIN_URL)
        task[:soql] = config.param("soql", :string)

        task[:config] = embulk_config_to_hash(config)
        task[:retry_limit] = config.param("retry_limit", :integer, default: 5)
        task[:retry_initial_wait_sec] = config.param("retry_initial_wait_sec", :integer, default: 1)
        task[:continue_from] = config.param("continue_from", :string, default: nil)

        task[:schema] = config.param("columns", :array)
        task[:incremental] = config.param("incremental", :bool, default: true)
        columns = []

        task[:schema].each do |column|
          name = column["name"]
          type = column["type"].to_sym

          columns << Column.new(nil, name, type, column["format"])
        end

        resume(task, columns, 1, &control)
      end

      def self.resume(task, columns, count, &control)
        task_reports = yield(task, columns, count)

        return task_reports.first
      end

      def self.guess(config)
        login_url = config.param("login_url", :string, default: SfdcApi::Api::DEFAULT_LOGIN_URL)
        target = config.param("target", :string)

        api = SfdcApi::Api.new.setup(login_url, embulk_config_to_hash(config))

        metadata = api.get_metadata(target)
        raise "Target #{target} can't be searched." unless searchable_target?(metadata)

        soql = SfdcInputPluginUtils.build_soql(target, metadata)
        sobjects = api.search("#{soql} LIMIT #{GUESS_RECORDS_COUNT}")

        {
          "soql" => soql,
          "columns" => SfdcInputPluginUtils.guess_columns(sobjects)
        }
      end

      def init
        @api = SfdcApi::Api.new.setup(task["login_url"], task["config"])
        @schema = task["schema"]
        @soql = task["soql"]
        @retryer = PerfectRetry.new do |config|
          config.limit = task[:retry_limit]
          config.sleep = proc{|n| task[:retry_initial_wait_sec] ** n}
          config.dont_rescues = [Embulk::ConfigError]
          config.logger = Embulk.logger
          config.log_level = nil
        end
      end

      def run
        @soql += " LIMIT #{PREVIEW_RECORDS_COUNT}" if preview?
        if task[:continue_from]
          @continue_from = @latest_updated = Time.parse(task[:continue_from])
        end

        response = @retryer.with_retry do
          @api.search(@soql)
        end
        Embulk.logger.info "Start to add records...(total #{response["totalSize"]} records)"
        add_records(response["records"])

        add_next_records(response, 1) unless preview?

        page_builder.finish

        Embulk.logger.debug "Added all records."

        return {} unless task[:incremental]

        task_report = {
          continue_from: @latest_updated.to_s
        }
        return task_report
      end

      private

      def preview?
        begin
          org.embulk.spi.Exec.isPreview()
        rescue java.lang.NullPointerException => e
          false
        end
      end

      def self.embulk_config_to_hash(config)
        {
          client_id: config.param("client_id", :string),
          client_secret: config.param("client_secret", :string),
          username: config.param("username", :string),
          password: config.param("password", :string),
          security_token: config.param("security_token", :string),
        }
      end

      def self.searchable_target?(metadata)
        !!metadata["queryable"]
      end

      def add_next_records(response, fetch_count)
        fetched_count = 0
        loop do
          fetched_count += response["records"].length
          Embulk.logger.info "Fetched #{fetched_count}/#{response["totalSize"]} records."
          break if response["done"]
          next_url = response["nextRecordsUrl"]

          response = @retryer.with_retry do
            @api.get(next_url)
          end

          add_records(response["records"])
          fetch_count += 1
        end
      end

      def add_records(records)
        records = SfdcInputPluginUtils.extract_records(records, @schema)

        records.each do |record|
          if record.has_key?("LastModifiedDate")
            updated_at = Time.parse(record["LastModifiedDate"])
            set_latest_updated_at(updated_at)

            if @continue_from && @continue_from >= updated_at
              Embulk.logger.warn "'#{updated_at}'(LastModifiedDate) is earlier than or equal to '#{@continue_from}'(continue_from). Skipped"
              next
            end
          end

          values = record_to_values(record)
          page_builder.add(values)
        end
      end

      def set_latest_updated_at(updated_at)
        @latest_updated = [
          @latest_updated || updated_at,
          updated_at
        ].max
      end

      def record_to_values(record)
        @schema.collect do |column|
          val = record[column["name"]]
          if column["type"] == "timestamp" && val
            begin
              val = Time.parse(val.to_s)
            rescue ArgumentError => e # invalid date
              raise ConfigError.new "The value '#{val}' (as '#{column['name']}') is invalid time format"
            end
          elsif val.is_a?(Hash)
            val = val.to_s
          end
          val
        end
      end
    end
  end
end
