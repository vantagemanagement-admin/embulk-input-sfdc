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

        task[:schema] = config.param("columns", :array)
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

        next_config_diff = {}
        return next_config_diff
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
      end

      def run
        @soql += " LIMIT #{PREVIEW_RECORDS_COUNT}" if preview?
        response = @api.search(@soql)
        logger.debug "Start to add records...(total #{response["totalSize"]} records)"
        add_records(response["records"])

        add_next_records(response, 1)

        page_builder.finish

        logger.debug "Added all records."

        task_report = {}
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
        !!(metadata["queryable"] && metadata["searchable"])
      end

      def add_next_records(response, fetch_count)
        return if response["done"]
        logger.debug "Added #{MAX_FETCHABLE_COUNT * fetch_count}/#{response["totalSize"]} records."
        next_url = response["nextRecordsUrl"]
        response = @api.get(next_url)

        add_records(response["records"])

        add_next_records(response, fetch_count + 1)
      end

      def add_records(records)
        records = SfdcInputPluginUtils.extract_records(records)

        records.each do |record|
          values = @schema.collect do |column|
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

          page_builder.add(values)
        end
      end

      def self.logger
        Embulk.logger
      end

      def logger
        self.class.logger
      end
    end
  end
end
