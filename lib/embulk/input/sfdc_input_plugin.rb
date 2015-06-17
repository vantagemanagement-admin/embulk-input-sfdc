require "embulk/input/sfdc/api"
require "embulk/input/sfdc_input_plugin_utils"
require "logger"

module Embulk
  module Input

    class SfdcInputPlugin < InputPlugin
      Plugin.register_input("sfdc", self)

      GUESS_RECORDS_COUNT = 30
      PREVIEW_RECORDS_COUNT = 15
      MAX_FETCHABLE_COUNT = 2000

      def self.transaction(config, &control)
        task = {}

        task[:login_url] = config.param("login_url", :string, default: Sfdc::Api::DEFAULT_LOGIN_URL)
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
        commit_reports = yield(task, columns, count)

        next_config_diff = {}
        return next_config_diff
      end

      def self.guess(config)
        login_url = config.param("login_url", :string, default: Sfdc::Api::DEFAULT_LOGIN_URL)
        target = config.param("target", :string)

        api = Sfdc::Api.new.setup(login_url, embulk_config_to_hash(config))

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
        @api = Sfdc::Api.new.setup(task["login_url"], task["config"])
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

        commit_report = {}
        return commit_report
      end

      private

      def preview?
        # NOTE: This is workaround for "org.embulk.spi.Exec.isPreview"
        # TODO: Extract process for preview command to method
        begin
          org.embulk.spi.Exec.session().isPreview()
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
            SfdcInputPluginUtils.cast(record[column["name"]], column["type"])
          end

          page_builder.add(values)
        end
      end

      # TODO: Replace Embulk::Logger after merging
      # https://github.com/embulk/embulk/pull/200
      def self.logger
        @logger ||=
          begin
            logger = Logger.new($stdout)
            logger.formatter = proc do |severity, datetime, progname, msg|
              "#{datetime.strftime("%Y-%m-%d %H:%M:%S.%L %z")} [#{severity}] #{msg}\n"
            end
            logger
          end
      end

      def logger
        self.class.logger
      end
    end
  end
end
