require "embulk/input/sfdc/api"
require "embulk/input/sfdc-input-plugin-utils"

module Embulk
  module Input

    class SfdcInputPlugin < InputPlugin
      Plugin.register_input("sfdc", self)

      def self.transaction(config, &control)
        task = {}

        task[:login_url] = config.param("login_url", :string, default: Sfdc::Api::DEFAULT_LOGIN_URL)
        task[:soql] = config.param("soql", :string)

        task[:config] = {
          client_id: config.param("client_id", :string),
          client_secret: config.param("client_secret", :string),
          username: config.param("username", :string),
          password: config.param("password", :string),
          security_token: config.param("security_token", :string),
        }

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

        config = {
          client_id: config.param("client_id", :string),
          client_secret: config.param("client_secret", :string),
          username: config.param("username", :string),
          password: config.param("password", :string),
          security_token: config.param("security_token", :string),
        }

        client = Sfdc::Api.setup(login_url, config)

        metadata = client.get_metadata(target)

        raise "Target #{target} can't be searched." if !metadata["queryable"] || !metadata["searchable"]

        # get objects for guess
        target_columns = metadata["fields"].map {|fields| fields["name"] }
        soql = "SELECT #{target_columns.join(',')} FROM #{target}"

        sobjects = client.search("#{soql} limit 5")
        sample_records = SfdcInputPluginUtils.extract_records(sobjects["records"])

        {
          "soql" => soql,
          "columns" => SfdcInputPluginUtils.guess_columns(sample_records)
        }
      end

      def init
        @api = Sfdc::Api.setup(task["login_url"], task["config"])
        @schema = task["schema"]
        @soql = task["soql"]
      end

      def run
        response = @api.search(@soql)
        add_records(page_builder, response["records"])

        while !response["done"] do
          next_url = response["nextRecordsUrl"]
          response = @api.get(next_url)

          add_records(page_builder, response["records"])
        end

        page_builder.finish

        commit_report = {}
        return commit_report
      end

      private

      def add_records(page_builder, records)
        records = SfdcInputPluginUtils.extract_records(records)

        records.each do |record|
          values = @schema.collect do |column|
            SfdcInputPluginUtils.cast(record[column["name"]], column["type"])
          end

          page_builder.add(values)
        end
      end
    end
  end
end
