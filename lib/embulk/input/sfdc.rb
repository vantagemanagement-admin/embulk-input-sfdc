require "httpclient"
require "json"
require "sfdc/api"

module Embulk
  module Input

    class SfdcInputPlugin < InputPlugin
      Plugin.register_input("sfdc", self)

      def self.transaction(config, &control)
        # configuration code:
        task = {
          "property1" => config.param("property1", :string),
          "property2" => config.param("property2", :integer, default: 0),
        }

        columns = [
          Column.new(0, "example", :string),
          Column.new(1, "column", :long),
          Column.new(2, "value", :double),
        ]

        resume(task, columns, 1, &control)
      end

      def self.resume(task, columns, count, &control)
        commit_reports = yield(task, columns, count)

        next_config_diff = {}
        return next_config_diff
      end

      def self.guess(config)
        username = config.param("username", :string)
        password = config.param("password", :string)
        login_url = config.param("login_url", :string, default: "https://login.salesforce.com")
        client_id = config.param("client_id", :string)
        client_secret = config.param("client_secret", :string)
        security_token = config.param("security_token", :string)
        target = config.param("target", :string)

        # Use named parameter or OStruct?
        config = {
          client_id: client_id,
          client_secret: client_secret,
          username: username,
          password: password,
          security_token: security_token
        }

        client = Sfdc::Api.setup(login_url, config)

        # get metadata

        sobject_metadata = client.get("/sobjects/#{target}/describe", :Accept => 'application/json; charset=UTF-8')
        metadata = JSON.parse(sobject_metadata.body)
        raise "Target #{target} can't be searched." if !metadata["queryable"] || !metadata["searchable"]

        # get objects for guess

        target_names = metadata["fields"].map do |fields|
          fields["name"]
        end

        sobjects = client.get("/query/?q=SELECT+#{target_names.join(',')}+from+#{target}+limit+5", :Accept => 'application/json; charset=UTF-8')

        raw_records = JSON.parse(sobjects.body)["records"]

        sample_records = raw_records.map do |record|
          record.reject {|key, _| key == "attributes"}
        end

        columns = Guess::SchemaGuess.from_hash_records(sample_records)
        {"columns" => columns}
      end

      def init
        # initialization code:
        @property1 = task["property1"]
        @property2 = task["property2"]
      end

      def run
        page_builder.add(["example-value", 1, 0.1])
        page_builder.add(["example-value", 2, 0.2])
        page_builder.finish

        commit_report = {}
        return commit_report
      end
    end

  end
end
