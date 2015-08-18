# This module contains methods for Plugin.

module Embulk
  module Input
    module SfdcInputPluginUtils
      # Guess::SchemaGuess.from_hash_records returns Columns
      # containing 'index' key, but it is needless.
      def self.guess_columns(sobjects)
        records = extract_records(sobjects["records"])
        schema = Guess::SchemaGuess.from_hash_records(records)

        schema.map do |c|
          column = {name: c.name, type: c.type}
          column[:format] = c.format if c.format
          column
        end
      end

      def self.build_soql(target, metadata)
        target_columns = metadata["fields"].map {|fields| fields["name"] }
        "SELECT #{target_columns.join(',')} FROM #{target}"
      end

      # NOTE: Force.com query API returns JSON including
      #       sobject name (a.k.a "attributes") and record data.
      def self.extract_records(json)
        json.map do |elements|
          elements.reject {|key, _| key == "attributes" }
        end
      end
    end
  end
end
