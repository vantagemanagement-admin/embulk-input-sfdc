# This module contains methods for Plugin.

module Embulk
  module Input
    module SfdcInputPluginUtils
      # Guess::SchemaGuess.from_hash_records returns Columns
      # containing 'index' key, but it is needless.
      def self.guess_columns(records)
        schema = Guess::SchemaGuess.from_hash_records(records)

        schema.map do |c|
          column = {name: c.name, type: c.type}
          column[:format] = c.format if c.format
          column
        end
      end
    end
  end
end
