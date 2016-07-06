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
      #       record data has 3 types
      #         1. self columns
      #         2. parent's columns (follow child-to-parent relationship)
      #         3. child's columns (follow parent-to-child relationship)
      def self.extract_records(json)
        json.map do |elements|
          record = {}

          elements.each {|key, value|
            if key != "attributes"
              if value.is_a?(Hash)
                # for parent's columns
                if value.has_key?("attributes")
                  record.merge!(extract_parent_elements(key, value))
                # for child's columns or something else
                else
                  record[key] = value
                end
              # for self columns
              else
                record[key] = value
              end
            end
          }
          record
        end
      end

      def self.extract_parent_elements(key, elements)
        record = {}

        elements.each{|k, v|
          full_key_name = key + '.' + k

          # follow child-to-parent relationship recursivly
          if v.is_a?(Hash)
            if v.has_key?("attributes")
              record.merge!(extract_parent_elements(full_key_name, v))
            end
          else
            record[full_key_name] = v if k != "attributes"
          end
        }

        record
      end
    end
  end
end
