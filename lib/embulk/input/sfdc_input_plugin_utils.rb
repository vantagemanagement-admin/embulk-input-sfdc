# This module contains methods for Plugin.

module Embulk
  module Input
    module SfdcInputPluginUtils
      # Guess::SchemaGuess.from_hash_records returns Columns
      # containing 'index' key, but it is needless.
      def self.guess_columns(fields={}, sobjects)
        records = extract_records(sobjects["records"])
        schema = Guess::SchemaGuess.from_hash_records(records)

        schema.map do |c|
          column = {name: c.name, type: c.type}
          column[:format] = c.format if c.format
          column
        end

        schema.reject { |col| !fields.include?(col["name"]) }
      end

      def self.build_soql(target, fields={}, metadata)
        target_columns = metadata["fields"].map {|metadata_fields| metadata_fields["name"] }
        filtered_columns = target_columns.reject { |col| !fields.include?(col) } 
        "SELECT #{filtered_columns.join(',')} FROM #{target}"
      end

      # NOTE: Force.com query API returns JSON including
      #       sobject name (a.k.a "attributes") and record data.
      #       record data has 3 types
      #         1. self columns
      #         2. parent's columns (follow child-to-parent relationship)
      #         3. child's columns (follow parent-to-child relationship)
      def self.extract_records(json, schema={})
        type_json_columns = schema_to_type_json_columns(schema)

        json.map do |elements|
          record = {}

          elements.each {|key, value|
            if key != "attributes"
              if value.is_a?(Hash) and !schema.empty?
                # for parent's columns (expand json)
                if value.has_key?("attributes") and !type_json_columns.has_key?(key)
                  record.merge!(extract_parent_elements(key, value, type_json_columns))
                # for child's columns, parent's columns (as json) or something else
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

      def self.extract_parent_elements(key, elements, type_json_columns)
        record = {}

        elements.each{|k, v|
          full_key_name = key + '.' + k

          if k != "attributes"
            if v.is_a?(Hash)
              # for parent's columns (expand json)
              # follow child-to-parent relationship recursivly
              if v.has_key?("attributes") and !type_json_columns.has_key?(full_key_name)
                record.merge!(extract_parent_elements(full_key_name, v, type_json_columns))
              # for parent's columns (as json)
              else
                record[full_key_name] = v
              end
            # for self columns
            else
              record[full_key_name] = v
            end
          end
        }
        record
      end

      def self.schema_to_type_json_columns(schema)
        columns = {}
        schema.map{|column| columns[column["name"]] = 1 if column["type"] == "json" }
        columns
      end
    end
  end
end
