# encoding: utf-8

module Sequel
  module JDBC
    # Database and Dataset instance methods for MS Access specific support via JDBC.
    module HXTT
      # Database instance methods for MS Access databases accessed via JDBC.
      module DatabaseMethods
        AUTO_INCREMENT = 'AUTO_INCREMENT'.freeze
        PRIMARY_KEY = 'PRIMARY KEY'.freeze
        SQL_BEGIN = "START TRANSACTION".freeze
        SQL_COMMIT = "COMMIT".freeze
        SQL_SAVEPOINT = 'SAVEPOINT autopoint_%d'.freeze
        SQL_ROLLBACK_TO_SAVEPOINT = 'ROLLBACK TO SAVEPOINT autopoint_%d'.freeze

        # Return instance of Sequel::JDBC::HXTT::Dataset with the given opts.
        def dataset(opts=nil)
          Sequel::JDBC::HXTT::Dataset.new(self, opts)
        end

        def database_type
          :access
        end

        def supports_savepoints?
          true
        end

        def tables
          ts = []
          m = output_identifier_meth
          metadata(:getTables, nil, nil, nil, ['TABLE'].to_java(:string)) do |h|
            h = downcase_hash_keys(h)
            ts << m.call(h[:table_name])
          end
          ts
        end

        private
        def identifier_input_method_default
          :to_s
        end

        def identifier_output_method_default
          :to_s
        end
        
        def schema_parse_table(table, opts={})
          m = output_identifier_meth
          im = input_identifier_meth
          ds = dataset
          schema, table = schema_and_table(table)
          schema ||= opts[:schema]
          schema = im.call(schema) if schema
          table = im.call(table)
          pks, ts = [], []
          metadata(:getPrimaryKeys, nil, schema, table) do |h|
            h = downcase_hash_keys(h)
            pks << h[:column_name]
          end
          metadata(:getColumns, nil, schema, table, nil) do |h|
            h = downcase_hash_keys(h)
            ts << [m.call(h[:column_name]), {:type=>schema_column_type(h[:type_name]), :db_type=>h[:type_name], :default=>(h[:column_def] == '' ? nil : h[:column_def]), :allow_null=>(h[:nullable] != 0), :primary_key=>pks.include?(h[:column_name]), :column_size=>h[:column_size]}]
          end
          ts
        end

        def downcase_hash_keys(h)
          lh = {}
          h.each { |k,v| lh[k.to_s.downcase.to_sym] = v }
          lh
        end

        def column_definition_sql(column)
          sql = "#{quote_identifier(column[:name])} #{type_literal(column)}"
          sql << UNIQUE if column[:unique]
          null = column.include?(:null) ? column[:null] : column[:allow_null]
          sql << NOT_NULL if null == false
          sql << NULL if null == true
          sql << " DEFAULT #{literal(column[:default])}" if column.include?(:default)
          sql << " #{auto_increment_sql} " if column[:auto_increment]
          sql << PRIMARY_KEY if column[:primary_key]
          sql << column_references_column_constraint_sql(column) if column[:table]
          sql
        end

        def auto_increment_sql
          AUTO_INCREMENT
        end
        
        def begin_transaction_sql
          SQL_BEGIN
        end

        def begin_savepoint_sql(depth)
          SQL_SAVEPOINT % depth
        end

        def rollback_savepoint_sql(depth)
          SQL_ROLLBACK_TO_SAVEPOINT % depth
        end

        def commit_transaction_sql
          SQL_COMMIT
        end

        def type_literal_generic_float(column)
          :float
        end
      end

      # Dataset class for MS Access datasets accessed via JDBC.
      class Dataset < JDBC::Dataset
        BOOL_TRUE = 'TRUE'.freeze
        BOOL_FALSE = 'FALSE'.freeze
        SELECT_CLAUSE_METHODS = Dataset.clause_methods(:select, %w'limit columns from where')

        # access uses [] to quote identifiers
        def quoted_identifier(name)
          "[#{name}]"
        end

        def literal_false
          BOOL_FALSE
        end

        def literal_true
          BOOL_TRUE
        end

        def select_clause_methods
          SELECT_CLAUSE_METHODS
        end

        def select_limit_sql(sql)
          sql << " TOP #{@opts[:limit]}" if @opts[:limit]
        end

        def supports_is_true?
          false
        end        
      end
    end
  end
end
