# encoding: utf-8

module Sequel
  module JDBC
    # Database and Dataset instance methods for MS Access specific support via JDBC.
    module HXTT
      # Database instance methods for MS Access databases accessed via JDBC.
      module DatabaseMethods
        AUTO_INCREMENT = 'AUTO_INCREMENT'.freeze
        PRIMARY_KEY = 'PRIMARY KEY'.freeze

        # Return instance of Sequel::JDBC::HXTT::Dataset with the given opts.
        def dataset(opts=nil)
          Sequel::JDBC::HXTT::Dataset.new(self, opts)
        end

        def database_type
          :access
        end

        private
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
