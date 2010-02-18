# encoding: utf-8

module Sequel
  module JDBC
    # Database and Dataset instance methods for MS Access specific support via JDBC.
    module HXTT
      # Database instance methods for MS Access databases accessed via JDBC.
      module DatabaseMethods
        def database_type
          :access
        end

        private
        def type_literal_generic_datetime(column)
          :time
        end
      end

      # Dataset class for MS Access datasets accessed via JDBC.
      class Dataset < JDBC::Dataset

      end
    end
  end
end
