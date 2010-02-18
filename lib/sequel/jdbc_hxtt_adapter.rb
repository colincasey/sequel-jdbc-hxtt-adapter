# encoding: utf-8
require 'sequel'
require 'sequel/adapters/jdbc'
require 'fileutils'

Sequel::JDBC::DATABASE_SETUP[:access] = proc do |db|
  # ensure the mdb file exists if the URI specifies a local resource
  if db.opts[:uri].match(/^jdbc:access:[\/]{3}(.*mdb)$/)
    mdb = $1
    unless File.exist?(mdb)
      if db.opts.delete(:create)
        #puts "No access database exists at #{mdb}, creating..."
        empty_mdb = "#{File.dirname(__FILE__)}/adapters/jdbc/resources/empty.mdb"
        FileUtils.cp(empty_mdb, mdb)
      else
        raise "No access database exists at #{mdb}"
      end
    end
  end
  require 'sequel/adapters/jdbc/hxtt'
  db.extend(Sequel::JDBC::HXTT::DatabaseMethods)
  com.hxtt.sql.access.AccessDriver
end