# encoding: utf-8
require 'sequel/adapters/jdbc'
Sequel::JDBC::DATABASE_SETUP[:hxtt] = proc do |db|
  require 'sequel/adapters/jdbc/hxtt'
  db.extend(Sequel::JDBC::HXTT::DatabaseMethods)
  com.hxtt.sql.access.AccessDriver
end