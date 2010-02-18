require 'test_helper'
require 'tempfile'

class SequelJdbcHxttAdapterTest < Test::Unit::TestCase
  WRITABLE_FOLDER = "/home/colin"
  ACCESS_DB = File.join(WRITABLE_FOLDER, 'sequel-jdbc-hxtt-adapter-test.mdb')

  def create_test_database
    Sequel.connect("jdbc:access:///#{ACCESS_DB}", :create => true)
  end

  def delete_test_database
    File.delete(ACCESS_DB) if File.exist?(ACCESS_DB)
  end

  def in_memory_db
    Sequel.connect("jdbc:access:/_memory_/")
  end

  context "when connecting to an access database" do
    should "fail to connect a non-existant database" do
      assert_raise RuntimeError  do
        db = Sequel.connect('jdbc:access:///i-dont-exist.mdb')
      end
    end
    
    should "create the database if requested and it doesn't exist" do
      db_to_create = File.join(WRITABLE_FOLDER, 'sequel-jdbc-hxtt-adapter-create-test.mdb')
      assert !File.exist?(db_to_create)
      assert_nothing_raised do
        Sequel.connect("jdbc:access:///#{db_to_create}", :create => true)
      end
      assert File.exist?(db_to_create)
      File.delete(db_to_create)
    end
    
    should "not create a new database when the specified file already exists" do
      temp = Tempfile.new('db.mdb')
      temp.open
      temp << 'the original file'
      temp.close
      mdb = temp.path
      assert File.exist?(mdb)
      assert_nothing_raised do
        Sequel.connect("jdbc:hxtt:///#{mdb}", :create => true)
      end
      assert_equal "the original file", IO.read(mdb)
      temp.delete
    end
  end

  should "report that the database type is access" do
    assert_equal :access, in_memory_db.database_type
  end

  context "when creating a table" do
    setup do
    end

    teardown do
    end
  end
end
