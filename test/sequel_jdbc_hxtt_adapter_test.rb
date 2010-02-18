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

  context "after connecting" do
    setup { @db = create_test_database }
    teardown { delete_test_database }

    should "report that the database type is access" do
      assert_equal :access, @db.database_type
    end

    should "provide a list of existing tables" do
      db = @db
      db.drop_table(:testing) rescue nil
      assert_kind_of Array, db.tables
      assert_does_not_contain(db.tables, :testing)
      db.create_table :testing do
        boolean :ok
      end
      assert_contains(db.tables, :testing)
    end

    should "support sequential primary keys" do
      @db.create_table(:with_pk) { primary_key :id; String :name }
      @db[:with_pk] << {:name => 'abc'}
      @db[:with_pk] << {:name => 'def'}
      @db[:with_pk] << {:name => 'ghi'}
      assert_equal @db[:with_pk].order(:name).all, [
        {:id => 1, :name => 'abc'},
        {:id => 2, :name => 'def'},
        {:id => 3, :name => 'ghi'}
      ]
      #@db.execute("CREATE TABLE [WITH_PK] ([ID] integer AUTO_INCREMENT PRIMARY KEY, [NAME] varchar(255))")
    end
  end
end
