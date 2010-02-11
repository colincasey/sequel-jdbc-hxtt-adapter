require 'test_helper'
require 'tempfile'

class SequelJdbcHxttAdapterTest < Test::Unit::TestCase
  context "when connecting to an access database" do
    should "fail to connect a non-existant database" do
      assert_raise RuntimeError  do
        db = Sequel.connect('jdbc:hxtt:///i-dont-exist.mdb')
      end
    end
    
    should "create the database if requested and it doesn't exist" do
      mdb = 'db.mdb'
      assert !File.exist?(mdb)
      assert_nothing_raised do
        Sequel.connect("jdbc:hxtt:///#{mdb}", :create => true)
      end
      assert File.exist?(mdb)
      File.delete(mdb)
    end
    
    should "not create the database if requested and it does exist" do
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
end
