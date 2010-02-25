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

  context "with test database" do
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
    end

    should "support timestamps, dates, and times" do
      @db.create_table(:time) do
        timestamp :ts
        date :d
        time :t
      end
      t1 = Time.now
      @db[:time] << { :ts => t1, :d => t1, :t => t1 }
      assert_equal [t1], @db[:time].map(:ts)
      assert_equal [t1], @db[:time].map(:d)
      assert_equal [t1], @db[:time].map(:t)
    end

    should "should correctly parse the schema" do
      @db.create_table(:schema_test) do
        primary_key :id
        boolean   :b
        integer   :i
        timestamp :t
        String    :s
        date      :d
      end

      assert_equal [
        [:id,{:type=>:integer, :db_type=>"INTEGER AUTO_INCREMENT", :default=>nil, :allow_null=>true, :primary_key=>true, :column_size=>4, :ruby_default=>nil}],
        [:b, {:type=>:boolean, :db_type=>"BOOLEAN", :default=>nil, :allow_null=>true, :primary_key=>false, :column_size=>0, :ruby_default=>nil}],
        [:i, {:type=>:integer, :db_type=>"INTEGER", :default=>nil, :allow_null=>true, :primary_key=>false, :column_size=>4, :ruby_default=>nil}],
        [:t, {:type=>:datetime, :db_type=>"TIMESTAMP", :default=>nil, :allow_null=>true, :primary_key=>false, :column_size=>8, :ruby_default=>nil}],
        [:s, {:type=>:string, :db_type=>"VARCHAR", :default=>nil, :allow_null=>true, :primary_key=>false, :column_size=>510, :ruby_default=>nil}],
        [:d, {:type=>:datetime, :db_type=>"TIMESTAMP", :default=>nil, :allow_null=>true, :primary_key=>false, :column_size=>8, :ruby_default=>nil}]
      ], @db.schema(:schema_test, :reload => true)
    end

    should "be able to set a default value on a column" do
      @db.create_table! :words do
        String :word
        String :language, :default => 'en'
      end

      @db[:words] << { :word => 'hello' }
      @db[:words] << { :word => 'bonjour', :language => 'fr' }
      
      assert_equal [
        { :word => 'hello', :language => 'en' },
        { :word => 'bonjour', :language => 'fr' }
      ], @db[:words].all
    end
    
    should "be able to filter boolean columns" do
      @db.create_table! :bool_test do
        boolean :val
      end
      @db[:bool_test] << { :val => true }
      @db[:bool_test] << { :val => true }
      @db[:bool_test] << { :val => false }      
      assert_equal 3, @db[:bool_test].count
      assert_equal 2, @db[:bool_test].filter(:val => true).count
      assert_equal 1, @db[:bool_test].filter(:val => false).count
    end

    context "when comparing strings fields" do
      setup do
        @db.create_table! :case do
          String :name
        end
        @db[:case] << { :name => 'colin casey' }
        @db[:case] << { :name => 'Colin Casey' }
      end

      should "be case-sensitive" do
        assert_equal 1, @db[:case].filter(:name => 'colin casey').count
        assert_equal 1, @db[:case].filter(:name => 'Colin Casey').count
      end

      should "be able to convert a string field to lowercase" do
        assert_equal 2, @db[:case].filter(:lower.sql_function(:name) => 'colin casey').count
      end

      should "be able to convert a string field to uppercase" do
        assert_equal 2, @db[:case].filter(:upper.sql_function(:name) => 'COLIN CASEY').count
      end

      should "be able to do a case-sensitive string comparison" do
        assert_equal 1, @db[:case].filter(:name.like('colin%')).count
      end

      should "be able to do a case-insensitive string comparison" do
        assert_equal 2, @db[:case].filter(:name.ilike('colin%')).count
      end
    end

    context "when handling deletions" do
      setup do
        @db.create_table! :items do
          primary_key :id
          String :name
          Float :value
        end
        @items = @db[:items]
        @items.delete # remove all records
        @items << {:name => 'abc', :value => 1.23}
        @items << {:name => 'def', :value => 4.56}
        @items << {:name => 'ghi', :value => 7.89}
      end

      should "return the number of records affected when filtered" do
        assert_equal 3, @items.count
        assert_equal 1, @items.filter(:value.sql_number < 3).delete
        assert_equal 2, @items.count
        assert_equal 0, @items.filter(:value.sql_number < 3).delete
        assert_equal 2, @items.count
      end

      should "return the number of records affected when unfiltered" do
        assert_equal 3, @items.count
        assert_equal 3, @items.delete
        assert_equal 0, @items.count
        assert_equal 0, @items.delete
      end
    end

    context "using transactions" do
      setup do
        @db.create_table! :transaction_test do
          primary_key :id
          String :name
        end
      end

      should "be able to commit a transaction" do
        assert_equal 0, @db[:transaction_test].count
        @db.transaction do
          @db[:transaction_test] << { :name => 'commited' }
        end
        assert_equal 1, @db[:transaction_test].count
      end

      should "be re-entrant" do
        assert_equal 0, @db[:transaction_test].count
        @db.transaction do
          @db[:transaction_test] << { :name => 'first' }
          @db.transaction do
            @db[:transaction_test] << { :name => 'second' }
          end
        end
        assert_equal 2, @db[:transaction_test].count
      end

      should "be able to rollback a transaction" do
        assert_equal 0, @db[:transaction_test].count
        @db.transaction do
          @db[:transaction_test] << { :name => 'rollback' }
          raise(Sequel::Rollback)
        end 
        assert_equal 0, @db[:transaction_test].count
      end
      
      should "automatically rollback a transaction on error" do
        assert_equal 0, @db[:transaction_test].count
        assert_raise(RuntimeError) do
          @db.transaction do
            @db[:transaction_test] << { :name => 'error-induced rollback' }
            raise "some error occurred"
          end 
        end
        assert_equal 0, @db[:transaction_test].count
      end

      should "support savepoints" do
        assert_equal 0, @db[:transaction_test].count
        @db.transaction do
          @db[:transaction_test] << { :name => 'Inigo Montoya' } # Inserted
          @db.transaction(:savepoint => true) do # This savepoint is rolled back
            @db[:transaction_test] << { :name => 'The 6-fingered Man' } # Not inserted
            raise(Sequel::Rollback)
          end
        end
        assert_equal 1, @db[:transaction_test].count
      end
    end
  end
end
