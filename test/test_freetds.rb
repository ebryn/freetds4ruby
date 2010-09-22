require "test/unit"
require "freetds"

class TestFreeTDS < Test::Unit::TestCase

  def setup
    @config = {
      :servername => 'local', # name of server in your freetds.conf
      :username => 'sa',
      :password => ''
    }
    @driver = FreeTDS::Driver.new
    assert_not_nil(@driver, "driver creation failed")
    
    @connection = @driver.connect(@config)

    @connection.statement('drop database freetds_test').execute    
    @connection.statement('create database freetds_test').execute
    @connection.statement('use freetds_test').execute
  end
  
  def teardown
    @connection.close
  end
  
  def test_basics
    @connection.statement('create table posts ( id int, name varchar(100), body text, post_date datetime)').execute
    @connection.statement("insert into posts values (1, 'Foo', 'This is a test message', getdate())").execute
    @connection.statement("insert into posts values (2, 'Bar', 'This is another test message', getdate())").execute

    @connection.statement("SET TEXTSIZE #{1024*1024*1024}").execute

    statement = @connection.statement('select * from posts')
    statement.execute

    # puts statement.rows[0].inspect
    assert_not_nil(statement.columns, "columns should not be nil")
    assert_equal(4, statement.columns.size, "there should be 4 columns")
    assert_not_nil(statement.rows, "rows should not be nil")
    assert_equal(2, statement.rows.length, "two rows should have been returned")
    assert_nil(statement.status, "status should be nil")
    
    assert_equal(1, statement.rows[0]["id"], "post id should match")
    assert_equal("Foo", statement.rows[0]["name"], "post name should match")
    # assert_equal(Time, statement.rows[0]["post_date"].class, "post date should be a Time")

    row = statement.rows.first
    # puts statement.columns.inspect
    statement.columns.each do |col|
      assert_not_nil(col['name'])
      assert(row.has_key?(col['name']), "Column #{col[name]} missing from selected row")
    end
  end
  
  def test_bad_statement
    bad_statement = @connection.statement('this should fail')
    assert_raise(IOError, "The statement execution should have failed with an IOError") {
      bad_statement.execute
    }
  end
  
  def test_floats
    @connection.statement('create table float_test ( num float )').execute
    @connection.statement("insert into float_test values ( 1.75 )").execute

    float_statement = @connection.statement('select * from float_test')
    float_statement.execute
    row = float_statement.rows.first
    assert_equal(1.75, row["num"], "floats should match")
  end
  
  def test_large_text_fields
    @connection.statement('create table text_field_test ( id int, data text )').execute
    @connection.statement("insert into text_field_test values ( 1, '#{'a'*20}' )").execute
    @connection.statement("insert into text_field_test values ( 2, '#{'a'*500}' )").execute
    @connection.statement("insert into text_field_test values ( 3, '#{'a'*5000000}' )").execute
    
    text_statement = @connection.statement('select * from text_field_test where id = 1')
    text_statement.execute
    row = text_statement.rows.first
    assert_equal('a'*20, row["data"], "data should match")
    text_statement = @connection.statement('select * from text_field_test where id = 2')
    text_statement.execute
    row = text_statement.rows.first
    assert_equal('a'*500, row["data"], "data should match")
    text_statement = @connection.statement('select * from text_field_test where id = 3')
    text_statement.execute
    row = text_statement.rows.first
    assert_equal('a'*5000000, row["data"], "data should match")
  end

  def test_identity_insert
    # identity insert testing
    @connection.statement('create table identity_insert_test ( id int identity, data text )').execute

    assert_nothing_raised {
      @connection.statement("SET IDENTITY_INSERT identity_insert_test ON").execute
      @connection.statement("INSERT INTO identity_insert_test (id, data) VALUES ( 1, '#{'a'*20}' )").execute
      @connection.statement("SET IDENTITY_INSERT identity_insert_test OFF").execute
    }
  end
  
  def test_previous_inserted_id  
    @connection.statement('create table posts ( id int, name varchar(100), body text, post_date datetime)').execute
    
    # get previously inserted record's id
    @connection.statement("insert into posts values (4, 'Another', 'This is yet another test message', getdate())").execute
    stmt = @connection.statement("SELECT SCOPE_IDENTITY() AS Ident")
    stmt.execute
    id_value = stmt.rows.first["Ident"]
    # require 'pp'
    # pp stmt.rows
    # pp stmt.columns
    assert_equal(4, id_value, "inserted record's ID should match")
  end
  
  def test_bigint
    @connection.statement('create table bigint_test ( id bigint )').execute
    bnum  = 9_000_000_000_000_000_000
    @connection.statement("insert into bigint_test ( id ) VALUES ( #{bnum} )").execute
    stmt = @connection.statement('select * from bigint_test')
    stmt.execute
    assert_equal bnum, stmt.rows.first["id"], "bigints should be equal"
  end
  
  def test_bad_connection
    # these don't hit the server
    assert_raise(ArgumentError) { @driver.connect({}) }
    
    assert_raise(IOError) { @driver.connect({:servername => 'beast', :username => 'xxxx'}) }
  end


end
