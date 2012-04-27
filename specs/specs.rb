require 'rubygems'
require 'active_record'
require 'rspec'

ActiveRecord::Base.logger =
  Logger.new(STDOUT).tap { |l| l.level = Logger::DEBUG }

$LOAD_PATH << File.expand_path(File.join( File.dirname( __FILE__ ), '..', 'lib' ))

require 'active_record/connection_adapters/master_slave_adapter'

class ActiveRecord::Base
  cattr_accessor :master_mock, :slave_mock
  def self.test_connection(config)
    config[:database] == 'slave' ? slave_mock : master_mock
  end
end

describe ActiveRecord::ConnectionAdapters::MasterSlaveAdapter do
  let(:default_database_setup) do
    {
      :adapter => 'master_slave',
      :username => 'root',
      :database => 'slave',
      :connection_adapter => 'test',
      :master => { :username => 'root', :database => 'master' },
      :slaves => [{ :database => 'slave' }],
    }
  end

  let(:database_setup) { default_database_setup }

  let(:mocked_methods) do
    {
      #:verify! => true,
      :reconnect! => true,
      #:run_callbacks => true,
      :disconnect! => true,
    }
  end

  let!(:master_connection) do
    mock(
      'master connection',
      mocked_methods.merge(:open_transactions => 0)
    ).tap do |conn|
      conn.stub!(:uncached).and_yield
      ActiveRecord::Base.master_mock = conn
    end
  end

  let!(:slave_connection) do
    mock('slave connection', mocked_methods).tap do |conn|
      conn.stub!(:uncached).and_yield
      ActiveRecord::Base.slave_mock = conn
    end
  end

  def adapter_connection
    ActiveRecord::Base.connection
  end

  SchemaStatements = ActiveRecord::ConnectionAdapters::SchemaStatements.instance_methods.map(&:to_sym)
  SelectMethods = [ :select_all, :select_one, :select_rows, :select_value, :select_values ]
  Clock = ActiveRecord::ConnectionAdapters::MasterSlaveAdapter::Clock

  before do
    unless database_setup[:disable_connection_test] == 'true'
      [ master_connection, slave_connection ].each do |c|
        c.should_receive(:active?).exactly(2).times.and_return(true)
      end
    end
    ActiveRecord::Base.establish_connection(database_setup)
  end

  after do
    ActiveRecord::Base.connection_handler.clear_all_connections!
  end

  describe 'common configuration' do
    before do
      [ master_connection, slave_connection ].each do |c|
        c.stub!( :select_value ).with( "SELECT 1", "test select" ).and_return( true )
      end
    end

    it "should call 'columns' on master" do
      master_connection.should_receive(:columns)
      adapter_connection.columns
    end

    SelectMethods.each do |method|
      it "should send the method '#{method}' to the slave connection" do
        master_connection.stub!( :open_transactions ).and_return( 0 )
        slave_connection.should_receive( method ).with('testing').and_return( true )
        adapter_connection.send( method, 'testing' )
      end

      it "should send the method '#{method}' to the master connection if with_master was specified" do
        master_connection.should_receive( method ).with('testing').and_return( true )
        ActiveRecord::Base.with_master do
          adapter_connection.send( method, 'testing' )
        end
      end

      it "should send the method '#{method}' to the slave connection if with_slave was specified" do
        slave_connection.should_receive( method ).with('testing').and_return( true )
        ActiveRecord::Base.with_slave do
          adapter_connection.send( method, 'testing' )
        end
      end

      it "should send the method '#{method}' to the master connection if there are open transactions" do
        master_connection.stub!( :open_transactions ).and_return( 1 )
        master_connection.should_receive( method ).with('testing').and_return( true )
        ActiveRecord::Base.with_master do
          adapter_connection.send( method, 'testing' )
        end
      end

      it "should send the method '#{method}' to the master connection if there are open transactions, even in with_slave" do
        master_connection.stub!( :open_transactions ).and_return( 1 )
        master_connection.should_receive( method ).with('testing').and_return( true )
        ActiveRecord::Base.with_slave do
          adapter_connection.send( method, 'testing' )
        end
      end
    end # /SelectMethods.each

    SchemaStatements.each do |method|
      it "should send the method '#{method}' from ActiveRecord::ConnectionAdapters::SchemaStatements to the master"  do
        master_connection.should_receive( method ).and_return( true )
        adapter_connection.send( method )
      end
    end

    (SchemaStatements - SelectMethods).each do |method|
      it "should send the method '#{method}' from ActiveRecord::ConnectionAdapters::DatabaseStatements to the master"  do
        master_connection.should_receive( method ).and_return( true )
        adapter_connection.send( method )
      end
    end

    it "should call #visitor on master connection" do
      master_connection.should_receive(:visitor)
      adapter_connection.visitor
    end

    it 'should be a master slave connection' do
      adapter_connection.class.should == ActiveRecord::ConnectionAdapters::MasterSlaveAdapter
    end

    it 'should have a master connection' do
      adapter_connection.master_connection.should == master_connection
    end

    it 'should have a slave connection' do
      master_connection.stub!( :open_transactions ).and_return( 0 )
      adapter_connection.slave_connection!.should == slave_connection
    end
  end

  describe "connection testing" do
    context "disabled" do
      let(:database_setup) do
        default_database_setup.merge(:disable_connection_test => 'true')
      end

      context "on master" do
        SchemaStatements.each do |method|
          it "should not perform the testing when #{method} is called" do
            master_connection.tap do |c|
              c.should_not_receive(:active?)
              c.should_receive(method).with('testing').and_return(true)
            end
            adapter_connection.send(method, 'testing')
          end
        end
      end

      context "on slave" do
        SelectMethods.each do |method|
          it "should not perform the testing when #{method} is called" do
            slave_connection.tap do |c|
              c.should_not_receive(:active?)
              c.should_receive(method).with('testing').and_return(true)
            end
            adapter_connection.send(method, 'testing')
          end
        end
      end
    end
  end

  describe 'with connection eager loading enabled' do
    it 'should eager load the connections' do
      adapter_connection.connections.should include(master_connection, slave_connection)
    end
  end

  describe 'consistency' do
    before do
      ActiveRecord::ConnectionAdapters::MasterSlaveAdapter.reset!

      [ master_connection, slave_connection ].each do |c|
        c.stub!(:select_value).with("SELECT 1", "test select").and_return(true)
      end
    end

    def zero
      Clock.zero
    end

    def master_position(pos)
      Clock.new('', pos)
    end

    def slave_should_report_clock(pos)
      pos = Array(pos)
      values = pos.map { |p| { 'Relay_Master_Log_File' => '', 'Exec_Master_Log_Pos' => p } }
      slave_connection.
        should_receive('select_one').exactly(pos.length).with('SHOW SLAVE STATUS').
        and_return(*values)
    end

    def master_should_report_clock(pos)
      pos = Array(pos)
      values = pos.map { |p| { 'File' => '', 'Position' => p } }
      master_connection.
        should_receive('select_one').exactly(pos.length).with('SHOW MASTER STATUS').
        and_return(*values)
    end

    SelectMethods.each do |method|
      it "should raise an exception if consistency is nil" do
        lambda do
          ActiveRecord::Base.with_consistency(nil) do
          end
        end.should raise_error(ArgumentError)
      end

      it "should send the method '#{method}' to the slave if clock.zero is given" do
        slave_should_report_clock(0)
        slave_connection.should_receive(method).with('testing').and_return(true)
        old_clock = zero
        new_clock = ActiveRecord::Base.with_consistency(old_clock) do
          adapter_connection.send(method, 'testing')
        end
        new_clock.should be_a(zero.class)
        new_clock.should equal(zero)
      end

      it "should send the method '#{method}' to the master if slave hasn't cought up to required clock yet" do
        slave_should_report_clock(0)
        master_connection.should_receive(method).with('testing').and_return(true)
        old_clock = master_position(1)
        new_clock = ActiveRecord::Base.with_consistency(old_clock) do
          adapter_connection.send(method, 'testing' )
        end
        new_clock.should be_a(zero.class)
        new_clock.should equal(old_clock)
      end

      it "should send the method '#{method}' to the master connection if there are open transactions" do
        master_connection.stub!(:open_transactions).and_return(1)
        master_connection.should_receive(method).with('testing').and_return(true)
        old_clock = zero
        new_clock = ActiveRecord::Base.with_consistency(old_clock) do
          adapter_connection.send(method, 'testing')
        end
        new_clock.should be_a(zero.class)
        new_clock.should equal(zero)
      end

      it "should send the method '#{method}' to the master after a write operation" do
        slave_should_report_clock(0)
        master_should_report_clock(2)
        slave_connection.should_receive(method).with('testing').and_return(true)
        master_connection.should_receive('update').with('testing').and_return(true)
        master_connection.should_receive(method).with('testing').and_return(true)
        old_clock = zero
        new_clock = ActiveRecord::Base.with_consistency(old_clock) do
          adapter_connection.send(method, 'testing')   # slave
          adapter_connection.send('update', 'testing') # master
          adapter_connection.send(method, 'testing')   # master
        end
        new_clock.should be_a(zero.class)
        new_clock.should > old_clock
      end
    end

    it "should update the clock after a transaction" do
      slave_should_report_clock(0)
      master_should_report_clock([0, 1, 1])

      slave_connection.
        should_receive('select_all').exactly(1).times.with('testing').
        and_return(true)

      master_connection.
        should_receive('update').exactly(3).times.with('testing').
        and_return(true)
      master_connection.
        should_receive('select_all').exactly(5).times.with('testing').
        and_return(true)
      %w(begin_db_transaction
         commit_db_transaction
         increment_open_transactions
         decrement_open_transactions
         outside_transaction?).each do |txstmt|
        master_connection.should_receive(txstmt).exactly(1).times
      end

      master_connection.
        should_receive('open_transactions').exactly(13).times.
        and_return(
          # adapter: with_consistency, select_all, update, select_all
          0, 0, 0, 0,
          # connection: transaction
          0,
          # adapter: select_all, update, select_all, commit_db_transaction
          1, 1, 1, 0,
          # connection: transaction (ensure)
          0,
          # adapter: select_all, update, select_all
          0, 0, 0
         )

      old_clock = zero
      new_clock = ActiveRecord::Base.with_consistency(old_clock) do
        adapter_connection.send('select_all', 'testing') # slave  s=0 m=0
        adapter_connection.send('update', 'testing')     # master s=0 m=1
        adapter_connection.send('select_all', 'testing') # master s=0 m=1

        ActiveRecord::Base.transaction do
          adapter_connection.send('select_all', 'testing') # master s=0 m=1
          adapter_connection.send('update', 'testing')     # master s=0 m=1
          adapter_connection.send('select_all', 'testing') # master s=0 m=1
        end

        adapter_connection.send('select_all', 'testing') # master s=0 m=2
        adapter_connection.send('update', 'testing')     # master s=0 m=3
        adapter_connection.send('select_all', 'testing') # master s=0 m=3
      end

      new_clock.should > old_clock
    end

    context "with nested with_consistency" do
      it "should return the same clock if not writing and no lag" do
        slave_should_report_clock(0) # note: tests memoizing slave clock
        slave_connection.
          should_receive('select_one').exactly(3).times.with('testing').
          and_return(true)

        old_clock = zero
        new_clock = ActiveRecord::Base.with_consistency(old_clock) do
          adapter_connection.send('select_one', 'testing')
          ActiveRecord::Base.with_consistency(old_clock) do
            adapter_connection.send('select_one', 'testing')
          end
          adapter_connection.send('select_one', 'testing')
        end
        new_clock.should equal(old_clock)
      end

      it "requesting a newer clock should return a new clock" do
        adapter_connection.
          should_receive('slave_consistent?').exactly(2).times.
          and_return(true, false)
        slave_connection.
          should_receive('select_all').exactly(2).times.with('testing').
          and_return(true)
        master_connection.
          should_receive('select_all').exactly(1).times.with('testing').
          and_return(true)

        start_clock = zero
        inner_clock = zero
        outer_clock = ActiveRecord::Base.with_consistency(start_clock) do
          adapter_connection.send('select_all', 'testing') # slave
          inner_clock = ActiveRecord::Base.with_consistency(master_position(1)) do
            adapter_connection.send('select_all', 'testing') # master
          end
          adapter_connection.send('select_all', 'testing') # slave
        end

        start_clock.should equal(outer_clock)
        inner_clock.should > start_clock
      end
    end

    it "should do the right thing when nested inside with_master" do
      slave_should_report_clock(0)
      slave_connection.should_receive('select_all').exactly(1).times.with('testing').and_return(true)
      master_connection.should_receive('select_all').exactly(2).times.with('testing').and_return(true)
      ActiveRecord::Base.with_master do
        adapter_connection.send('select_all', 'testing') # master
        ActiveRecord::Base.with_consistency(zero) do
          adapter_connection.send('select_all', 'testing') # slave
        end
        adapter_connection.send('select_all', 'testing') # master
      end
    end

    it "should do the right thing when nested inside with_slave" do
      slave_should_report_clock(0)
      slave_connection.should_receive('select_all').exactly(3).times.with('testing').and_return(true)
      ActiveRecord::Base.with_slave do
        adapter_connection.send('select_all', 'testing') # slave
        ActiveRecord::Base.with_consistency(zero) do
          adapter_connection.send('select_all', 'testing') # slave
        end
        adapter_connection.send('select_all', 'testing') # slave
      end
    end

    it "should do the right thing when wrapping with_master" do
      slave_should_report_clock(0)
      slave_connection.should_receive('select_all').exactly(2).times.with('testing').and_return(true)
      master_connection.should_receive('select_all').exactly(1).times.with('testing').and_return(true)
      ActiveRecord::Base.with_consistency(zero) do
        adapter_connection.send('select_all', 'testing') # slave
        ActiveRecord::Base.with_master do
          adapter_connection.send('select_all', 'testing') # master
        end
        adapter_connection.send('select_all', 'testing') # slave
      end
    end

    it "should do the right thing when wrapping with_slave" do
      slave_should_report_clock(0)
      slave_connection.should_receive('select_all').exactly(1).times.with('testing').and_return(true)
      master_connection.should_receive('select_all').exactly(2).times.with('testing').and_return(true)
      ActiveRecord::Base.with_consistency(master_position(1)) do
        adapter_connection.send('select_all', 'testing') # master
        ActiveRecord::Base.with_slave do
          adapter_connection.send('select_all', 'testing') # slave
        end
        adapter_connection.send('select_all', 'testing') # master
      end
    end
  end # /with_consistency

  describe "transaction callbacks" do
    before do
      ActiveRecord::ConnectionAdapters::MasterSlaveAdapter.reset!
    end

    def run_tx
      adapter_connection.
        should_receive('master_clock').
        and_return(Clock.new('', 1))
      %w(begin_db_transaction
         commit_db_transaction
         increment_open_transactions
         decrement_open_transactions
         outside_transaction?).each do |txstmt|
        master_connection.
          should_receive(txstmt).exactly(1).times
      end
      master_connection.
        should_receive('open_transactions').exactly(4).times.
        and_return(0, 1, 0, 0)

      master_connection.
        should_receive('update').with('testing').
        and_return(true)

      ActiveRecord::Base.transaction do
        adapter_connection.send('update', 'testing')
      end
    end

    def fail_tx
      %w(begin_db_transaction
         rollback_db_transaction
         increment_open_transactions
         decrement_open_transactions).each do |txstmt|
        master_connection.
          should_receive(txstmt).exactly(1).times
      end
      master_connection.
        should_receive('outside_transaction?').exactly(2).times
      master_connection.
        should_receive('open_transactions').exactly(3).times.
        and_return(0, 1, 0)
      master_connection.
        should_receive('update').with('testing').
        and_return(true)

      ActiveRecord::Base.transaction do
        adapter_connection.send('update', 'testing')
        raise "rollback"
      end
    rescue
      nil
    end

    context "on commit" do
      it "on_commit callback should be called" do
        x = false
        adapter_connection.on_commit { x = true }
        lambda { run_tx }.should change { x }.to(true)
      end

      it "on_rollback callback should not be called" do
        x = false
        adapter_connection.on_rollback { x = true }
        lambda { run_tx }.should_not change { x }
      end
    end

    context "on rollback" do
      it "on_commit callback should not be called" do
        x = false
        adapter_connection.on_commit { x = true }
        lambda { fail_tx }.should_not change { x }
      end

      it "on_rollback callback should be called" do
        x = false
        adapter_connection.on_rollback { x = true }
        lambda { fail_tx }.should change { x }.to(true)
      end
    end
  end

  describe "query cache" do
    describe "#cache" do
      it "activities query caching on all connections" do
        master_connection.should_receive(:cache).and_yield
        slave_connection.should_receive(:cache).and_yield
        master_connection.should_not_receive(:select_value)
        slave_connection.should_receive(:select_value)

        adapter_connection.cache do
          adapter_connection.select_value("SELECT 42")
        end
      end
    end

    describe "#uncached" do
      it "deactivates query caching on all connections" do
        master_connection.should_receive(:uncached).and_yield
        slave_connection.should_receive(:uncached).and_yield
        master_connection.should_not_receive(:select_value)
        slave_connection.should_receive(:select_value)

        adapter_connection.uncached do
          adapter_connection.select_value("SELECT 42")
        end
      end
    end

    describe "#clear_query_cache" do
      it "clears the query cache on all connections" do
        master_connection.should_receive(:clear_query_cache)
        slave_connection.should_receive(:clear_query_cache)

        adapter_connection.clear_query_cache
      end
    end
  end
end
