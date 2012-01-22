require 'active_record'

module ActiveRecord
  class Base
    class << self
      def with_consistency(clock, &blk)
        if connection.respond_to? :with_consistency
          connection.with_consistency(clock, &blk)
        else
          yield
          nil
        end
      end

      def with_master(&blk)
        if connection.respond_to? :with_master
          connection.with_master(&blk)
        else
          yield
        end
      end

      def with_slave(&blk)
        if connection.respond_to? :with_slave
          connection.with_slave(&blk)
        else
          yield
        end
      end

      def on_commit(&blk)
        connection.on_commit(&blk) if connection.respond_to? :on_commit
      end

      def on_rollback(&blk)
        connection.on_rollback(&blk) if connection.respond_to? :on_rollback
      end

      def master_slave_connection(config)
        config = massage(config)
        load_adapter(config.fetch(:connection_adapter))
        ConnectionAdapters::MasterSlaveAdapter.new(config, logger)
      end

      def mysql_master_slave_connection(config)
        master_slave_connection(config)
      end

    private

      def massage(config)
        config = config.symbolize_keys
        skip = [ :adapter, :connection_adapter, :master, :slaves ]
        defaults = config.
          reject { |k,_| skip.include?(k) }.
          merge(:adapter => config.fetch(:connection_adapter))
        ([config.fetch(:master)] + config.fetch(:slaves, [])).map do |cfg|
          cfg[:database] = defaults[:database] if defaults.has_key?(:database)
          cfg.symbolize_keys!.reverse_merge!(defaults)
        end
        config
      end

      def load_adapter(adapter_name)
        unless self.respond_to?("#{adapter_name}_connection")
          begin
            require 'rubygems'
            gem "activerecord-#{adapter_name}-adapter"
            require "active_record/connection_adapters/#{adapter_name}_adapter"
          rescue LoadError
            begin
              require "active_record/connection_adapters/#{adapter_name}_adapter"
            rescue LoadError
              raise %Q{Please install the #{adapter_name} adapter:
                       `gem install activerecord-#{adapter_name}-adapter` (#{$!})"}
            end
          end
        end
      end
    end
  end

  module ConnectionAdapters

    class AbstractAdapter
      alias_method :orig_log_info, :log_info
      def log_info(sql, name, ms)
        connection_name =
          [ @config[:name], @config[:host], @config[:port] ].compact.join(":")
        orig_log_info sql, "[#{connection_name}] #{name || 'SQL'}", ms
      end
    end

    class MasterSlaveAdapter < AbstractAdapter

      class Clock
        include Comparable
        attr_reader :file, :position

        def initialize(file, position)
          raise ArgumentError, "file and postion may not be nil" if file.nil? || position.nil?
          @file, @position = file, position.to_i
        end

        def <=>(other)
          @file == other.file ? @position <=> other.position : @file <=> other.file
        end

        def to_s
          [ @file, @position ].join('@')
        end

        def self.zero
          @zero ||= Clock.new('', 0)
        end

        def self.infinity
          @infinity ||= Clock.new('', Float::MAX.to_i)
        end
      end

      checkout :active?

      def initialize(config, logger)
        super(nil, logger)

        @connections = {}
        @connections[:master] = connect(config.fetch(:master), :master)
        
        @connections[:slaves] = []
        if config[:slaves]
          config.fetch(:slaves).each do |cfg|
            begin
              @connections[:slaves] << connect(cfg, :slave)
            rescue StandardError => e
              Rails.logger.error "Slave can not connect ::: #{cfg.inspect}"
            end
          end
        end

        @disable_connection_test = config.delete(:disable_connection_test) == 'true'

        if config.delete(:initial_connection) == 'master'
          self.current_connection = master_connection
        else
          self.current_connection = slave_connection!
        end
      end

      # MASTER SLAVE ADAPTER INTERFACE ========================================

      def with_master
        with(self.master_connection, :master) { yield }
      end

      def with_slave
        with(self.slave_connection!, :slave) { yield }
      end

      def with_consistency(clock)
        raise ArgumentError, "consistency cannot be nil" if clock.nil?
        # try random slave, else fall back to master
        slave = slave_connection!
        conn =
          if !open_transaction? && slave_consistent?(slave, clock)
            [ slave, :slave ]
          else
            [ master_connection, :master ]
          end

        with(*conn) { yield }

        self.current_clock || clock
      end

      def on_commit(&blk)
        on_commit_callbacks.push blk
      end

      def on_rollback(&blk)
        on_rollback_callbacks.push blk
      end


      # backwards compatibility
      class << self
        def with_master(&blk)
          ActiveRecord::Base.with_master(&blk)
        end
        def with_slave(&blk)
          ActiveRecord::Base.with_slave(&blk)
        end
        def with_consistency(clock, &blk)
          ActiveRecord::Base.with_consistency(clock, &blk)
        end
        def reset!
          Thread.current[:master_slave_clock]      =
          Thread.current[:master_slave_connection] =
          Thread.current[:on_commit_callbacks]     =
          Thread.current[:on_rollback_callbacks]   =
          nil
        end
      end

      # ADAPTER INTERFACE OVERRIDES ===========================================

      def insert(*args)
        on_write { |conn| conn.insert(*args) }
      end

      def update(*args)
        on_write { |conn| conn.update(*args) }
      end

      def delete(*args)
        on_write { |conn| conn.delete(*args) }
      end

      def commit_db_transaction
        on_write { |conn| conn.commit_db_transaction }
        on_commit_callbacks.shift.call(current_clock) until on_commit_callbacks.blank?
      end

      def rollback_db_transaction
        on_commit_callbacks.clear
        with(master_connection, :master) { |conn| conn.rollback_db_transaction }
        on_rollback_callbacks.shift.call until on_rollback_callbacks.blank?
      end

      def active?
        return true if @disable_connection_test
        self.connections.map { |c| c.active? }.reduce(true) { |m,s| s ? m : s }
      end

      def reconnect!
        @connections[:slaves].delete_if do |slave|
          begin
            slave.reconnect!
            false
          rescue StandardError => e
            Rails.logger.error "Slave can not reconnect! ::: #{slave}"
            true
          end
        end
        @connections[:master].reconnect!
      end

      def disconnect!
        self.connections.each { |c| c.disconnect! }
      end

      def reset!
        self.connections.each { |c| c.reset! }
      end

      # Someone calling execute directly on the connection is likely to be a
      # write, respectively some DDL statement. People really shouldn't do that,
      # but let's delegate this to master, just to be sure.
      def execute(*args)
        on_write { |conn| conn.execute(*args) }
      end

      # ADAPTER INTERFACE DELEGATES ===========================================

      # === must go to master
      delegate :adapter_name,
               :supports_migrations?,
               :supports_primary_key?,
               :supports_savepoints?,
               :native_database_types,
               :raw_connection,
               :open_transactions,
               :increment_open_transactions,
               :decrement_open_transactions,
               :transaction_joinable=,
               :create_savepoint,
               :rollback_to_savepoint,
               :release_savepoint,
               :current_savepoint_name,
               :begin_db_transaction,
               :outside_transaction?,
               :add_limit!,
               :add_limit_offset!,
               :add_lock!,
               :default_sequence_name,
               :reset_sequence!,
               :insert_fixture,
               :empty_insert_statement,
               :case_sensitive_equality_operator,
               :limited_update_conditions,
               :insert_sql,
               :update_sql,
               :delete_sql,
               :sanitize_limit,
               :to => :master_connection
      delegate *(ActiveRecord::ConnectionAdapters::SchemaStatements.instance_methods + [{
               :to => :master_connection }])
      # no clear interface contract:
      delegate :tables,         # commented in SchemaStatements
               :indexes,        # migrations
               :truncate_table, # monkeypatching database_cleaner gem
               :primary_key,    # is Base#primary_key meant to be the contract?
               :to => :master_connection
      # ok, we might have missed more
      def method_missing(name, *args, &blk)
        master_connection.send(name.to_sym, *args, &blk).tap do
          warn %Q{
            You called the unsupported method '#{name}' on #{self.class.name}.
            In order to help us improve master_slave_adapter, please report this
            to: https://github.com/soundcloud/master_slave_adapter/issues

            Thank you.
          }
        end
      end

      # === determine read connection
      delegate :select_all,
               :select_one,
               :select_rows,
               :select_value,
               :select_values,
               :to => :connection_for_read

      def connection_for_read
        open_transaction? ? master_connection : self.current_connection
      end
      private :connection_for_read

      # === doesn't really matter, but must be handled by underlying adapter
      delegate *(ActiveRecord::ConnectionAdapters::Quoting.instance_methods + [{
               :to => :current_connection }])
      # issue #4: current_database is not supported by all adapters, though
      delegate :current_database, :to => :current_connection

      # UTIL ==================================================================

      def master_connection
        @connections[:master]
      end

      # Returns a random slave connection
      # Note: the method is not referentially transparent, hence the bang
      def slave_connection!
        @connections[:slaves].sample || master_connection
      end

      def connections
        @connections.values.inject([]) { |m,c| m << c }.flatten.compact
      end

      def current_connection
        connection_stack.first || master_connection
      end

      def current_connection=(conn)
        connection_stack.unshift conn
      end

      def current_clock
        Thread.current[:master_slave_clock]
      end

      def current_clock=(clock)
        Thread.current[:master_slave_clock] = clock
      end

      def master_clock
        conn = master_connection
        out = nil
        if status = conn.uncached { conn.select_one("SHOW MASTER STATUS") }
          out = Clock.new(status['File'], status['Position'])
        end
        if Rails.env.production?
          out ||= Clock.infinity
        else
          out ||= Clock.zero
        end
        out
      end

      def slave_clock(conn)
        out = nil
        if status = conn.uncached { conn.select_one("SHOW SLAVE STATUS") }
          out = Clock.new(status['Relay_Master_Log_File'], status['Exec_Master_Log_Pos']).tap do |c|
            set_last_seen_slave_clock(conn, c)
          end
        end
        out ||= Clock.zero
        out
      end

      def slave_consistent?(conn, clock)
        get_last_seen_slave_clock(conn).try(:>=, clock) ||
          slave_clock(conn).try(:>=, clock)
      end

    protected

      def on_write
        with(master_connection, :master) do |conn|
          yield(conn).tap do
            unless open_transaction?
              if mc = master_clock
                self.current_clock = mc unless current_clock.try(:>=, mc)
              end
              # keep using master after write
              self.current_connection = conn
            end
          end
        end
      end

      def with(conn, name)
        self.current_connection = conn
        yield(conn).tap { connection_stack.shift }
      end

    private

      def logger
        @logger # ||= Logger.new(STDOUT)
      end

      def info(msg)
        logger.try(:info, msg)
      end

      def warn(msg)
        logger.try(:warn, msg)
      end

      def debug(msg)
        logger.debug(msg) if logger && logger.debug?
      end

      def connect(cfg, name)
        adapter_method = "#{cfg.fetch(:adapter)}_connection".to_sym
        ActiveRecord::Base.send(adapter_method, { :name => name }.merge(cfg))
      end

      def open_transaction?
        master_connection.open_transactions > 0
      end

      def connection_stack
        Thread.current[:master_slave_connection] ||= []
      end

      def on_commit_callbacks
        Thread.current[:on_commit_callbacks] ||= []
      end

      def on_rollback_callbacks
        Thread.current[:on_rollback_callbacks] ||= []
      end

      def get_last_seen_slave_clock(conn)
        conn.instance_variable_get(:@last_seen_slave_clock)
      end

      def set_last_seen_slave_clock(conn, clock)
        last_seen = get_last_seen_slave_clock(conn)
        if last_seen.nil? || last_seen < clock
          conn.instance_variable_set(:@last_seen_slave_clock, clock)
        end
      end
    end
  end
end
