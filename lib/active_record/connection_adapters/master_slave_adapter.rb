require 'active_record'
require 'active_record/connection_adapters/abstract_adapter'
require 'active_record/connection_adapters/master_slave_adapter/circuit_breaker'

module ActiveRecord
  class MasterUnavailable < ConnectionNotEstablished; end

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

      def master_slave_connection(config)
        config  = massage(config)
        adapter = config.fetch(:connection_adapter)
        name    = "#{adapter}_master_slave"

        load_adapter(name)
        send(:"#{name}_connection", config)
      end

    private

      def massage(config)
        config = config.symbolize_keys
        skip = [ :adapter, :connection_adapter, :master, :slaves ]
        defaults = config.
          reject { |k,_| skip.include?(k) }.
          merge(:adapter => config.fetch(:connection_adapter))
        ([config.fetch(:master)] + config.fetch(:slaves, [])).map do |cfg|
          cfg.symbolize_keys!.reverse_merge!(defaults)
        end
        config
      end

      def load_adapter(adapter_name)
        unless respond_to?("#{adapter_name}_connection")
          begin
            require "active_record/connection_adapters/#{adapter_name}_adapter"
          rescue LoadError
            begin
              require 'rubygems'
              gem "activerecord-#{adapter_name}-adapter"
              require "active_record/connection_adapters/#{adapter_name}_adapter"
            rescue LoadError
              raise %Q{Please install the #{adapter_name} adapter:
                       `gem install activerecord-#{adapter_name}-adapter` (#{$!})}
            end
          end
        end
      end
    end
  end

  module ConnectionAdapters
    class AbstractAdapter
      if instance_methods.map(&:to_sym).include?(:log_info)
        # ActiveRecord v2.x
        alias_method :orig_log_info, :log_info
        def log_info(sql, name, ms)
          orig_log_info(sql, "[#{connection_info}] #{name || 'SQL'}", ms)
        end
      else
        # ActiveRecord v3.x
        alias_method :orig_log, :log
        def log(sql, name = 'SQL', *args, &block)
          orig_log(sql, "[#{connection_info}] #{name || 'SQL'}", *args, &block)
        end
      end

    private
      def connection_info
        @connection_info ||= @config.values_at(:name, :host, :port).compact.join(':')
      end
    end

    module MasterSlaveAdapter
      def initialize(config, logger)
        super(nil, logger)

        @config = config
        @connections = {}
        @connections[:master] = connect_to_master
        @connections[:slaves] = @config.fetch(:slaves).map { |cfg| connect(cfg, :slave) }
        @last_seen_slave_clocks = {}
        @disable_connection_test = @config[:disable_connection_test] == 'true'
        @circuit = CircuitBreaker.new(logger)

        self.current_connection = slave_connection!
      end

      # MASTER SLAVE ADAPTER INTERFACE ========================================

      def with_master
        with(master_connection) { yield }
      end

      def with_slave
        with(slave_connection!) { yield }
      end

      def with_consistency(clock)
        if clock.nil?
          raise ArgumentError, "consistency must be a valid comparable value"
        end

        # try random slave, else fall back to master
        slave = slave_connection!
        conn =
          if !open_transaction? && slave_consistent?(slave, clock)
            slave
          else
            master_connection
          end

        with(conn) { yield }

        current_clock || clock
      end

      def on_commit(&blk)
        on_commit_callbacks.push blk
      end

      def on_rollback(&blk)
        on_rollback_callbacks.push blk
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

      def execute(*args)
        on_write { |conn| conn.execute(*args) }
      end

      def commit_db_transaction
        on_write { |conn| conn.commit_db_transaction }
        on_commit_callbacks.shift.call(current_clock) until on_commit_callbacks.blank?
      end

      def rollback_db_transaction
        on_commit_callbacks.clear
        with(master_connection) { |conn| conn.rollback_db_transaction }
        on_rollback_callbacks.shift.call until on_rollback_callbacks.blank?
      end

      def active?
        return true if @disable_connection_test
        connections.map { |c| c.active? }.all?
      end

      def reconnect!
        connections.each { |c| c.reconnect! }
      end

      def disconnect!
        connections.each { |c| c.disconnect! }
      end

      def reset!
        connections.each { |c| c.reset! }
      end

      def cache(&blk)
        connections.inject(blk) do |block, connection|
          lambda { connection.cache(&block) }
        end.call
      end

      def uncached(&blk)
        connections.inject(blk) do |block, connection|
          lambda { connection.uncached(&block) }
        end.call
      end

      def clear_query_cache
        connections.each { |connection| connection.clear_query_cache }
      end

      def outside_transaction?
        nil
      end

      # ADAPTER INTERFACE DELEGATES ===========================================

      def self.rescued_delegate(*methods)
        options = methods.pop
        to = options[:to]

        file, line = caller.first.split(':', 2)
        line = line.to_i

        methods.each do |method|
          module_eval(<<-EOS, file, line)
            def #{method}(*args, &block)
              begin
                #{to}.__send__(:#{method}, *args, &block)
              rescue ActiveRecord::StatementInvalid => error
                handle_error(#{to}, error)
              end
            end
          EOS
        end
      end
      class << self; private :rescued_delegate; end

      # === must go to master
      rescued_delegate :adapter_name,
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
                       :add_limit!,
                       :default_sequence_name,
                       :reset_sequence!,
                       :insert_fixture,
                       :empty_insert_statement,
                       :case_sensitive_equality_operator,
                       :limited_update_conditions,
                       :insert_sql,
                       :update_sql,
                       :delete_sql,
                       :visitor,
                       :to => :master_connection
      # schema statements
      rescued_delegate :table_exists?,
                       :column_exists?,
                       :index_name_exists?,
                       :create_table,
                       :change_table,
                       :rename_table,
                       :drop_table,
                       :add_column,
                       :remove_column,
                       :remove_columns,
                       :change_column,
                       :change_column_default,
                       :rename_column,
                       :add_index,
                       :remove_index,
                       :remove_index!,
                       :rename_index,
                       :index_name,
                       :index_exists?,
                       :structure_dump,
                       :dump_schema_information,
                       :initialize_schema_migrations_table,
                       :assume_migrated_upto_version,
                       :type_to_sql,
                       :add_column_options!,
                       :distinct,
                       :add_order_by_for_association_limiting!,
                       :add_timestamps,
                       :remove_timestamps,
                       :to => :master_connection
      # no clear interface contract:
      rescued_delegate :tables,         # commented in SchemaStatements
                       :truncate_table, # monkeypatching database_cleaner gem
                       :primary_key,    # is Base#primary_key meant to be the contract?
                       :to => :master_connection
      # No need to be so picky about these methods
      rescued_delegate :add_limit_offset!, # DatabaseStatements
                       :add_lock!, #DatabaseStatements
                       :columns,
                       :table_alias_for,
                       :to => :prefer_master_connection

      # === determine read connection
      rescued_delegate :select_all,
                       :select_one,
                       :select_rows,
                       :select_value,
                       :select_values,
                       :to => :connection_for_read

      # === doesn't really matter, but must be handled by underlying adapter
      rescued_delegate *(ActiveRecord::ConnectionAdapters::Quoting.instance_methods + [{
                       :to => :current_connection }])
      # issue #4: current_database is not supported by all adapters, though
      rescued_delegate :current_database, :to => :current_connection

      # ok, we might have missed more
      def method_missing(name, *args, &blk)
        master_connection.send(name.to_sym, *args, &blk).tap do
          @logger.try(:warn, %Q{
            You called the unsupported method '#{name}' on #{self.class.name}.
            In order to help us improve master_slave_adapter, please report this
            to: https://github.com/soundcloud/master_slave_adapter/issues

            Thank you.
          })
        end
      rescue ActiveRecord::StatementInvalid => exception
        handle_error(master_connection, exception)
      end

      # UTIL ==================================================================

      def master_connection
        if circuit.tripped?
          raise MasterUnavailable
        end

        @connections[:master] ||= connect_to_master
        if @connections[:master]
          circuit.success!
          @connections[:master]
        else
          circuit.fail!
          raise MasterUnavailable
        end
      end

      def master_available?
        !@connections[:master].nil?
      end

      # Returns a random slave connection
      # Note: the method is not referentially transparent, hence the bang
      def slave_connection!
        @connections[:slaves].sample
      end

      def connections
        @connections.values.flatten.compact
      end

      def current_connection
        connection_stack.first
      end

      def current_clock
        @master_slave_clock
      end

      def master_clock
        raise NotImplementedError
      end

      def slave_clock(conn)
        raise NotImplementedError
      end

    protected

      def open_transaction?
        master_available? ? (master_connection.open_transactions > 0) : false
      end

      def connection_for_read
        open_transaction? ? master_connection : current_connection
      end

      def prefer_master_connection
        master_available? ? master_connection : slave_connection!
      end

      def master_connection?(connection)
        @connections[:master] == connection
      end

      def reset_master_connection
        @connections[:master] = nil
      end

      def slave_consistent?(conn, clock)
        if @last_seen_slave_clocks[conn].try(:>=, clock)
          true
        elsif (slave_clk = slave_clock(conn))
          @last_seen_slave_clocks[conn] = clock
          slave_clk >= clock
        else
          false
        end
      end

      def current_clock=(clock)
        @master_slave_clock = clock
      end

      def connection_stack
        @master_slave_connection ||= []
      end

      def current_connection=(conn)
        connection_stack.unshift(conn)
      end

      def on_write
        with(master_connection) do |conn|
          yield(conn).tap do
            unless open_transaction?
              master_clk = master_clock
              unless current_clock.try(:>=, master_clk)
                self.current_clock = master_clk
              end

              # keep using master after write
              connection_stack.replace([ conn ])
            end
          end
        end
      end

      def with(connection)
        self.current_connection = connection
        yield(connection).tap { connection_stack.shift if connection_stack.size > 1 }
      rescue ActiveRecord::StatementInvalid => exception
        handle_error(connection, exception)
      end

      def connect(cfg, name)
        adapter_method = "#{cfg.fetch(:adapter)}_connection".to_sym
        ActiveRecord::Base.send(adapter_method, { :name => name }.merge(cfg))
      end

      def connect_to_master
        connect(@config.fetch(:master), :master)
      rescue => exception
        if connection_error?(exception)
          @logger.try(:warn, "Can't connect to master. #{exception.message}")
          nil
        else
          raise
        end
      end

      def on_commit_callbacks
        @on_commit_callbacks ||= []
      end

      def on_rollback_callbacks
        @on_rollback_callbacks ||= []
      end

      def connection_error?(exception)
        raise NotImplementedError
      end

      def handle_error(connection, exception)
        if master_connection?(connection) && connection_error?(exception)
          reset_master_connection
          raise MasterUnavailable
        else
          raise exception
        end
      end

      def circuit
        @circuit
      end
    end
  end
end
