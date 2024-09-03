#!/usr/bin/ruby
require 'optparse'
require 'set'

def assert(condition, msg)
  raise RuntimeError, msg unless condition
end

def assert_eq(a, b, prefix)
  raise RuntimeError, "#{prefix}: '#{a}' != '#{b}'" unless a == b
end

params = {}
OptionParser.new do |opts|
  opts.on('-d', '--debug')
end.parse!(into: params)

# $DEBUG = params[:debug]
$DEBUG = true

def debug(*a)
  if !$DEBUG
    return
  end

  puts "[DEBUG] #{a}"
end

module TransactionState
  InProgressTransaction = :inprogress_tx
  AbortedTransaction = :aborted_tx
  CommittedTransaction = :commited_tx
end

module IsolationLevel
  ReadUncommittedIsolation = :read_uncommitted
  ReadCommittedIsolation = :read_commited
  RepeatableReadIsolation = :repeatable_read
  SnapshotIsolation = :snapshot
  SerializableIsolation = :serializable
end

Value = Struct.new(:tx_start_id, :tx_end_id, :value)
Transaction = Struct.new(:isolation_level, :id, :state, :inprogress, :writeset, :readset)

class Database
  attr_reader :default_isolation, :store, :transactions, :next_transaction_id

  def initialize(default_isolation)
    @default_isolation = default_isolation
    @store = Hash.new { |h, k| h[k] = [] }
    @transactions = Hash.new { |h, k| h[k] = [] }
    @next_transaction_id = 1
  end

  def inprogress
    transactions.select { |_, tx| tx.state == TransactionState::InProgressTransaction }.keys
  end

  def new_transaction
    tx = Transaction.new(
      @default_isolation,
      @next_transaction_id,
      TransactionState::InProgressTransaction,
      inprogress,
      Set.new,
      Set.new
    )

    @transactions[tx.id] = tx
    @next_transaction_id += 1

    # debug("starting transaction", tx.id)

    tx
  end

  def complete_transaction(transaction, transaction_state)
    debug('completing transaction', transaction.id)

    transaction.state = transaction_state
    @transactions[transaction.id] = transaction
  end

  def transaction_state(tx_id)
    tx = @transactions[tx_id]
    assert(tx, 'valid transaction')
    tx
  end

  def assert_valid_transaction(transaction)
    assert(transaction.id > 0, 'valid id')
    assert(transaction_state(transaction.id).state == TransactionState::InProgressTransaction, 'in progress')
  end

  def visible?(transaction, value)
    # Read Uncommitted means we simply read the last value
    # written. Even if the transaction that wrote this value has
    # not committed, and even if it has aborted.
    if transaction.isolation_level == IsolationLevel::ReadUncommittedIsolation
      # We must merely make sure the value has not been deleted.
      return value.tx_end_id == 0
    end

    assert(false, "unsupported isolation level")
    return false
  end

  def new_connection = Connection.new(self, nil)
end

class Connection
  attr_reader :db, :tx

  def initialize(db, tx)
    @tx = tx
    @db = db
  end

  def exec_command(command, *args)
    debug(command, args)

    case command
    when 'begin'
      assert_eq(@tx, nil, 'no running transaction')
      @tx = @db.new_transaction
      @db.assert_valid_transaction(@tx)
      @tx.id
    when 'abort'
      @db.assert_valid_transaction(@tx)
      @db.complete_transaction(@tx, TransactionState::AbortedTransaction)
      @tx = nil
      ''
    when 'commit'
      @db.assert_valid_transaction(@tx)
      @db.complete_transaction(@tx, TransactionState::CommittedTransaction)
      @tx = nil
      ''
    when 'set', 'delete'
      @db.assert_valid_transaction(@tx)
      key = args[0]
      found = false
      @db.store[key].reverse_each do |value|
        debug(value, @tx, @db.visible?(@tx, value))
        if @db.visible?(@tx, value)
          value.tx_end_id = @tx.id
          found = true
        end
      end
      if command == 'delete' and !found
        puts 'cannot delete key that does not exist'
        return nil
      end
      @tx.writeset.add(key)
      # And add a new version if it's a set command.
      if command == "set"
        value = args[1]
        @db.store[key] << Value.new(@tx.id, 0, value)
      end
      # Delete ok.
      ''
    when 'get'
      @db.assert_valid_transaction(@tx)
      key = args[0]
      @tx.readset.add(key)
      @db.store[key].reverse_each do |value|
        debug(value, @tx, @db.visible?(@tx, value))
        return value.value if @db.visible?(@tx, value)
      end
      nil
    else
      ''
    end
  end

  def must_exec_command(cmd, *args)
    res = exec_command(cmd, *args)
    # assert_eq(res, '', 'unexpected error')
    res
  end
end
