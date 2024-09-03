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
$DEBUG = false

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
    @transactions.select { |_, tx| tx.state == TransactionState::InProgressTransaction }.keys
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

    debug("starting transaction", tx.id)

    tx
  end

  def complete_transaction(transaction, transaction_state)
    debug('completing transaction', transaction.id)

    if transaction_state == TransactionState::CommittedTransaction
      # Snapshot Isolation imposes the additional constraint that
      # no transaction A may commit after writing any of the same
      # keys as transaction B has written and committed during
      # transaction A's life.
      conflict_fn = ->(t1, t2) { (t1.writeset & t2.writeset).length > 0 }
      if transaction.isolation_level == IsolationLevel::SnapshotIsolation &&
          has_conflict(transaction, conflict_fn)
        complete_transaction(transaction, TransactionState::AbortedTransaction)
        raise RuntimeError, 'write-write conflict'
      end

      # Serializable Isolation imposes the additional constraint that
      # no transaction A may commit after reading any of the same
      # keys as transaction B has written and committed during
      # transaction A's life, or vice-versa.
      puts transaction
      conflict_fn2 = ->(t1, t2) { (t1.writeset & t2.readset).length > 0 || (t1.readset & t2.writeset).length > 0 }
      if transaction.isolation_level == IsolationLevel::SerializableIsolation &&
        has_conflict(transaction, conflict_fn2)
        complete_transaction(transaction, TransactionState::AbortedTransaction)
        raise RuntimeError, 'read-write conflict'
      end
    end

    transaction.state = transaction_state
    @transactions[transaction.id] = transaction
    nil
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

    # Read Committed means we are allowed to read any values that
    # are committed at the point in time where we read.
    if transaction.isolation_level == IsolationLevel::ReadCommittedIsolation
      # If the value was created by a transaction that is
      # not committed, and not this current transaction,
      # it's no good.
      if value.tx_start_id != transaction.id &&
      transaction_state(value.tx_start_id).state != TransactionState::CommittedTransaction
        return false
      end
      # If the value was deleted in this transaction, it's no good.
      if value.tx_end_id == transaction.id
        return false
      end

      # Or if the value was deleted in some other committed
      # transaction, it's no good.
      if value.tx_end_id > 0 &&
      transaction_state(value.tx_end_id).state == TransactionState::CommittedTransaction
        return false
      end
      # Otherwise the value is good.
      return true
    end

    # Repeatable Read, Snapshot Isolation, and Serializable
    # further restricts Read Committed so only versions from
    # transactions that completed before this one started are
    # visible.

    # Snapshot Isolation and Serializable will do additional
    # checks at commit time.
    assert(transaction.isolation_level == IsolationLevel::RepeatableReadIsolation ||
          transaction.isolation_level == IsolationLevel::SnapshotIsolation ||
          transaction.isolation_level == IsolationLevel::SerializableIsolation, "invalid isolation level")
    # Ignore values from transactions started after this one.
    if value.tx_start_id > transaction.id
      return false
    end
    # Ignore values created from transactions in progress when
    # this one started.
    if transaction.inprogress.include? value.tx_start_id
      return false
    end

    # If the value was created by a transaction that is not
    # committed, and not this current transaction, it's no good.
    if transaction_state(value.tx_start_id).state != TransactionState::CommittedTransaction &&
      value.tx_start_id != transaction.id
      return false
    end

    # If the value was deleted in this transaction, it's no good.
    if value.tx_end_id == transaction.id
      return false
    end

    # Or if the value was deleted in some other committed
    # transaction that started before this one, it's no good.
    if value.tx_end_id < transaction.id && value.tx_end_id > 0 &&
      transaction_state(value.tx_end_id).state == TransactionState::CommittedTransaction &&
      !(transaction.inprogress.include? value.tx_end_id)
        return false
    end

    return true
  end

  def has_conflict(t1, conflict_fn)

    # First see if there is any conflict with transactions that
    # were in progress when this one started.
    t1.inprogress.each do |id|
      next unless @transactions.key? id
      t2 = @transactions[id]
      if t2.state == TransactionState::CommittedTransaction
        if conflict_fn.call(t1, t2)
          return true
        end
      end
    end

    # Then see if there is any conflict with transactions that
    # started and committed after this one started.

    for id in t1.id...@next_transaction_id do
      next unless @transactions.key? id
      t2 = @transactions[id]
      if t2.state == TransactionState::CommittedTransaction
        if conflict_fn(t1, t2)
          return true
        end
      end
    end

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
    when 'commit'
      @db.assert_valid_transaction(@tx)
      @db.complete_transaction(@tx, TransactionState::CommittedTransaction)
      @tx = nil
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
        raise RuntimeError, 'cannot delete key that does not exist'
      end
      @tx.writeset.add(key)
      # And add a new version if it's a set command.
      if command == "set"
        value = args[1]
        @db.store[key] << Value.new(@tx.id, 0, value)
      end
      # Delete ok.
    when 'get'
      @db.assert_valid_transaction(@tx)
      key = args[0]
      @tx.readset.add(key)
      @db.store[key].reverse_each do |value|
        debug(value, @tx, @db.visible?(@tx, value))
        return value.value if @db.visible?(@tx, value)
      end
      raise RuntimeError, 'cannot get key that does not exist'
    else
      ''
    end
  end

  def must_exec_command(cmd, *args)
    exec_command(cmd, *args)
  end
end
