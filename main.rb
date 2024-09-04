#!/usr/bin/ruby
require 'optparse'
require 'set'

module Helper
  def assert(condition, msg)
    raise RuntimeError, msg unless condition
  end

  def assert_eq(a, b, prefix)
    raise RuntimeError, "#{prefix}: '#{a}' != '#{b}'" unless a == b
  end
end

params = {}
OptionParser.new do |opts|
  opts.on('-d', '--debug')
end.parse!(into: params)

$DEBUG = params[:debug]
# $DEBUG = false

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
  include Helper
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
      handle_conflict(transaction)
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
    assert(transaction.id.positive?, 'valid id')
    assert(transaction_state(transaction.id).state == TransactionState::InProgressTransaction, 'in progress')
  end

  def visible?(transaction, value)
    case transaction.isolation_level
    when IsolationLevel::ReadUncommittedIsolation
      # Read Uncommitted means we simply read the last value
      # written. Even if the transaction that wrote this value has
      # not committed, and even if it has aborted.
      value.tx_end_id.zero?
    when IsolationLevel::ReadCommittedIsolation
      # Read Committed means we are allowed to read any values that
      # are committed at the point in time where we read.
      read_committed_visible?(transaction, value)
    else
      # Repeatable Read, Snapshot Isolation, and Serializable
      # further restricts Read Committed so only versions from
      # transactions that completed before this one started are
      # visible.
      #
      # Snapshot Isolation and Serializable will do additional
      # checks at commit time.
      repeatable_read_visible?(transaction, value)
    end
  end

  def has_conflict(t1, conflict_fn)
    # First see if there is any conflict with transactions that
    # were in progress when this one started.
    in_progress_conflict?(t1, conflict_fn) ||
      # Then see if there is any conflict with transactions that
      # started and committed after this one started.
      committed_after_conflict?(t1, conflict_fn)
  end

  def new_connection = Connection.new(self, nil)

  private

  def handle_conflict(transaction)
    # Snapshot Isolation imposes the additional constraint that
    # no transaction A may commit after writing any of the same
    # keys as transaction B has written and committed during
    # transaction A's life.
    if transaction.isolation_level == IsolationLevel::SnapshotIsolation
      snapshot_conflict_fn = ->(t1, t2) { (t1.writeset & t2.writeset).any? }
      if has_conflict(transaction, snapshot_conflict_fn)
        abort_transaction(transaction, 'write-write conflict')
      end
    end

    # Serializable Isolation imposes the additional constraint that
    # no transaction A may commit after reading any of the same
    # keys as transaction B has written and committed during
    # transaction A's life, or vice-versa.
    if transaction.isolation_level == IsolationLevel::SerializableIsolation
      serializable_conflict_fn = ->(t1, t2) { (t1.writeset & t2.readset).any? || (t1.readset & t2.writeset).any? }
      if has_conflict(transaction, serializable_conflict_fn)
        abort_transaction(transaction, 'read-write conflict')
      end
    end
  end

  def abort_transaction(transaction, message)
    complete_transaction(transaction, TransactionState::AbortedTransaction)
    raise RuntimeError, message
  end

  def read_committed_visible?(transaction, value)
    # If the value was created by a transaction that is
    # not committed, and not this current transaction,
    # it's no good.
    return false if value.tx_start_id != transaction.id &&
                    transaction_state(value.tx_start_id).state != TransactionState::CommittedTransaction

    # If the value was deleted in this transaction, it's no good.
    return false if value.tx_end_id == transaction.id

    # Or if the value was deleted in some other committed
    # transaction, it's no good.
    return false if value.tx_end_id.positive? &&
                    transaction_state(value.tx_end_id).state == TransactionState::CommittedTransaction

    # Otherwise the value is good.
    true
  end

  def repeatable_read_visible?(transaction, value)
    # Ignore values from transactions started after this one.
    return false if value.tx_start_id > transaction.id

    # Ignore values created from transactions in progress when
    # this one started.
    return false if transaction.inprogress.include?(value.tx_start_id)

    # If the value was created by a transaction that is not
    # committed, and not this current transaction, it's no good.
    return false if transaction_state(value.tx_start_id).state != TransactionState::CommittedTransaction &&
                    value.tx_start_id != transaction.id

    # If the value was deleted in this transaction, it's no good.
    return false if value.tx_end_id == transaction.id

    # Or if the value was deleted in some other committed
    # transaction that started before this one, it's no good.
    return false if value.tx_end_id < transaction.id && value.tx_end_id.positive? &&
                    transaction_state(value.tx_end_id).state == TransactionState::CommittedTransaction &&
                    !transaction.inprogress.include?(value.tx_end_id)

    true
  end

  def in_progress_conflict?(t1, conflict_fn)
    t1.inprogress.any? do |id|
      @transactions.key?(id) && @transactions[id].state == TransactionState::CommittedTransaction && conflict_fn.call(t1, @transactions[id])
    end
  end

  def committed_after_conflict?(t1, conflict_fn)
    (t1.id...@next_transaction_id).any? do |id|
      @transactions.key?(id) && @transactions[id].state == TransactionState::CommittedTransaction && conflict_fn.call(t1, @transactions[id])
    end
  end
end

class Connection
  include Helper
  attr_reader :db, :tx

  def initialize(db, tx)
    @db = db
    @tx = tx
  end

  def exec_command(command, *args)
    debug(command, args)

    case command
    when 'begin' then begin_transaction
    when 'abort' then abort_transaction
    when 'commit' then commit_transaction
    when 'set' then set_value(*args)
    when 'delete' then delete_value(*args)
    when 'get' then get_value(*args)
    else raise RuntimeError, 'unsupported command'
    end
  end

  private

  def begin_transaction
    assert_eq(@tx, nil, 'no running transaction')
    @tx = @db.new_transaction
    @db.assert_valid_transaction(@tx)
    @tx.id
  end

  def abort_transaction
    @db.assert_valid_transaction(@tx)
    @db.complete_transaction(@tx, TransactionState::AbortedTransaction)
    @tx = nil
  end

  def commit_transaction
    @db.assert_valid_transaction(@tx)
    @db.complete_transaction(@tx, TransactionState::CommittedTransaction)
    @tx = nil
  end

  def set_value(key, value)
    handle_write_operation(key, 'set') do
      @db.store[key] << Value.new(@tx.id, 0, value)
    end
  end

  def delete_value(key)
    handle_write_operation(key, 'delete') do
      raise RuntimeError, 'cannot delete key that does not exist' unless @db.store[key].any?
    end
  end

  def get_value(key)
    @db.assert_valid_transaction(@tx)
    @tx.readset.add(key)
    @db.store[key].reverse_each do |value|
      debug(value, @tx, @db.visible?(@tx, value))
      return value.value if @db.visible?(@tx, value)
    end
    raise RuntimeError, 'cannot get key that does not exist'
  end

  def handle_write_operation(key, operation)
    @db.assert_valid_transaction(@tx)
    found = false

    @db.store[key].reverse_each do |value|
      debug(value, @tx, @db.visible?(@tx, value))
      if @db.visible?(@tx, value)
        value.tx_end_id = @tx.id
        found = true
      end
    end

    yield if block_given?

    @tx.writeset.add(key)

    raise RuntimeError, "cannot #{operation} key that does not exist" if operation == 'delete' && !found
  end
end
