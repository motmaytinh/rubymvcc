#!/usr/bin/env ruby
require 'optparse'
require 'set'

module Helper
  def assert(condition, msg)
    raise msg unless condition
  end

  def assert_eq(a, b, prefix)
    raise "#{prefix}: '#{a}' != '#{b}'" unless a == b
  end
end

$DEBUG = false
OptionParser.new do |opts|
  opts.on('-d', '--debug', 'Enable debug mode') { $DEBUG = true }
end.parse!

# $DEBUG = false

def debug(*args)
  puts "[DEBUG] #{args.join(' ')}" if $DEBUG
end

module TransactionState
  IN_PROGRESS = :in_progress
  ABORTED = :aborted
  COMMITTED = :committed
end

module IsolationLevel
  READ_UNCOMMITTED = :read_uncommitted
  READ_COMMITTED = :read_committed
  REPEATABLE_READ = :repeatable_read
  SNAPSHOT = :snapshot
  SERIALIZABLE = :serializable
end

Value = Struct.new(:tx_start_id, :tx_end_id, :value)
Transaction = Struct.new(:isolation_level, :id, :state, :in_progress, :write_set, :read_set)

class Database
  include Helper
  attr_reader :default_isolation, :store, :transactions, :next_transaction_id

  def initialize(default_isolation)
    @default_isolation = default_isolation
    @store = Hash.new { |h, k| h[k] = [] }
    @transactions = {}
    @next_transaction_id = 1
  end

  def in_progress
    @transactions.select { |_, tx| tx.state == TransactionState::IN_PROGRESS }.keys
  end

  def new_transaction
    tx = Transaction.new(
      @default_isolation,
      @next_transaction_id,
      TransactionState::IN_PROGRESS,
      in_progress,
      Set.new,
      Set.new
    )

    @transactions[tx.id] = tx
    @next_transaction_id += 1

    debug("Starting transaction", tx.id)
    tx
  end

  def complete_transaction(transaction, transaction_state)
    debug('Completing transaction', transaction.id)

    if transaction_state == TransactionState::COMMITTED
      handle_conflict(transaction)
    end

    transaction.state = transaction_state
    @transactions[transaction.id] = transaction
    nil
  end

  def transaction_state(tx_id)
    tx = @transactions[tx_id]
    assert(tx, 'Valid transaction')
    tx
  end

  def assert_valid_transaction(transaction)
    assert(transaction.id.positive?, 'Valid id')
    assert(transaction_state(transaction.id).state == TransactionState::IN_PROGRESS, 'In progress')
  end

  def visible?(transaction, value)
    case transaction.isolation_level
    when IsolationLevel::READ_UNCOMMITTED
      # Read Uncommitted means we simply read the last value
      # written. Even if the transaction that wrote this value has
      # not committed, and even if it has aborted.
      value.tx_end_id.zero?
    when IsolationLevel::READ_COMMITTED
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

  def conflict?(t1, conflict_fn)
    # First see if there is any conflict with transactions that
    # were in progress when this one started.
    in_progress_conflict?(t1, conflict_fn) ||
      # Then see if there is any conflict with transactions that
      # started and committed after this one started.
      committed_after_conflict?(t1, conflict_fn)
  end

  def new_connection
    Connection.new(self)
  end

  private

  def handle_conflict(transaction)
    # Snapshot Isolation imposes the additional constraint that
    # no transaction A may commit after writing any of the same
    # keys as transaction B has written and committed during
    # transaction A's life.
    if transaction.isolation_level == IsolationLevel::SNAPSHOT
      abort_transaction(transaction, 'Write-write conflict') if snapshot_conflict?(transaction)
    end

    # Serializable Isolation imposes the additional constraint that
    # no transaction A may commit after reading any of the same
    # keys as transaction B has written and committed during
    # transaction A's life, or vice-versa.
    if transaction.isolation_level == IsolationLevel::SERIALIZABLE
      abort_transaction(transaction, 'Read-write conflict') if serializable_conflict?(transaction)
    end
  end

  def abort_transaction(transaction, message)
    complete_transaction(transaction, TransactionState::ABORTED)
    raise message
  end

  def read_committed_visible?(transaction, value)
    # If the value was created by a transaction that is
    # not committed, and not this current transaction,
    # it's no good.
    return false if value.tx_start_id != transaction.id &&
                    transaction_state(value.tx_start_id).state != TransactionState::COMMITTED

    # If the value was deleted in this transaction, it's no good.
    return false if value.tx_end_id == transaction.id

    # Or if the value was deleted in some other committed
    # transaction, it's no good.
    return false if value.tx_end_id.positive? &&
                    transaction_state(value.tx_end_id).state == TransactionState::COMMITTED

    # Otherwise the value is good.
    true
  end

  def repeatable_read_visible?(transaction, value)
    # Ignore values from transactions started after this one.
    return false if value.tx_start_id > transaction.id

    # Ignore values created from transactions in progress when
    # this one started.
    return false if transaction.in_progress.include?(value.tx_start_id)

    # If the value was created by a transaction that is not
    # committed, and not this current transaction, it's no good.
    return false if transaction_state(value.tx_start_id).state != TransactionState::COMMITTED &&
                    value.tx_start_id != transaction.id

    # If the value was deleted in this transaction, it's no good.
    return false if value.tx_end_id == transaction.id

    # Or if the value was deleted in some other committed
    # transaction that started before this one, it's no good.
    return false if value.tx_end_id < transaction.id && value.tx_end_id.positive? &&
                    transaction_state(value.tx_end_id).state == TransactionState::COMMITTED &&
                    !transaction.in_progress.include?(value.tx_end_id)

    true
  end

  def snapshot_conflict?(transaction)
    conflict?(transaction, ->(t1, t2) { (t1.write_set & t2.write_set).any? })
  end

  def serializable_conflict?(transaction)
    conflict?(transaction, ->(t1, t2) { (t1.write_set & t2.read_set).any? || (t1.read_set & t2.write_set).any? })
  end

  def in_progress_conflict?(t1, conflict_fn)
    t1.in_progress.any? do |id|
      tx = @transactions[id]
      tx && tx.state == TransactionState::COMMITTED && conflict_fn.call(t1, tx)
    end
  end

  def committed_after_conflict?(t1, conflict_fn)
    (t1.id...@next_transaction_id).any? do |id|
      tx = @transactions[id]
      tx && tx.state == TransactionState::COMMITTED && conflict_fn.call(t1, tx)
    end
  end
end

class Connection
  include Helper

  def initialize(db)
    @db = db
    @tx = nil
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
    else raise 'Unsupported command'
    end
  end

  private

  def begin_transaction
    assert_eq(@tx, nil, 'No running transaction')
    @tx = @db.new_transaction
    @db.assert_valid_transaction(@tx)
    @tx.id
  end

  def abort_transaction
    @db.assert_valid_transaction(@tx)
    @db.complete_transaction(@tx, TransactionState::ABORTED)
    @tx = nil
  end

  def commit_transaction
    @db.assert_valid_transaction(@tx)
    @db.complete_transaction(@tx, TransactionState::COMMITTED)
    @tx = nil
  end

  def set_value(key, value)
    handle_write_operation(key, 'set') do
      @db.store[key] << Value.new(@tx.id, 0, value)
    end
  end

  def delete_value(key)
    handle_write_operation(key, 'delete') do
      raise 'Cannot delete key that does not exist' if @db.store[key].empty?
    end
  end

  def get_value(key)
    @db.assert_valid_transaction(@tx)
    @tx.read_set.add(key)
    @db.store[key].reverse_each do |value|
      debug(value, @tx, @db.visible?(@tx, value))
      return value.value if @db.visible?(@tx, value)
    end
    raise 'Cannot get key that does not exist'
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

    @tx.write_set.add(key)

    raise "Cannot #{operation} key that does not exist" if operation == 'delete' && !found
  end
end
