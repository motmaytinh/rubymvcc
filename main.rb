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

$DEBUG = params[:debug]

def debug(*a)
  if !DEBUG
    return
  end
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

Value = Data.define(:tx_start_id, :tx_end_id, :value)
Transaction = Struct.new(:isolation_level, :id, :state, :inprogress, :writeset, :readset)

class Database
  attr_reader :default_isolation, :store, :transactions, :next_transaction_id

  def initialize(default_isolation)
    @default_isolation = default_isolation
    @store = {}
    @transactions = {}
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
      {},
      {}
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
    assert(transaction_state(transaction.id).state == TransactionState::InProgressTransaction)
  end

  def new_connection = Connection.new(self, nil)
end

Connection = Struct.new(:tx, :db) do
  def exec_command(cmd, *args)
    debug(command, args)

    # TODO
    ''
  end

  def must_exec_command(cmd, args)
    res = exec_command(cmd, args)
    assert_eq(res, '', 'unexpected error')
    res
  end
end
