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
Transaction = Data.define(:isolation_level, :id, :state, :inprogress, :writeset, :readset)

class Database
  attr_reader :default_isolation, :store, :transactions, :next_transaction_id
  # attr_accessor :store
  def initialize(default_isolation)
    @default_isolation = default_isolation
    @store = Hash.new
    @transactions = Hash.new
    @next_transaction_id = 1
  end
end
