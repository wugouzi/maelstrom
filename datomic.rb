#!/usr/bin/env ruby

require_relative "node.rb"

class State
  def initialize
    @state = {}
  end

  def transact!(txn)
    txn2 = []
    txn.each do |op|
      f, k, v = op
      case f
      when "r"
        txn2 << [f, k, (@state[k] or [])]
      when "append"
        txn2 << op
        list = (@state[k].clone or [])
        list << v
        @state[k] = list
      end
    end
    txn2
  end
end

class Transactor
  attr_reader :node

  def initialize
    @node = Node.new
    @lock = Mutex.new
    @state = State.new

    @node.on "txn" do |msg|
      txn = msg[:body][:txn]
      @node.log "\nTxn: #{txn}"
      txn2 = @lock.synchronize { @state.transact! txn }
      @node.reply! msg, type: "txn_ok", txn: txn2
    end
  end
end

Transactor.new.node.main!
