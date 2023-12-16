#!/usr/bin/env ruby

require_relative "node.rb"

class Map
  def initialize(map = {})
    @map = map
  end

  # To re-inflate a Map from a JSON representation, we construct a Hashmap of
  # pairs.
  def self.from_json(json)
    if json
      Map.new Hash[json]
    else
      Map.new {}
    end
  end

  # Turns this Map into a JSON object. We serialize ourselves as a list of
  # pairs, so that our keys can be any type of object, not just strings.
  def to_json
    @map.entries
  end

  # Get the value of key k
  def [](k)
    @map[k]
  end

  # Return a copy of this Map with k = v
  def assoc(k, v)
    Map.new @map.merge({ k => v })
  end

  # Applies txn to this Map, returning a tuple of the resulting Map and the
  # completed transaction.
  def transact(txn)
    txn2 = []
    map2 =
      txn.reduce(self) do |m, op|
        f, k, v = op
        case f
        when "r"
          txn2 << [f, k, m[k]]
          m
        when "append"
          txn2 << op
          list = (m[k].clone or [])
          list << v
          m.assoc k, list
        end
      end

    [map2, txn2]
  end
end

class State
  KEY = "root"
  def initialize(node)
    @node = node
  end

  def transact!(txn)
    # Load the current value from lin-kv
    map1 =
      Map.from_json(
        @node.sync_rpc!("lin-kv", { type: "read", key: KEY })[:body][:value]
      )

    # Apply txn
    map2, txn2 = map1.transact txn

    # Save resulting state iff it hasn't changed
    res =
      @node.sync_rpc!(
        "lin-kv",
        {
          type: "cas",
          key: KEY,
          from: map1.to_json,
          to: map2.to_json,
          create_if_not_exists: true
        }
      )
    unless res[:body][:type] == "cas_ok"
      raise RPCError.txn_conflict "CAS failed!"
    end

    txn2
  end
end

class Transactor
  attr_reader :node

  def initialize
    @node = Node.new
    @lock = Mutex.new
    @state = State.new @node

    @node.on "txn" do |msg|
      txn = msg[:body][:txn]
      @node.log "\nTxn: #{txn}"
      txn2 = @lock.synchronize { @state.transact! txn }
      @node.reply! msg, type: "txn_ok", txn: txn2
    end
  end
end

Transactor.new.node.main!
