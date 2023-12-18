#!/usr/bin/env ruby

require_relative "node.rb"

class IDGen
  def initialize(node)
    @node = node
    @lock = Mutex.new
    @i = -1
  end

  # Generates a new unique ID
  def new_id
    i = @lock.synchronize { @i += 1 }
    "#{@node.node_id}-#{i}"
  end
end

class Thunk
  SVC = "lin-kv"

  def initialize(node, id, value, saved)
    @node = node
    @id = id
    @value = value
    @saved = saved
  end

  # By default, thunks serialize their entire value to/from JSON directly.
  def to_json
    @value
  end

  def from_json(json)
    json
  end

  # Returns this thunk's ID, creating a new one if necessary.
  def id
    @id ||= @idgen.new_id
  end

  # Returns the value of this thunk from the storage service.
  def value
    unless @value
      res = @node.sync_rpc! SVC, type: "read", key: id
      value = from_json res[:body][:value]
      @value ||= value
    end
    @value
  end

  # Saves this thunk to the storage service
  def save!
    unless @saved
      res = @node.sync_rpc! SVC, type: "write", key: id, value: to_json
      if res[:body][:type] == "write_ok"
        @saved = true
      else
        raise RPCError.abort "Unable to save thunk #{id}"
      end
    end
  end
end

class Map < Thunk
  def initialize(node, idgen, id, value, saved)
    super node, id, value, saved
    @idgen = idgen
  end

  # We have a list of [k, id] pairs, each id belonging to a saved Thunk.
  def from_json(json)
    pairs = json || []
    m = {}
    pairs.each { |k, id| m[k] = Thunk.new @node, id, nil, true }
    m
  end

  # Turns this Map into a JSON object. We serialize ourselves as a list of
  # [k, thunk-id] pairs, so that our keys can be any type of object, not just
  # strings.
  def to_json
    value.entries.map { |k, thunk| [k, thunk.id] }
  end

  # Saves this Map's thunks to the storage service
  def save!
    value.values.each do |thunk|
      thunk.save!
      super
    end
  end

  # Get the value of key k
  def [](k)
    if v = value[k]
      v.value
    end
  end

  # Return a copy of this Map with k = v
  def assoc(k, v)
    thunk = Thunk.new @node, @idgen.new_id, v, false
    Map.new @node, @idgen, @idgen.new_id, value.merge({ k => thunk }), false
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
  SVC = "lin-kv"
  KEY = "root"

  def initialize(node, idgen)
    @node = node
    @idgen = idgen
  end

  def transact!(txn)
    # Load the current map id from lin-kv
    map_id1 =
      @node.sync_rpc!("lin-kv", { type: "read", key: KEY })[:body][:value]

    map1 =
      if map_id1
        Map.new(@node, @idgen, map_id1, nil, true)
      else
        Map.new(@node, @idgen, @idgen.new_id, {}, false)
      end

    # Apply txn
    map2, txn2 = map1.transact txn

    # Save resulting state iff it hasn't changed
    if map1.id != map2.id
      map2.save!
      res =
        @node.sync_rpc!(
          SVC,
          {
            type: "cas",
            key: KEY,
            from: map1.id,
            to: map2.id,
            create_if_not_exists: true
          }
        )
      unless res[:body][:type] == "cas_ok"
        raise RPCError.txn_conflict "CAS failed!"
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
    idgen = IDGen.new @node
    @state = State.new @node, idgen

    @node.on "txn" do |msg|
      txn = msg[:body][:txn]
      @node.log "\nTxn: #{txn}"
      txn2 = @lock.synchronize { @state.transact! txn }
      @node.reply! msg, type: "txn_ok", txn: txn2
    end
  end
end

Transactor.new.node.main!
