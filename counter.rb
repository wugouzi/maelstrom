#!/usr/bin/env ruby

require_relative "node.rb"
require "set"

class GSet
  attr_reader :set

  def initialize(set = Set.new)
    @set = set
  end

  # Returns a new GSet from a JSON representation
  def from_json(json_array)
    GSet.new json_array.to_set
  end

  # Turns a GSet into a JSON representation: an array.
  def to_json
    @set.to_a
  end

  # Returns a JSON-serializable representation of the effective value of this
  # GSet.
  def read
    @set.to_a
  end

  # Merging two GSets means taking the set union of their sets.
  def merge(other)
    GSet.new @set.merge(other.set)
  end

  # Returns a new GSet with this element added.
  def add(element)
    GSet.new(@set | [element])
  end
end

class GCounter
  attr_reader :counters

  def initialize(counters = {})
    @counters = counters
  end

  def from_json(json)
    GCounter.new json
  end

  def to_json
    @counters
  end

  # The effective value of a G-counter is the sum of its values.
  def read
    @counters.values.reduce(0) { |sum, x| sum + x }
  end

  # Merging two G-counters means taking the maxima of corresponding hash
  # elements.
  def merge(other)
    GCounter.new(@counters.merge(other.counters) { |k, v1, v2| [v1, v2].max })
  end

  # Adding a value to a counter means incrementing the value for this
  # node_id.
  def add(node_id, delta)
    counters = @counters.dup
    counters[node_id] = (counters[node_id] || 0) + delta
    GCounter.new counters
  end
end

class PNCounter
  attr_reader :inc, :dec
  def initialize(inc = GCounter.new, dec = GCounter.new)
    @inc = inc
    @dec = dec
  end

  def from_json(json)
    PNCounter.new(@inc.from_json(json["inc"]), @dec.from_json(json["dec"]))
  end

  def to_json
    { inc: @inc.to_json, dec: @dec.to_json }
  end

  def read
    @inc.read - @dec.read
  end

  def merge(other)
    PNCounter.new @inc.merge(other.inc), @dec.merge(other.dec)
  end

  def add(node_id, delta)
    if 0 <= delta
      PNCounter.new @inc.add(node_id, delta), @dec
    else
      PNCounter.new @inc, @dec.add(node_id, -delta)
    end
  end
end

class CounterServer
  attr_reader :node
  def initialize(set = Set.new)
    @node = Node.new
    @lock = Mutex.new
    @crdt = PNCounter.new

    @node.on "add" do |msg|
      @lock.synchronize { @crdt = @crdt.add(@node.node_id, msg[:body][:delta]) }
      @node.reply! msg, type: "add_ok"
    end

    @node.on "read" do |msg|
      @node.reply! msg, type: "read_ok", value: @crdt.read
    end

    @node.on "replicate" do |msg|
      other = @crdt.from_json(msg[:body][:value])
      @lock.synchronize { @crdt = @crdt.merge(other) }
    end

    @node.every 5 do
      @node.log "Replicating current value #{@crdt.to_json}"
      @node.node_ids.each do |n|
        # Don't replicate to self
        unless n == @node.node_id
          @node.send! n, type: "replicate", value: @crdt.to_json
        end
      end
    end
  end
end

CounterServer.new.node.main!
