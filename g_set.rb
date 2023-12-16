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

class GSetServer
  attr_reader :node
  def initialize(set = Set.new)
    @node = Node.new
    @lock = Mutex.new
    @crdt = GSet.new

    @node.on "read" do |msg|
      @node.reply! msg, type: "read_ok", value: @crdt.read
    end

    @node.on "add" do |msg|
      @lock.synchronize { @crdt = @crdt.add msg[:body][:element] }
      @node.reply! msg, type: "add_ok"
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

GSetServer.new.node.main!
