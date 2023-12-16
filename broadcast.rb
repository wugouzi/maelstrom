#!/usr/bin/env ruby

require_relative "node.rb"
require "set"

class Broadcast
  attr_reader :node
  def initialize
    @node = Node.new
    @neighbors = []
    @lock = Mutex.new
    @messages = Set.new

    @node.on "topology" do |msg|
      @neighbors = msg[:body][:topology][@node.node_id]
      @node.log "My neighbors are #{@neighbors.inspect}"
      @node.reply! msg, type: "topology_ok"
    end

    @node.on "read" do |msg|
      @lock.synchronize do
        @node.reply! msg, type: "read_ok", messages: @messages.to_a
      end
    end

    @node.on "broadcast" do |msg|
      m = msg[:body][:message]
      @lock.synchronize do
        unless @messages.include? m
          @messages.add m

          # Gossip this message to neighbors
          @neighbors.each do |neighbor|
            @node.send! neighbor, type: "broadcast", message: m
          end
        end
      end

      # Inter-server messages don't have a msg_id, and don't need a response
      @node.reply! msg, type: "broadcast_ok" if msg[:body][:msg_id]
    end
  end
end

Broadcast.new.node.main!
