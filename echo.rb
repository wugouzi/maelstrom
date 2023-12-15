#!/usr/bin/env ruby
require "json"

class EchoServer
  def initialize
    @node_id = nil
    @next_msg_id = 0
  end

  def reply!(request, body)
    id = @next_msg_id += 1
    body = body.merge msg_id: id, in_reply_to: request[:body][:msg_id]
    msg = { src: @node_id, dest: request[:src], body: body }
    # JSON.dump msg, $stderr
    JSON.dump msg, $stdout
    $stdout << "\n"
    $stdout.flush
  end

  def main!
    while line = $stdin.gets
      req = JSON.parse line, symbolize_names: true
      $stderr.puts "Received #{req.inspect}"

      body = req[:body]
      case body[:type]
      # Initialize this node
      when "init"
        @node_id = body[:node_id]
        warn "Initialized node #{@node_id}"
        reply! req, { type: "init_ok" }
      when "echo"
        warn "Echoing #{body}"
        reply! req, body.merge({ type: "echo_ok" })
      end
    end
  end
end

EchoServer.new.main!
