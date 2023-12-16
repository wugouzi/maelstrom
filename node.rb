require "json"

class Node
  attr_reader :node_id, :node_ids

  def initialize
    @node_id = nil
    @node_ids = nil
    @next_msg_id = 0
    @lock = Monitor.new
    @log_lock = Mutex.new

    @handlers = {}
    @callbacks = {}
    @periodic_tasks = []

    # Register an initial handler for the init message
    on "init" do |msg|
      # Set our node ID and IDs
      @node_id = msg[:body][:node_id]
      @node_ids = msg[:body][:node_ids]

      reply! msg, type: "init_ok"
      log "Node #{@node_id} initialized"

      # Spawn periodic task handlers
      start_periodic_tasks!
    end
  end

  # Send an async RPC request. Invokes block with response message once one
  # arrives.
  def rpc!(dest, body, &handler)
    @lock.synchronize do
      msg_id = @next_msg_id += 1
      @callbacks[msg_id] = handler
      body = body.merge({ msg_id: msg_id })
      send! dest, body
    end
  end

  # Writes a message to stderr
  def log(message)
    @log_lock.synchronize do
      warn message
      $stderr.flush
    end
  end

  # Send a body to the given node id. Fills in src with our own node_id.
  def send!(dest, body)
    msg = { dest: dest, src: @node_id, body: body }
    @lock.synchronize do
      log "Sent #{msg.inspect}"
      JSON.dump msg, $stdout
      $stdout << "\n"
      $stdout.flush
    end
  end

  # Reply to a request with a response body
  def reply!(req, body)
    body = body.merge({ in_reply_to: req[:body][:msg_id] })
    send! req[:src], body
  end

  # Turns a line of STDIN into a message hash
  def parse_msg(line)
    msg = JSON.parse line
    msg.transform_keys!(&:to_sym)
    msg[:body].transform_keys!(&:to_sym)
    msg
  end

  # Register a new message type handler
  def on(type, &handler)
    raise "Already have a handler for #{type}!" if @handlers[type]

    @handlers[type] = handler
  end

  # Periodically evaluates block every dt seconds with the node lock
  # held--helpful for building periodic replication tasks, timeouts, etc.
  def every(dt, &block)
    @periodic_tasks << { dt: dt, f: block }
  end

  # Launches threads to process periodic handlers
  def start_periodic_tasks!
    @periodic_tasks.each do |task|
      Thread.new do
        loop do
          task[:f].call
          sleep task[:dt]
        end
      end
    end
  end

  # Loops, processing messages from STDIN
  def main!
    while line = STDIN.gets
      msg = parse_msg line
      log "Received #{msg.inspect}"

      handler = nil
      @lock.synchronize do
        if handler = @callbacks[msg[:body][:in_reply_to]]
          @callbacks.delete msg[:body][:in_reply_to]
        elsif handler = @handlers[msg[:body][:type]]
        else
          raise "No handler for #{msg.inspect}"
        end
      end

      # Actually handle message
      Thread.new(handler, msg) do |handler, msg|
        begin
          handler.call msg
        rescue => e
          log "Exception handling #{msg}:\n#{e.full_message}"
        end
      end
    end
  end
end
