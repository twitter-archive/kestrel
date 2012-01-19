require "rubygems"
require "thrift"
require "thrift_client"
require "kestrel"

class KestrelClient
  DEFAULTS = { :transport_wrapper => Thrift::FramedTransport }
  DEFAULT_ITEM_TIMEOUT_MSEC = 60000

  def initialize(servers = nil, options = {})
    if servers.nil? or servers.empty?
      STDERR.puts "No servers specified, using 127.0.0.1:2229"
      servers = ['127.0.0.1:2229']
    else
      servers = Array(servers)
    end

    @client = ThriftClient.new(Kestrel::Client, servers, DEFAULTS.merge(options))
  end

  def version
    @client.get_version()
  end

  def queue(name)
    Queue.new(@client, name)
  end

  class Queue
    def initialize(client, name)
      @client = client
      @name = name
    end

    def peek
      @client.peek(@name)
    end

    def put(items, expiration_msec = 0)
      @client.put(@name, Array(items), expiration_msec)
    end

    def get(max_items = 1, timeout_msec = 0, auto_abort_msec = 0)
      @client.get(@name, max_items, timeout_msec, auto_abort_msec)
    end

    def open(max_items = 1, timeout_msec = 0)
      get(max_items, timeout_msec, DEFAULT_ITEM_TIMEOUT_MSEC)
    end

    def confirm(items)
      items = Array(items)
      case items[0]
      when Item
        @client.confirm(@name, items.map { |x| x.id })
      else
        @client.confirm(@name, items)
      end
    end

    def abort(items)
      items = Array(items)
      case items[0]
      when Item
        @client.abort(@name, items.map { |x| x.id })
      else
        @client.abort(@name, items)
      end
    end

    def flush
      @client.flush_queue(@name)
    end
  end
end
