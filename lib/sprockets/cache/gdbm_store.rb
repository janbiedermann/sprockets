require 'gdbm'
require 'logger'
require 'sprockets/encoding_utils'
require 'sprockets/path_utils'

module Sprockets
  class Cache
    # Public: A file system cache store that automatically cleans up old keys.
    #
    # Assign the instance to the Environment#cache.
    #
    #     environment.cache = Sprockets::Cache::FileStore.new("/tmp")
    #
    # See Also
    #
    #   ActiveSupport::Cache::FileStore
    #
    class GdbmStore
      # Internal: Default key limit for store.
      DEFAULT_MAX_SIZE = 10000

      # Internal: Default standard error fatal logger.
      #
      # Returns a Logger.
      def self.default_logger
        logger = Logger.new($stderr)
        logger.level = Logger::FATAL
        logger
      end

      # Public: Initialize the cache store.
      #
      # root     - A String path to a directory to persist cached values to.
      # max_size - A Integer of the maximum number of keys the store will hold.
      #            (default: 1000).
      def initialize(root, max_size = DEFAULT_MAX_SIZE, logger = self.class.default_logger)
        start_time = Time.now
        @root = root
        @logger = logger
        @gdbm = GDBM.open(File.join(root, 'sprockets_cache.gdbm'))
        at_exit do
          @gdbm.close
        end
        @max_size = max_size
        @gc_size = max_size * 0.90
        @hash_cache = {}
        @access_cache = {}
        @gdbm.to_hash.each do |k, v|
          key = Marshal.load(k)
          @hash_cache[key] = Marshal.load(v)
          @access_cache[key] = start_time
        end
        gc! if @hash_cache.size > @max_size
        load_time = Time.now.to_f - start_time.to_f
        puts "Sprockets GDBM Cache - max entries: #{@max_size}, current entries: #{@hash_cache.size}, load time: #{(load_time * 1000).to_i}ms"
      end

      # Public: Retrieve value from cache.
      #
      # This API should not be used directly, but via the Cache wrapper API.
      #
      # key - String cache key.
      #
      # Returns Object or nil or the value is not set.
      def get(key)
        value = @hash_cache[key]
        @access_cache[key] = Time.now

        if value.nil?
          str = @gdbm[Marshal.dump(key)]
          if str
            major, minor = str[0], str[1]
            if major && major.ord == Marshal::MAJOR_VERSION &&
              minor && minor.ord <= Marshal::MINOR_VERSION
              value = Marshal.load(str)
            end
          end
          @hash_cache[key] = value unless value.nil?
        end
        value
      end

      # Public: Set a key and value in the cache.
      #
      # This API should not be used directly, but via the Cache wrapper API.
      #
      # key   - String cache key.
      # value - Object value.
      #
      # Returns Object value.
      def set(key, value)
        @gdbm[Marshal.dump(key)] = Marshal.dump(value)
        @hash_cache[key] = value
        @access_cache[key] = Time.now

        # GC if necessary
        gc! if @hash_cache.size > @max_size

        value
      end

      # Public: Pretty inspect
      #
      # Returns String.
      def inspect
        "#<#{self.class} size=#{@hash_cache.size}/#{@max_size}>"
      end

      private

      def gc!
        start_time = Time.now
        before_size = @hash_cache.size

        sorted_kv = @access_cache.sort_by { |k,v| v.to_i }
        sorted_kv.reverse!

        while sorted_kv.size > @gc_size
          kv = sorted_kv.pop
          @gdbm.delete(Marshal.dump(kv[0]))
          @hash_cache.delete(kv[0])
          @access_cache.delete(kv[0])
        end
        after_size = @hash_cache.size

        @logger.warn do
          time_diff = Time.now - start_time
          "Sprockets GDBM Cache [#{File.join(@root, 'sprockets_cache.gdbm')}] garbage collected " +
            "#{before_size - after_size} entries (#{(time_diff * 1000).to_i}ms)"
        end
      end
    end
  end
end
