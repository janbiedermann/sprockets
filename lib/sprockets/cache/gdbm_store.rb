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
      DEFAULT_MAX_SIZE = 25 * 1024 * 1024

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
        @gdbm = GDBM.open(File.join(root, 'sprockets_cache.gdbm'))
        @max_size = max_size
        @gc_size = max_size * 0.90
        @hash_cache = Cache.new(Hash.new)
        @logger = logger
        @size = @gdbm.size
      end

      # Public: Retrieve value from cache.
      #
      # This API should not be used directly, but via the Cache wrapper API.
      #
      # key - String cache key.
      #
      # Returns Object or nil or the value is not set.
      def get(key)
        value = @hash_cache.get(key)

        if value.nil?
          str = @gdbm[key.to_s]
          if str
            major, minor = str[0], str[1]
            if major && major.ord == Marshal::MAJOR_VERSION &&
              minor && minor.ord <= Marshal::MINOR_VERSION
              value = Marshal.load(str)
            end
          end
          @hash_cache.set(key, value) unless value.nil?
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
        return value unless value
        @gdbm[key.to_s] = Marshal.dump(value)
        @hash_cache.set(key, value)

        # GC if necessary
        # gc! if size > @max_size

        value
      end

      # Public: Pretty inspect
      #
      # Returns String.
      def inspect
        "#<#{self.class} size=#{size}/#{@max_size}>"
      end

      private

      # Internal: Expand object cache key into a short String key.
      #
      # The String should be under 250 characters so its compatible with
      # Memcache.
      #
      # key - JSON serializable key
      #
      # Returns a String with a length less than 250 characters.
      def expand_key(key)
        digest_key = DigestUtils.pack_urlsafe_base64digest(DigestUtils.digest(key))
        namespace = digest_key[0, 2]
        "sprockets/v#{VERSION}/#{namespace}/#{digest_key}"
      end

      def size
        @gdbm.size
      end

      def gc!
        # start_time = Time.now
        #
        # caches = find_caches
        # size = compute_size(caches)
        #
        # delete_caches, keep_caches = caches.partition { |_, stat|
        #   deleted = size > @gc_size
        #   size -= stat.size
        #   deleted
        # }
        #
        # return if delete_caches.empty?
        #
        # FileUtils.remove(delete_caches.map(&:first), force: true)
        # @size = compute_size(keep_caches)
        #
        # @logger.warn do
        #   secs = Time.now.to_f - start_time.to_f
        #   "#{self.class}[#{@root}] garbage collected " +
        #     "#{delete_caches.size} files (#{(secs * 1000).to_i}ms)"
        # end
      end
    end
  end
end
