## CRD-operations-of-a-file-based-key-value-data-store-in-ruby
# DataStore -- File based key-value data store for ruby.
# It can store the JSON objects(value) in to the data store by name(key).

require 'fileutils'
require 'json'
require 'time'
require 'rubygems'

class DataStore

  # Error messages thrown by all DataStore methods.
  class Error < StandardError
  end
  
  # Datastore can be initialized using an optional file path. If one is not provided, it  
  # will reliably create itself in a reasonable location on the laptop.
  def initialize(file_name=nil)
    # Mutex can be used to coordinate access to shared data from multiple concurrent threads.
    @mutex = Mutex.new
    file_name = file_name.nil? ? "default_storage" : file_name
    # Locate the system root path
    file_path = File.expand_path(file_name)
    dir_name = File.dirname(file_path)
    # Raise an error when the directory doesn't exist in the system.
    raise DataStore::Error, "Directory #{dir_name} does not exist" unless File.directory? dir_name
    # Raise an error when the given file is not readable or writable.
    if File.exist?(file_path) && !(File.readable?(file_path) && File.writable?(file_path))
      raise DataStore::Error, "File #{file_name} not readable/writable"      
    end
    # Open a file if file already exist or create a new file with read and write mode.
    @file = File.open(file_path, 'a+')
    @file_path = file_path
    # Read the contents of a file.eval is used to convert the content as hash.
    file_content = @file.read
    @content = eval(file_content.gsub(':', '=>'))
    # Assign empty hash when the file is empty.
    @content={} if @content.nil?
  end
  
  # To write and update the data in to the data store.
  def commit_data
    # JSON.dump is to save the data in json format.
    File.open(@file_path, 'w+') { |f| f.write(JSON.dump(@content)) }
  end
  
  # Check Time-To-Live for a key has expired or not.
  def key_expired?(key)
    if @content.key?(key)
      ttl = @content[key][1]
      # To check Time-To-Live property has been set and expired for a key.
      if ttl != 0 && Time.parse(ttl.to_s.gsub('=>',':')) <= Time.now()
        @content.delete(key)
        commit_data
      end
    end
  end
  
  # A new key-value pair can be added to the data store using this method. 
  def create(key, value, ttl=0)
    @mutex.synchronize do
      # Check for the file size never exceeds 1GB
      if @file.size < (1024*1024*1024)
        # Ensure the key and value are in valid format.
        if is_valid_key?(key) && is_valid_json?(value)
          key_expired?(key)
          raise DataStore::Error, "Time to live should be in Integer type." unless ttl.is_a?(Integer)
          # If Create is invoked for an existing key
          if @content.key?(key)
            raise DataStore::Error, "Key '#{key}' already exist in the data store.Please try with different name."      
          else
            # create and save the data in to the file.
            time_to_live = ttl.zero? ? ttl : (Time.now + ttl)
            @content[key] = [value, time_to_live]
            commit_data
            p "Successfully created the data"
          end
        end
      else
        raise DataStore::Error, "File size exceeds maximum limit of 1GB"
      end
    end
  end

  # To read the data from the data store.A read operation can be performed by providing 
  # the key, and receiving the value in response, as a JSON object.
  def read(key)
    @mutex.synchronize do
      key_expired?(key)
      # Check whether the key has exist or not.
      if @content.key?(key)
        p @content[key][0]
        p "Successfully read the data"
      else
        raise DataStore::Error, "Key '#{key}' does not exist in the data store"
      end
    end
  end

  # To delete the data from the data store.A delete operation can be performed 
  # by providing the key.
  def delete(key)
    @mutex.synchronize do
      key_expired?(key)
      # Check whether the key has exist or not.
      if @content.key?(key)
        @content.delete(key)
        commit_data
        p "Successfully deleted the data"
      else
        raise DataStore::Error, "Key '#{key}' does not exist in the data store"
      end
    end
  end
  
  # Method to check for a valid key
  def is_valid_key?(key)
    # The key is always a string - capped at 32chars.
    if key.is_a?(String) && key.size <= 32
      return true
    else
      raise DataStore::Error, "Key should be always a string with size <= 32 characters"
    end
  end
  
  # Method to check for a valid json.
  def is_valid_json?(value)
    begin
      # The value is always a JSON object - capped at 16KB.
      json_size = JSON.parse(value.to_json).to_s.bytesize
      if json_size <= 16*1024
        return true
      else
        raise DataStore::Error, "JSON size should be <= 16KB"
      end
    rescue JSON::ParserError => e
      raise DataStore::Error, "Value should be always a JSON object"
    end
  end
end