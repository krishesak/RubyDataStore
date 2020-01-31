## Execute this file using 'ruby init_store.rb' to create the data store 
# and perform CRD operations.
require_relative 'data_store'

# initialize data store with optional file path
p "initialising data store with optional file path.."
data_store1 = DataStore.new("sample_test_store")
# initialize data store in default location
p "initialising data store in default location.."
data_store2 = DataStore.new

# it creates a key-value pair with no time-to-live property.
data_store1.create("Employee", {"name"=>"Esakki","Age"=> "TCS", "phone"=>"9597270720"})

data_store2.create("Company", {"name"=>"Tcs","location"=> "chennai", "phone"=>"5455454"})

# it creates a key-value pair with time-to-live property.
data_store1.create("Vehicle", {"name"=>"R15","prize"=> "200000"}, 60)

data_store2.create("Owner", {"name"=>"veera","location"=> "chennai"}, 45)
# it returns the value of a respective key in json format if key has not expired
# otherwise it will raise an error
data_store1.read("Employee")

data_store2.read("Owner")
# it deletes the respective key from the data store if key has not expired
# otherwise it will raise an error
data_store1.delete("Vehicle")

data_store2.delete("Company")