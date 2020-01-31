require_relative 'data_store'
require 'minitest/autorun'

describe DataStore do
  
  before(:all) do
    @data_store = DataStore.new
  end

  describe "create data" do
    it "when creating with time-to-live value" do
      response = @data_store.create("Employee", {"name"=>"Esakki"}, 40)
      response.must_equal "Successfully created the data"
    end

    it "when creating without time-to-live value" do
      response = @data_store.create("Vehicle", {"name"=>"RE"})
      response.must_equal "Successfully created the data"
    end
  end
end
