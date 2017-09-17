require 'serverspec'

set :backend, :exec

describe "Flink Master" do
  describe port(8081) do
    it { should be_listening.with('tcp') }
  end

  describe port(6123) do
    it { should be_listening.with('tcp') }
  end
end

describe "Zookeeper" do
  describe port(2181) do
    it { should be_listening.with('tcp') }
  end
end

describe "Kafka" do
  describe port(9092) do
    it { should be_listening.with('tcp') }
  end
end
