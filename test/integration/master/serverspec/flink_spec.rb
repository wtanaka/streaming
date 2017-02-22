require 'serverspec'

set :backend, :exec

describe "Flink Master" do
  describe port(8081) do
    it { should be_listening.with('tcp') }
  end
end
