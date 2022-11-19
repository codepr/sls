defmodule SlsTest do
  use ExUnit.Case
  doctest Sls

  test "greets the world" do
    assert Sls.hello() == :world
  end
end
