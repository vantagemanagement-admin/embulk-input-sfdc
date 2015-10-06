module OverrideAssertRaise
  # NOTE: Embulk 0.7.1+ required to raise ConfigError to do as `ConfigError.new("message")`,
  #       original `assert_raise` method can't catch that, but `begin .. rescue` can.
  #       So we override assert_raise as below.
  def assert_raise(expected_klass = StandardError, &block)
    assert begin
      block.call
      false
    rescue expected_klass => e
      true
    end
  end
end
