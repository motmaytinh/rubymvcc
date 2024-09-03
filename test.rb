require 'test/unit'
require_relative 'main'

class MyTest < Test::Unit::TestCase

  def test_read_uncommitted
    database = Database.new(IsolationLevel::ReadUncommittedIsolation)

    c1 = database.new_connection
    c1.must_exec_command('begin', nil)

    c2 = database.new_connection()
    c2.must_exec_command('begin', nil)

    c1.must_exec_command('set', 'x', 'hey')

    # Update is visible to self.
    res = c1.must_exec_command('get', 'x')
    assert_equal('hey', res, 'c1 cannot get x')

    # But since read uncommitted, also available to everyone else.
    res = c2.must_exec_command('get', 'x')
    assert_equal('hey', res, 'c2 cannot get x')

    # And if we delete, that should be respected.
    res = c1.must_exec_command('delete', 'x')
    assert_equal('', res, 'c1 cannot delete x')

    res = c1.exec_command('get', 'x')
    assert_equal(res, '', 'c1 sees no x')
    assert_equal(err.Error(), 'cannot get key that does not exist', 'c1 sees no x')

    res, err = c2.exec_command('get', 'x')
    assert_equal(res, '', 'c2 sees no x')
    assert_equal(err.Error(), 'cannot get key that does not exist)
  end
end
