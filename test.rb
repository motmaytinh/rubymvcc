require 'test/unit'
require_relative 'main'

class MyTest < Test::Unit::TestCase

  def test_read_uncommitted
    database = Database.new(IsolationLevel::ReadUncommittedIsolation)

    c1 = database.new_connection
    tx_id = c1.must_exec_command('begin', nil)

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
    assert_equal(nil, res, 'c1 cannot delete x')

    assert_raise RuntimeError do
      c1.exec_command('get', 'x')
    end

    assert_raise RuntimeError do
      c2.exec_command('get', 'x')
    end
  end

  def test_read_committed
    database = Database.new(IsolationLevel::ReadCommittedIsolation)

    c1 = database.new_connection
    c1.must_exec_command('begin', nil)

    c2 = database.new_connection()
    c2.must_exec_command('begin', nil)

    # Local change is visible locally.
    c1.must_exec_command('set', 'x', 'hey')

    res = c1.must_exec_command('get', 'x')
    assert_equal('hey', res, 'c1 cannot get x')

    # Update not available to this transaction since this is not committed.
    assert_raise RuntimeError do
      c2.must_exec_command('get', 'x')
    end

    c1.must_exec_command('commit', nil)

    # Now that it's been committed, it's visible in c2.
    res = c2.must_exec_command('get', 'x')
    assert_equal('hey', res, 'c2 cannot get x')

    c3 = database.new_connection
    c3.must_exec_command('begin', nil)

    # Local change is visible locally.
    c3.must_exec_command('set', 'x', 'yall')

    res = c3.must_exec_command('get', 'x')
    assert_equal('yall', res, 'c3 cannot get x')

    # But not on the other commit, again.
    res = c2.must_exec_command('get', 'x')
    assert_equal('hey', res, 'c2 cannot get x')

    # And if we delete it, it should show up deleted locally.
    c2.must_exec_command('delete', 'x')

    assert_raise RuntimeError do
      c2.must_exec_command('get', 'x')
    end

    c2.must_exec_command('commit', nil)

    # It should also show up as deleted in new transactions now
    # that it has been committed.
    c4 = database.new_connection
    c4.must_exec_command('begin', nil)

    assert_raise RuntimeError do
      c4.must_exec_command('get', 'x')
    end
  end
end
