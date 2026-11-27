from blinkview.parsers.time_sync_profiler import SeqPairBook

SRC = 1
A = 10
B = 20
S = 1_000_000_000  # 1 s in ns


def test_source_first_then_consumers_pair_once_each():
    book = SeqPairBook([A, B])
    assert book.add_server(0, 100, 1, rx_ts=1 * S) == []
    assert book.add_consumer(A, 0, 150, 1, rx_ts=1 * S) == (A, (100, 1), (150, 1))
    assert book.add_consumer(B, 0, 170, 1, rx_ts=1 * S) == (B, (100, 1), (170, 1))


def test_consumer_first_waits_for_source():
    book = SeqPairBook([A])
    assert book.add_consumer(A, 5, 150, 0, rx_ts=1 * S) is None
    assert book.add_server(5, 100, 0, rx_ts=1 * S) == [(A, (100, 0), (150, 0))]
    # paired rows are gone: the source's next row of the same seq finds nothing waiting
    assert book.add_server(5, 200, 0, rx_ts=1 * S) == []


def test_reset_drops_rows_left_over_from_the_last_run():
    """A consumer row the last run never paired must not pair with the next run's edge of the same seq."""
    book = SeqPairBook([A])
    assert book.add_consumer(A, 90, -257 * S, 1, rx_ts=1 * S) is None  # last run, never paired
    book.reset_consumer(A)  # the board logged `new_seq_nr: 0`
    assert book.add_server(90, 5 * S, 1, rx_ts=2 * S) == []
    assert book.add_consumer(A, 90, 5 * S + 300, 1, rx_ts=2 * S) == (A, (5 * S, 1), (5 * S + 300, 1))


def test_reset_server_forgets_old_source_rows():
    book = SeqPairBook([A])
    book.add_server(3, 100, 1, rx_ts=1 * S)
    book.reset_server()
    assert book.add_consumer(A, 3, 150, 1, rx_ts=1 * S) is None


def test_rows_older_than_max_age_are_evicted():
    book = SeqPairBook([A], max_age_ns=30 * S)
    book.add_consumer(A, 7, 100, 1, rx_ts=0)
    # 31 s later the source logs seq 7 of a new run: the stale consumer row is gone
    assert book.add_server(7, 31 * S, 1, rx_ts=31 * S) == []
    book.add_server(8, 32 * S, 0, rx_ts=32 * S)
    # a row within the age limit still pairs
    assert book.add_consumer(A, 8, 32 * S + 50, 0, rx_ts=40 * S) == (A, (32 * S, 0), (32 * S + 50, 0))


def test_many_pending_source_rows_are_kept_for_late_consumers():
    """The old 50-entry cap lost pairs when a consumer's rows (a Zephyr log thread) lagged the source by > 50 edges."""
    book = SeqPairBook([A])
    for seq in range(600):
        book.add_server(seq, seq * 10_000_000, seq % 2, rx_ts=seq * 10_000_000)
    pairs = [book.add_consumer(A, seq, seq * 10_000_000 + 100, seq % 2, rx_ts=7 * S) for seq in range(600)]
    assert all(p is not None for p in pairs)


def test_max_entries_caps_memory():
    book = SeqPairBook([A], max_entries=100)
    for seq in range(1000):
        book.add_server(seq, seq, 0, rx_ts=0)
    assert len(book.server) == 100
    assert min(book.server) == 900


def test_state_mismatch_is_reported_as_a_pair_for_the_caller_to_check():
    book = SeqPairBook([A])
    book.add_server(0, 100, 1, rx_ts=0)
    assert book.add_consumer(A, 0, 150, 0, rx_ts=0) == (A, (100, 1), (150, 0))
