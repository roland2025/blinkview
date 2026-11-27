from blinkview.parsers.time_sync_profiler import SeqPairBook

SRC = 1
A = 10
B = 20
S = 1_000_000_000  # 1 s in ns


def test_source_first_then_consumers_pair_once_each():
    book = SeqPairBook([SRC, A, B])
    assert book.add_source(SRC, 0, 100, 1, rx_ts=1 * S) == []
    assert book.add_consumer(A, 0, 150, 1, rx_ts=1 * S) == (A, (100, 1), (150, 1))
    assert book.add_consumer(B, 0, 170, 1, rx_ts=1 * S) == (B, (100, 1), (170, 1))


def test_consumer_first_waits_for_source():
    book = SeqPairBook([SRC, A])
    assert book.add_consumer(A, 5, 150, 0, rx_ts=1 * S) is None
    assert book.add_source(SRC, 5, 100, 0, rx_ts=1 * S) == [(A, (100, 0), (150, 0))]
    # paired rows are gone: the source's next row of the same seq finds nothing waiting
    assert book.add_source(SRC, 5, 200, 0, rx_ts=1 * S) == []


def test_reset_drops_rows_left_over_from_the_last_run():
    """A consumer row the last run never paired must not pair with the next run's edge of the same seq."""
    book = SeqPairBook([SRC, A])
    assert book.add_consumer(A, 90, -257 * S, 1, rx_ts=1 * S) is None  # last run, never paired
    book.reset(A)  # the board logged `new_seq_nr: 0`
    assert book.add_source(SRC, 90, 5 * S, 1, rx_ts=2 * S) == []
    assert book.add_consumer(A, 90, 5 * S + 300, 1, rx_ts=2 * S) == (A, (5 * S, 1), (5 * S + 300, 1))


def test_reset_of_the_source_forgets_its_rows():
    book = SeqPairBook([SRC, A])
    book.add_source(SRC, 3, 100, 1, rx_ts=1 * S)
    book.reset(SRC)
    assert book.add_consumer(A, 3, 150, 1, rx_ts=1 * S) is None


def test_rows_older_than_max_age_are_evicted():
    book = SeqPairBook([SRC, A], max_age_ns=30 * S)
    book.add_consumer(A, 7, 100, 1, rx_ts=0)
    # 31 s later the source logs seq 7 of a new run: the stale consumer row is gone
    assert book.add_source(SRC, 7, 31 * S, 1, rx_ts=31 * S) == []
    book.add_source(SRC, 8, 32 * S, 0, rx_ts=32 * S)
    # a row within the age limit still pairs
    assert book.add_consumer(A, 8, 32 * S + 50, 0, rx_ts=40 * S) == (A, (32 * S, 0), (32 * S + 50, 0))


def test_many_pending_source_rows_are_kept_for_late_consumers():
    """The old 50-entry cap lost pairs when a consumer's rows (a Zephyr log thread) lagged the source by > 50 edges."""
    book = SeqPairBook([SRC, A])
    for seq in range(600):
        book.add_source(SRC, seq, seq * 10_000_000, seq % 2, rx_ts=seq * 10_000_000)
    pairs = [book.add_consumer(A, seq, seq * 10_000_000 + 100, seq % 2, rx_ts=7 * S) for seq in range(600)]
    assert all(p is not None for p in pairs)


def test_max_entries_caps_memory():
    book = SeqPairBook([SRC, A], max_entries=100)
    for seq in range(1000):
        book.add_source(SRC, seq, seq, 0, rx_ts=0)
    assert len(book.source) == 100
    assert min(book.source) == 900


def test_state_mismatch_is_reported_as_a_pair_for_the_caller_to_check():
    book = SeqPairBook([SRC, A])
    book.add_source(SRC, 0, 100, 1, rx_ts=0)
    assert book.add_consumer(A, 0, 150, 0, rx_ts=0) == (A, (100, 1), (150, 0))


class TestAnyBoardCanBeTheSource:
    """The profiler used to pair only a fixed `server` board's edges, so it showed nothing while another board was
    the source (test 3 of wiki:reports/blinksync-hw-time-sync runs every board as source in turn)."""

    def test_the_old_source_pairs_as_a_consumer_of_the_new_one(self):
        book = SeqPairBook([SRC, A, B])
        book.add_source(SRC, 0, 100, 1, rx_ts=1 * S)
        assert book.add_consumer(A, 0, 150, 1, rx_ts=1 * S) == (A, (100, 1), (150, 1))

        # next run: A is the source, SRC a consumer
        assert book.add_source(A, 0, 5 * S, 1, rx_ts=5 * S) == []
        assert book.add_consumer(SRC, 0, 5 * S + 40, 1, rx_ts=5 * S) == (SRC, (5 * S, 1), (5 * S + 40, 1))
        assert book.add_consumer(B, 0, 5 * S + 70, 1, rx_ts=5 * S) == (B, (5 * S, 1), (5 * S + 70, 1))

    def test_a_new_source_drops_the_old_sources_rows(self):
        book = SeqPairBook([SRC, A, B])
        book.add_source(SRC, 7, 100, 1, rx_ts=1 * S)
        book.add_source(A, 0, 5 * S, 1, rx_ts=5 * S)

        # a late row of the last run must not pair with SRC's seq 7
        assert book.add_consumer(B, 7, 150, 1, rx_ts=5 * S) is None

    def test_a_new_source_drops_its_own_waiting_consumer_rows(self):
        book = SeqPairBook([SRC, A])
        assert book.add_consumer(A, 3, 150, 1, rx_ts=1 * S) is None  # A as consumer, last run

        book.add_source(A, 0, 5 * S, 1, rx_ts=5 * S)
        assert book.consumers[A] == {}

    def test_consumer_rows_waiting_for_the_next_source_pair_with_it(self):
        """A consumer's first row of a run can arrive before the new source's (each board has its own delay)."""
        book = SeqPairBook([SRC, A, B])
        book.add_source(SRC, 199, 100, 0, rx_ts=1 * S)  # end of the last run
        book.reset(SRC)
        book.reset(B)
        assert book.add_consumer(B, 0, 5 * S + 70, 1, rx_ts=5 * S) is None

        assert book.add_source(A, 0, 5 * S, 1, rx_ts=5 * S) == [(B, (5 * S, 1), (5 * S + 70, 1))]

    def test_a_board_does_not_pair_with_its_own_source_rows(self):
        """Switched to consumer for the next run, the old source's first pin_state rows wait for the new source."""
        book = SeqPairBook([SRC, A])
        book.add_source(SRC, 0, 100, 1, rx_ts=1 * S)

        assert book.add_consumer(SRC, 0, 5 * S, 1, rx_ts=5 * S) is None
        assert book.add_source(A, 0, 5 * S - 40, 1, rx_ts=5 * S) == [(SRC, (5 * S - 40, 1), (5 * S, 1))]
