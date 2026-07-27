# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""End-to-end coverage for TelemetryPlotter's playback-clock wiring, driving the real widget
(not just the kernels/buffers in isolation) through a real Registry/GUIContext. This is the
level that caught a real bug during development: ReplayWindowBuffer was initially missing
is_dirty/is_dirty_overview, which only surfaced once apply_updates()/_update_plots() actually
ran together against real data - each piece in isolation looked fine."""

import pytest

import blinkview.ui.widgets.plotter as plotter_module
from blinkview.core.numpy_batch_manager import PooledLogBatch
from blinkview.core.playback_clock import PlaybackMode
from blinkview.ui.widgets.plotter import TelemetryPlotter
from tests.fakes.real_registry import make_real_gui_context, make_real_registry


@pytest.fixture
def registry(tmp_path):
    reg = make_real_registry(tmp_path, "plotter_playback_test")
    yield reg
    reg.stop()


@pytest.fixture
def plotter(qapp, qtbot, registry):
    gui_context = make_real_gui_context(registry)

    device = registry.id_registry.get_device("plottertest")
    module = device.get_module("floats")

    base = registry.now_ns()
    array_pool = registry.system_ctx.array_pool
    log_pool = registry.central.log_pool
    src = array_pool.create(PooledLogBatch, 20, 4096, has_levels=True, has_modules=True, has_devices=True)
    with src:
        for i in range(20):
            ts = base + i * 100_000_000  # 100ms apart
            src.insert_any(ts, ts, f"{float(i)}".encode("ascii"), level=0, module=module.id, device=device.id)
        log_pool.batch_append(src)

    # PlaybackClock only refreshes its cached bounds_min_ns/bounds_max_ns inside tick() (called
    # continuously by PlaybackControlWidget in the real app) - without at least one tick here,
    # bounds still reflect the empty pool from clock construction time (before this fixture's
    # data was inserted), so clock.bounds_min_ns below would be 0 and enter_replay()/seek() would
    # clamp into that stale range instead of anchoring within the real data.
    registry.playback_clock.tick(registry.now_ns())

    w = TelemetryPlotter(gui_context)
    qtbot.addWidget(w)
    w.modules = [module]
    w.resize(800, 600)
    w.module = module  # stash for tests
    yield w


def test_live_fetch_discovers_and_populates_the_module_buffer(plotter):
    for _ in range(3):
        plotter.apply_updates(force=True)

    buf = plotter.buffers.get(plotter.module)
    assert buf is not None
    assert buf.size > 0


def test_replay_follow_populates_a_separate_replay_buffer_without_touching_the_live_ring(plotter):
    for _ in range(3):
        plotter.apply_updates(force=True)
    live_buf = plotter.buffers.get(plotter.module)
    live_size_before = live_buf.size

    clock = plotter.gui_context.registry.playback_clock
    clock.enter_replay(clock.bounds_min_ns + 1_000_000_000)
    clock.play(speed=1.0)
    assert clock.mode is PlaybackMode.REPLAY

    for _ in range(3):
        clock.tick(plotter.gui_context.registry.now_ns())
        plotter.apply_updates(force=True)

    active = plotter._active_buffer(plotter.module)
    from blinkview.core.buffers import ReplayWindowBuffer

    assert isinstance(active, ReplayWindowBuffer)
    assert active.size > 0
    assert live_buf.size == live_size_before  # live ring untouched by REPLAY fetching


def test_manual_pan_during_replay_detaches_follow(plotter):
    for _ in range(3):
        plotter.apply_updates(force=True)

    clock = plotter.gui_context.registry.playback_clock
    clock.enter_replay(clock.bounds_min_ns + 1_000_000_000)
    plotter.apply_updates(force=True)

    assert plotter.follow_playback is True
    plotter._on_main_plot_range_changed()
    assert plotter.follow_playback is False


def test_clear_resets_the_active_replay_buffer_not_just_the_live_ring(plotter):
    for _ in range(3):
        plotter.apply_updates(force=True)

    clock = plotter.gui_context.registry.playback_clock
    clock.enter_replay(clock.bounds_min_ns + 1_000_000_000)
    plotter.apply_updates(force=True)
    assert plotter._active_buffer(plotter.module).size > 0

    plotter.clear()

    assert plotter._active_buffer(plotter.module).size == 0


def test_paused_stationary_replay_does_not_refetch_or_redraw(plotter, monkeypatch):
    """A paused REPLAY scrub the user isn't touching must do zero work per tick - no
    fetch_telemetry_window call and no _update_plots() call - not just skip the fetch while
    still re-applying the view range and redrawing on a fixed timer."""
    for _ in range(3):
        plotter.apply_updates(force=True)

    clock = plotter.gui_context.registry.playback_clock
    clock.enter_replay(clock.bounds_min_ns + 1_000_000_000)
    plotter.apply_updates(force=True)  # establishes replay_buffers + initial view range/redraw

    fetch_calls = 0
    orig_fetch = plotter_module.fetch_telemetry_window

    def counting_fetch(*args, **kwargs):
        nonlocal fetch_calls
        fetch_calls += 1
        return orig_fetch(*args, **kwargs)

    monkeypatch.setattr(plotter_module, "fetch_telemetry_window", counting_fetch)

    redraw_calls = 0
    orig_update_plots = plotter._update_plots

    def counting_update_plots(*args, **kwargs):
        nonlocal redraw_calls
        redraw_calls += 1
        return orig_update_plots(*args, **kwargs)

    monkeypatch.setattr(plotter, "_update_plots", counting_update_plots)

    for _ in range(5):
        # Bypass the top-of-tick throttle gate so each call actually reaches the follow logic,
        # simulating several heartbeat ticks passing with the clock paused and untouched.
        plotter._last_update_ns = 0
        plotter.apply_updates(force=False)

    assert clock.is_playing is False
    assert fetch_calls == 0
    assert redraw_calls == 0


def test_paused_stationary_replay_falls_back_to_the_configured_live_cadence(plotter):
    """The REPLAY-follow throttle gate must only demand the fast 10Hz cadence while the
    playhead is actually moving (playing, or scrubbed since the last tick that got past the
    gate) - a paused, stationary REPLAY session should poll at the same, typically much slower,
    cadence as ordinary LIVE viewing rather than spinning the whole apply_updates() body
    (including the per-module live-ring-freshness loop) at 10Hz forever for nothing to show."""
    plotter._update_interval_ns = 1_000_000_000  # simulate a realistic 1Hz LIVE cadence

    for _ in range(3):
        plotter.apply_updates(force=True)

    clock = plotter.gui_context.registry.playback_clock
    clock.enter_replay(clock.bounds_min_ns + 1_000_000_000)
    plotter.apply_updates(force=True)  # settle into the stationary follow state

    # A tick arriving well past the fast 100ms follow interval, but nowhere near the slow 1Hz
    # LIVE cadence, must be rejected by the gate while stationary.
    now_ns = plotter.gui_context.registry.now_ns()
    plotter._last_update_ns = now_ns - 150_000_000
    before = plotter._last_update_ns
    plotter.apply_updates(force=False)
    assert plotter._last_update_ns == before  # gate rejected it - stayed on the slow cadence


def test_playhead_line_appears_only_in_replay_and_tracks_current_ts(plotter):
    plotter.apply_updates(force=True)  # discovers the module, builds plots/curves/playhead lines

    assert plotter._playhead_lines
    assert plotter._overview_playhead_line is not None
    for line in plotter._playhead_lines:
        assert line.isVisible() is False  # LIVE mode - no playhead to show

    clock = plotter.gui_context.registry.playback_clock
    target_ts = clock.bounds_min_ns + 1_000_000_000
    clock.enter_replay(target_ts)
    plotter.apply_updates(force=True)

    for line in plotter._playhead_lines:
        assert line.isVisible() is True
        assert line.value() == pytest.approx(target_ts / 1_000_000_000.0)
    assert plotter._overview_playhead_line.isVisible() is True

    clock.go_live()
    plotter.apply_updates(force=True)

    for line in plotter._playhead_lines:
        assert line.isVisible() is False
    assert plotter._overview_playhead_line.isVisible() is False


def test_autoscroll_button_tracks_follow_playback_during_replay_and_lets_you_resume(plotter):
    """The toolbar's Auto-Scroll button doubles as the REPLAY follow control (see
    _sync_autoscroll_button/_on_autoscroll_toggled) - it must reflect follow_playback (not the
    LIVE-only is_auto_scroll) while in REPLAY, update immediately on a manual detach rather than
    waiting for the next tick, and let a click resume following."""
    plotter.apply_updates(force=True)
    assert plotter.autoscroll_action.text() == "Auto-Scroll: ON"
    assert plotter.autoscroll_action.isChecked() is True

    clock = plotter.gui_context.registry.playback_clock
    clock.enter_replay(clock.bounds_min_ns + 1_000_000_000)
    plotter.apply_updates(force=True)

    assert plotter.follow_playback is True
    assert plotter.autoscroll_action.text() == "Follow: ON"
    assert plotter.autoscroll_action.isChecked() is True

    # Manual pan detaches follow_playback - the button must update right away, not on the next
    # apply_updates tick.
    plotter._on_main_plot_range_changed()
    assert plotter.follow_playback is False
    assert plotter.autoscroll_action.text() == "Follow: OFF"
    assert plotter.autoscroll_action.isChecked() is False

    # Clicking the button while detached resumes following.
    plotter._on_autoscroll_toggled(True)
    assert plotter.follow_playback is True
    assert plotter.autoscroll_action.text() == "Follow: ON"
    assert plotter.autoscroll_action.isChecked() is True

    clock.go_live()
    plotter.apply_updates(force=True)
    assert plotter.autoscroll_action.text() == "Auto-Scroll: ON"
    assert plotter.autoscroll_action.isChecked() is True


def test_replay_follow_fetches_one_sample_past_each_displayed_edge(plotter):
    """Regression test: the REPLAY-follow fetch used to request exactly [current_ts -
    half_span, current_ts + half_span] - the same span later applied as the view range - so
    nb_slice_and_downsample_linear's own edge-snap logic (which needs one real sample just past
    each visible boundary to interpolate the line through) had nothing to snap to, and the
    plotted line stopped short of the left/right edge instead of filling the whole view.

    fetch_telemetry_window's plus_one=True fixes this by fetching the single nearest sample just
    outside each boundary, unbounded on the far side - so it's found regardless of how sparse
    the data is, unlike a fixed-time padding on the fetched span. Verified here against real data
    (the `plotter` fixture's 20 samples, 100ms apart) with the window narrow enough (0.5s, so a
    250ms half-span) that its boundary deliberately falls strictly between two samples rather
    than landing on one."""
    for _ in range(3):
        plotter.apply_updates(force=True)  # discover the module on the live edge

    plotter.view_duration = 0.5  # half-span 250ms - narrower than a 100ms sample gap

    clock = plotter.gui_context.registry.playback_clock
    base_ts = clock.bounds_min_ns  # ts of the fixture's first (index 0) sample
    target_ts = base_ts + 1_000_000_000  # exactly sample index 10
    clock.enter_replay(target_ts)
    plotter.apply_updates(force=True)

    replay_buf = plotter.replay_buffers[plotter.module]
    x_ns = replay_buf.x_data_int64[: replay_buf.size]

    half_span_ns = int(plotter.view_duration * 1_000_000_000 / 2)
    window_min_ns = target_ts - half_span_ns  # base + 750ms
    window_max_ns = target_ts + half_span_ns  # base + 1250ms

    assert x_ns.min() < window_min_ns  # the edge neighbor just before the left boundary (700ms)
    assert x_ns.max() > window_max_ns  # the edge neighbor just past the right boundary (1300ms)


def test_manual_pan_refetch_also_fetches_one_sample_past_each_edge(plotter):
    """Same plus_one requirement as the follow-fetch test above, but for the one-shot re-fetch
    triggered by a manual pan/zoom during REPLAY (_refetch_replay_window)."""
    for _ in range(3):
        plotter.apply_updates(force=True)

    clock = plotter.gui_context.registry.playback_clock
    clock.enter_replay(clock.bounds_min_ns + 1_000_000_000)
    plotter.apply_updates(force=True)

    base_s = clock.bounds_min_ns / 1_000_000_000.0
    t_min_s, t_max_s = base_s + 0.75, base_s + 1.25  # same boundary-between-samples setup

    plotter._refetch_replay_window(t_min_s, t_max_s)

    replay_buf = plotter.replay_buffers[plotter.module]
    x_ns = replay_buf.x_data_int64[: replay_buf.size]

    window_min_ns = int(round(t_min_s * 1_000_000_000))
    window_max_ns = int(round(t_max_s * 1_000_000_000))

    assert x_ns.min() < window_min_ns
    assert x_ns.max() > window_max_ns


def test_returning_to_live_resets_follow_playback_for_the_next_replay_session(plotter):
    plotter.apply_updates(force=True)
    clock = plotter.gui_context.registry.playback_clock
    clock.enter_replay(clock.bounds_min_ns + 1_000_000_000)
    plotter.apply_updates(force=True)
    plotter._on_main_plot_range_changed()  # detach
    assert plotter.follow_playback is False

    clock.go_live()
    plotter.apply_updates(force=True)

    assert plotter.follow_playback is True
