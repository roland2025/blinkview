# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import pytest

from blinkview.core.numpy_batch_manager import PooledLogBatch
from blinkview.ui.widgets.plotter import SeriesContainer, TelemetryPlotter
from tests.fakes.real_registry import make_real_gui_context, make_real_registry


@pytest.fixture
def registry(tmp_path):
    reg = make_real_registry(tmp_path, "plotter_test")
    yield reg
    reg.stop()


def _push_samples(registry, module, device, count=20, spacing_ns=100_000_000):
    base = registry.now_ns()
    array_pool = registry.system_ctx.array_pool
    log_pool = registry.central.log_pool
    src = array_pool.create(PooledLogBatch, count, 4096, has_levels=True, has_modules=True, has_devices=True)
    with src:
        for i in range(count):
            ts = base + i * spacing_ns
            src.insert_any(ts, ts, f"{float(i)}".encode("ascii"), level=0, module=module.id, device=device.id)
        log_pool.batch_append(src)


@pytest.fixture
def plotter(qapp, qtbot, registry):
    gui_context = make_real_gui_context(registry)
    device = registry.id_registry.get_device("plottertest")
    module = device.get_module("floats")
    _push_samples(registry, module, device)

    w = TelemetryPlotter(gui_context)
    qtbot.addWidget(w)
    w.modules = [module]
    w.resize(800, 600)
    w.module = module
    w.device = device
    yield w


class TestGetColor:
    def test_returns_distinct_colors_for_different_indices(self, qapp, qtbot, registry):
        gui_context = make_real_gui_context(registry)
        w = TelemetryPlotter(gui_context)
        qtbot.addWidget(w)

        assert w.get_color(0).name() != w.get_color(1).name()


class TestFrequencyParsing:
    def test_on_freq_changed_parses_hz_into_nanosecond_interval(self, plotter):
        plotter._on_freq_changed("30 Hz")
        assert plotter._update_interval_ns == 1_000_000_000 // 30

    def test_on_freq_changed_ignores_unparseable_text(self, plotter):
        before = plotter._update_interval_ns
        plotter._on_freq_changed("garbage")
        assert plotter._update_interval_ns == before


class TestParseDuration:
    @pytest.mark.parametrize(
        "text,expected",
        [
            ("10s", 10.0),
            ("5", 5.0),
            ("2m", 120.0),
            ("1h", 3600.0),
            ("0.5s", 0.5),
        ],
    )
    def test_parses_valid_durations(self, plotter, text, expected):
        assert plotter._parse_duration(text) == expected

    def test_invalid_text_returns_none(self, plotter):
        assert plotter._parse_duration("not-a-duration") is None


class TestGetStateAndRestore:
    def test_round_trips_basic_fields(self, qapp, qtbot, registry):
        gui_context = make_real_gui_context(registry)
        device = registry.id_registry.get_device("restore_test")
        module = device.get_module("floats")
        _push_samples(registry, module, device)

        w = TelemetryPlotter(gui_context)
        qtbot.addWidget(w)
        w.modules = [module]
        w.is_split = True
        w.show_overview = False

        state = w.get_state()
        assert state["is_split"] is True
        assert state["show_overview"] is False
        assert state["modules"] == [module.name_with_device()]

        w2 = TelemetryPlotter(gui_context, state=state)
        qtbot.addWidget(w2)

        assert w2.is_split is True
        assert w2.show_overview is False
        assert w2.modules == [module]

    def test_restore_rebuilds_series_list_from_state(self, qapp, qtbot, registry):
        gui_context = make_real_gui_context(registry)
        device = registry.id_registry.get_device("restore_series_test")
        module = device.get_module("floats")

        state = {
            "tab_name": "MyPlot",
            "is_split": False,
            "modules": [module.name_with_device()],
            "series": [
                {"module": module.name_with_device(), "index": 0, "name": "custom", "visible": True},
            ],
        }

        w = TelemetryPlotter(gui_context, state=state)
        qtbot.addWidget(w)

        assert w.tab_name == "MyPlot"
        assert len(w.series_list) == 1
        assert w.series_list[0].name == "custom"
        assert w.series_list[0].module == module


class TestSplitMode:
    def test_switching_to_split_creates_one_plot_per_series(self, plotter):
        plotter.apply_updates(force=True)  # discover module -> populate series_list
        plotter.set_split_mode(True)

        assert plotter.is_split is True
        for s in plotter.series_list:
            assert s.plot_item is not None

    def test_switching_back_to_shared_reuses_a_single_plot(self, plotter):
        plotter.apply_updates(force=True)
        plotter.set_split_mode(True)
        plotter.set_split_mode(False)

        assert plotter.is_split is False
        plot_items = {s.plot_item for s in plotter.series_list}
        assert len(plot_items) == 1

    def test_has_name_collisions_true_when_names_repeat(self, plotter):
        plotter.series_list = [
            SeriesContainer(module=plotter.module, index=0, name="dup", color="#fff"),
            SeriesContainer(module=plotter.module, index=1, name="dup", color="#000"),
        ]
        assert plotter.has_name_collisions() is True

    def test_has_name_collisions_false_when_names_unique(self, plotter):
        plotter.series_list = [
            SeriesContainer(module=plotter.module, index=0, name="a", color="#fff"),
            SeriesContainer(module=plotter.module, index=1, name="b", color="#000"),
        ]
        assert plotter.has_name_collisions() is False


class TestToggleSeries:
    def test_toggle_series_hides_curve_and_overview_curve(self, plotter):
        plotter.apply_updates(force=True)
        series = plotter.series_list[0]

        plotter.toggle_series(series, False)

        assert series.visible is False
        assert series.curve.isVisible() is False
        assert series.overview_curve.isVisible() is False

    def test_toggle_series_shows_again(self, plotter):
        plotter.apply_updates(force=True)
        series = plotter.series_list[0]
        plotter.toggle_series(series, False)

        plotter.toggle_series(series, True)

        assert series.visible is True
        assert series.curve.isVisible() is True


class TestToggleDiscrete:
    def test_single_series_toggles_immediately_without_a_menu(self, plotter):
        plotter.apply_updates(force=True)
        series = plotter.series_list[0]
        assert series.is_discrete is False

        plotter._show_discrete_menu()

        assert series.is_discrete is True

    def test_toggle_discrete_flips_the_flag(self, plotter):
        plotter.apply_updates(force=True)
        series = plotter.series_list[0]

        plotter.toggle_discrete(series, True)

        assert series.is_discrete is True

    def test_show_discrete_menu_is_a_noop_with_no_series(self, plotter):
        plotter.series_list = []
        plotter._show_discrete_menu()  # must not raise


class TestAutoscroll:
    def test_set_autoscroll_is_a_noop_when_already_matching(self, plotter):
        assert plotter.is_auto_scroll is True
        plotter.set_autoscroll(True)  # already True - should be a no-op, not raise

    def test_set_autoscroll_updates_flag_and_button(self, plotter):
        plotter.set_autoscroll(False)
        assert plotter.is_auto_scroll is False
        assert plotter.autoscroll_action.text() == "Auto-Scroll: OFF"

    def test_on_autoscroll_toggled_in_live_mode_delegates_to_set_autoscroll(self, plotter):
        plotter._on_autoscroll_toggled(False)
        assert plotter.is_auto_scroll is False


class TestAxisVisibility:
    def test_update_axis_visibility_does_not_raise_without_series(self, plotter):
        plotter.series_list = []
        plotter._update_axis_visibility()  # must not raise

    def test_split_mode_only_shows_labels_on_bottom_plot_when_paused(self, plotter):
        plotter.apply_updates(force=True)
        plotter.set_split_mode(True)
        plotter.set_autoscroll(False)  # show_labels becomes True (not auto-scrolling)

        plotter._update_axis_visibility()

        bottom = plotter.series_list[-1].plot_item.getAxis("bottom")
        assert bottom.style["showValues"] is True


class TestApplyViewRange:
    def test_enforces_a_minimum_span(self, plotter):
        plotter.apply_updates(force=True)
        plotter._apply_view_range(100.0, 100.05, force=True)

        low, high = plotter.series_list[0].plot_item.viewRange()[0]
        assert pytest.approx(high - low, abs=0.01) == 0.1


class TestOnDurationChanged:
    def test_autoscroll_mode_recomputes_view_range_from_latest_timestamp(self, plotter):
        plotter.apply_updates(force=True)
        plotter.set_autoscroll(True)

        plotter._on_duration_changed("30s")

        assert plotter.view_duration == 30.0
        low, high = plotter.series_list[0].plot_item.viewRange()[0]
        assert pytest.approx(high - low, abs=1.0) == 30.0

    def test_manual_mode_keeps_the_current_right_edge(self, plotter):
        plotter.apply_updates(force=True)
        plotter.set_autoscroll(False)

        plotter._on_duration_changed("10s")

        assert plotter.view_duration == 10.0

    def test_invalid_duration_is_ignored(self, plotter):
        before = plotter.view_duration
        plotter._on_duration_changed("garbage")
        assert plotter.view_duration == before

    def test_zero_duration_is_ignored(self, plotter):
        before = plotter.view_duration
        plotter._on_duration_changed("0s")
        assert plotter.view_duration == before


class TestScrollToNow:
    def test_noop_when_not_auto_scrolling(self, plotter):
        plotter.apply_updates(force=True)
        plotter.set_autoscroll(False)
        low_before, high_before = plotter.series_list[0].plot_item.viewRange()[0]

        plotter._scroll_to_now()

        low_after, high_after = plotter.series_list[0].plot_item.viewRange()[0]
        assert (low_before, high_before) == (low_after, high_after)

    def test_moves_view_to_current_time_when_auto_scrolling(self, plotter):
        plotter.apply_updates(force=True)
        plotter.set_autoscroll(True)

        plotter._scroll_to_now()

        low, high = plotter.series_list[0].plot_item.viewRange()[0]
        now_sec = plotter.gui_context.registry.now_ns() / 1e9
        assert pytest.approx(high, abs=1.0) == now_sec


class TestGetLatestTimestamp:
    def test_returns_zero_with_no_buffers(self, plotter):
        assert plotter._get_latest_timestamp() == 0.0

    def test_returns_the_newest_buffer_timestamp(self, plotter):
        plotter.apply_updates(force=True)
        assert plotter._get_latest_timestamp() > 0


class TestGetTargetResolution:
    def test_falls_back_to_default_when_width_is_not_positive(self, plotter):
        plotter.apply_updates(force=True)
        plot_item = plotter.series_list[0].plot_item
        vb = plot_item.getViewBox()
        original_width = vb.width
        vb.width = lambda: 0
        try:
            assert plotter._get_target_resolution(plot_item) == 1000
        finally:
            vb.width = original_width

    def test_falls_back_to_default_on_attribute_error(self, plotter):
        class BadPlotItem:
            def getViewBox(self):
                raise AttributeError("no viewbox")

        assert plotter._get_target_resolution(BadPlotItem()) == 1000


class TestSetOverviewVisible:
    def test_hides_the_overview_plot(self, plotter):
        plotter.apply_updates(force=True)
        plotter.set_overview_visible(False)

        assert plotter.show_overview is False
        assert plotter.overview_action.isChecked() is False
        assert plotter.overview_plot is None


class TestDragAndDrop:
    class FakeMimeData:
        def __init__(self, text, has_text=True):
            self._text = text
            self._has_text = has_text

        def hasText(self):
            return self._has_text

        def text(self):
            return self._text

    class FakeDropEvent:
        def __init__(self, text):
            self._mime = TestDragAndDrop.FakeMimeData(text)
            self.accepted = False

        def mimeData(self):
            return self._mime

        def acceptProposedAction(self):
            self.accepted = True

    def test_drop_event_adds_a_resolvable_module(self, plotter, registry):
        device = registry.id_registry.get_device("drop_test_device")
        module = device.get_module("dropped")

        event = self.FakeDropEvent(module.name_with_device())
        plotter.modules = []

        plotter.dropEvent(event)

        assert module in plotter.modules
        assert event.accepted is True

    def test_drop_event_ignores_unresolvable_identifiers(self, plotter):
        # resolve_module splits on "." to get (device, module) - a string with no dot at all
        # can't be unpacked and resolve_module returns None for it (real modules always resolve
        # by auto-creating whatever device.module path is given, so a dotted string always
        # "succeeds" - this is the one shape that genuinely fails).
        event = self.FakeDropEvent("nodothere")
        before = list(plotter.modules)

        plotter.dropEvent(event)

        assert plotter.modules == before
        assert event.accepted is False

    def test_drop_event_skips_a_module_already_present(self, plotter):
        before = list(plotter.modules)
        event = self.FakeDropEvent(plotter.module.name_with_device())

        plotter.dropEvent(event)

        assert plotter.modules == before

    def test_drop_event_with_empty_text_does_nothing(self, plotter):
        event = self.FakeDropEvent("   ")
        plotter.dropEvent(event)
        assert event.accepted is False

    def test_drag_enter_accepts_text_mime_data(self, plotter):
        class FakeDragEvent:
            def __init__(self):
                self.accepted = False

            def mimeData(self):
                return TestDragAndDrop.FakeMimeData("something")

            def acceptProposedAction(self):
                self.accepted = True

        event = FakeDragEvent()
        plotter.dragEnterEvent(event)
        assert event.accepted is True


class TestApplyHysteresisToPlot:
    def test_zero_height_viewbox_is_a_noop(self, plotter):
        plotter.apply_updates(force=True)
        plot_item = plotter.series_list[0].plot_item
        vb = plot_item.getViewBox()
        original_height = vb.height
        vb.height = lambda: 0.0
        try:
            plotter.plot_range_states.clear()
            plotter._apply_hysteresis_to_plot(plot_item, 0.0, 1.0)
            assert plot_item not in plotter.plot_range_states
        finally:
            vb.height = original_height

    def test_nan_bounds_are_a_noop(self, plotter):
        plotter.apply_updates(force=True)
        plot_item = plotter.series_list[0].plot_item
        plotter.plot_range_states.clear()

        plotter._apply_hysteresis_to_plot(plot_item, float("nan"), 1.0)

        assert plot_item not in plotter.plot_range_states

    def test_inf_bounds_are_a_noop(self, plotter):
        plotter.apply_updates(force=True)
        plot_item = plotter.series_list[0].plot_item
        plotter.plot_range_states.clear()

        plotter._apply_hysteresis_to_plot(plot_item, float("-inf"), 1.0)

        assert plot_item not in plotter.plot_range_states

    def test_valid_bounds_initialize_state_and_set_range(self, plotter):
        plotter.apply_updates(force=True)
        plotter.set_split_mode(False)
        plot_item = plotter.series_list[0].plot_item
        plotter.plot_range_states.clear()

        plotter._apply_hysteresis_to_plot(plot_item, 0.0, 10.0)

        assert plot_item in plotter.plot_range_states
        assert plotter.plot_range_states[plot_item]["init"] is True


class TestClearLiveMode:
    def test_clear_resets_buffers_and_curves(self, plotter):
        plotter.apply_updates(force=True)
        buf = plotter.buffers[plotter.module]
        assert buf.size > 0

        plotter.clear()

        assert buf.size == 0
        assert buf.is_dirty is True
        for s in plotter.series_list:
            if s.curve:
                assert s.curve.xData is None or len(s.curve.xData) == 0


class TestOnRegionChanged:
    def test_ignored_while_system_is_updating(self, plotter):
        plotter.apply_updates(force=True)
        plotter._is_system_updating = True
        plotter.set_autoscroll(True)

        plotter._on_region_changed()  # must be a no-op

        assert plotter.is_auto_scroll is True

    def test_live_mode_manual_region_change_detaches_autoscroll(self, plotter):
        plotter.apply_updates(force=True)
        plotter.set_autoscroll(True)
        plotter._is_system_updating = False

        plotter._on_region_changed()

        assert plotter.is_auto_scroll is False


class TestInitModuleChannelsDropsStaleSeries:
    def test_stale_series_referencing_removed_channels_are_dropped(self, plotter):
        plotter.apply_updates(force=True)
        buf = plotter.buffers[plotter.module]
        num_channels = buf.num_channels

        # Simulate a persisted series referencing a channel index that no longer exists.
        plotter.series_list.append(
            SeriesContainer(module=plotter.module, index=num_channels + 5, name="stale", color="#123456")
        )

        plotter._init_module_channels(plotter.module, num_channels, buf.last_seq)

        assert all(s.index < num_channels for s in plotter.series_list if s.module == plotter.module)
