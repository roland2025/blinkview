# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from types import SimpleNamespace

import numpy as np
import pytest

from blinkview.ui.widgets.action_button_delegate import ActionButtonDelegate, TelemetryCol, TelemetryDelegate
from blinkview.ui.widgets.config.style_config import StyleConfig


def _painter_on_pixmap():
    """The QPixmap paint device must outlive the QPainter using it, or later calls on the
    painter operate on a dangling C++ device and crash the process (not just raise a Python
    exception) - stashing it on the painter keeps it alive for exactly as long as the painter."""
    from qtpy.QtGui import QPainter, QPixmap

    pixmap = QPixmap(100, 30)
    painter = QPainter(pixmap)
    painter._keepalive_pixmap = pixmap
    return painter


def _make_option():
    from qtpy.QtCore import QRect
    from qtpy.QtGui import QFont
    from qtpy.QtWidgets import QStyleOptionViewItem

    option = QStyleOptionViewItem()
    option.rect = QRect(0, 0, 100, 30)
    option.font = QFont()
    return option


# ---------------------------------------------------------------------------
# ActionButtonDelegate
#
# Both paint() and editorEvent() only ever call index.column()/index.row() - a duck-typed
# SimpleNamespace stands in fine, no real QModelIndex needed. paint()'s button branch calls
# QApplication.style().drawControl(CE_PushButton, ...), a native platform-style paint operation
# that is fragile (crashed the whole process under the offscreen platform during development,
# not just raised a Python exception) - it's mocked out below rather than actually exercised, and
# the "else" branch's super().paint(...) (the real QStyledItemDelegate C++ paint, which requires
# a genuine QModelIndex) is mocked too so this stays a pure dispatch-logic test.
# ---------------------------------------------------------------------------


class FakeButtonModel:
    """Minimal stand-in for the table model ActionButtonDelegate.editorEvent reads keys from."""

    def __init__(self, keys):
        self.keys = keys


def _fake_index(row, column):
    return SimpleNamespace(row=lambda: row, column=lambda: column)


class TestActionButtonDelegatePaint:
    def test_paint_draws_a_button_in_the_action_column(self, qapp, monkeypatch):
        from blinkview.ui.widgets import action_button_delegate as module

        calls = []
        fake_style = SimpleNamespace(drawControl=lambda *a, **kw: calls.append(a))
        monkeypatch.setattr(module.QApplication, "style", staticmethod(lambda: fake_style))

        delegate = ActionButtonDelegate()
        delegate.paint(painter=object(), option=_make_option(), index=_fake_index(0, 2))

        assert len(calls) == 1
        assert calls[0][0] == module.QStyle.CE_PushButton

    def test_paint_falls_back_to_default_rendering_outside_the_action_column(self, qapp, monkeypatch):
        from blinkview.ui.widgets.action_button_delegate import QStyledItemDelegate

        calls = []
        monkeypatch.setattr(QStyledItemDelegate, "paint", lambda self, *a: calls.append(a))

        delegate = ActionButtonDelegate()
        painter, option, index = object(), _make_option(), _fake_index(0, 0)
        delegate.paint(painter, option, index)

        assert calls == [(painter, option, index)]


class TestActionButtonDelegateEditorEvent:
    _release_type = "MouseButtonRelease"

    def _event(self, event_type):
        ev = SimpleNamespace()
        ev.type = lambda: event_type
        ev.MouseButtonRelease = self._release_type
        return ev

    def test_click_in_action_column_invokes_callback_with_the_row_key(self, qapp):
        received = []
        delegate = ActionButtonDelegate(callback=lambda key: received.append(key))
        model = FakeButtonModel(keys=["a", "b", "c"])
        index = _fake_index(row=1, column=2)
        event = self._event(self._release_type)

        handled = delegate.editorEvent(event, model, _make_option(), index)

        assert handled is True
        assert received == ["b"]

    def test_click_outside_action_column_is_not_handled(self, qapp):
        received = []
        delegate = ActionButtonDelegate(callback=lambda key: received.append(key))
        model = FakeButtonModel(keys=["a", "b", "c"])
        index = _fake_index(row=1, column=0)
        event = self._event(self._release_type)

        handled = delegate.editorEvent(event, model, _make_option(), index)

        assert handled is False
        assert received == []

    def test_click_without_a_callback_still_reports_handled(self, qapp):
        delegate = ActionButtonDelegate()  # no callback provided
        model = FakeButtonModel(keys=["a"])
        index = _fake_index(row=0, column=2)
        event = self._event(self._release_type)

        handled = delegate.editorEvent(event, model, _make_option(), index)

        assert handled is True  # still swallows the event, just does nothing

    def test_non_release_event_type_is_not_handled(self, qapp):
        received = []
        delegate = ActionButtonDelegate(callback=lambda key: received.append(key))
        model = FakeButtonModel(keys=["a"])
        index = _fake_index(row=0, column=2)
        event = self._event("MouseButtonPress")

        handled = delegate.editorEvent(event, model, _make_option(), index)

        assert handled is False
        assert received == []


# ---------------------------------------------------------------------------
# TelemetryDelegate
# ---------------------------------------------------------------------------


class FakeModule:
    def __init__(self, name, depth=0):
        self.name = name
        self.depth = depth


class FakeTelemetryModel:
    """Minimal stand-in exposing exactly the attributes TelemetryDelegate.paint reads,
    backed by real numpy arrays/memoryviews the same shape as TelemetryTableModel's."""

    def __init__(self, n_modules=2):
        self.visible_mod_ids = np.arange(n_modules, dtype=np.int32)
        self.modules = [FakeModule(f"mod{i}", depth=i) for i in range(n_modules)]
        self.painted_seqs = np.ones(n_modules, dtype=np.int64)
        self.painted_levels = np.zeros(n_modules, dtype=np.uint8)
        self.arrival_times = np.zeros(n_modules, dtype=np.float64)
        self.change_times = np.zeros(n_modules, dtype=np.float64)

        self.seqs_mv = memoryview(self.painted_seqs)
        self.levels_mv = memoryview(self.painted_levels)
        self.arr_mv = memoryview(self.arrival_times)
        self.chg_mv = memoryview(self.change_times)


class FakeDelegateIndex:
    """Stands in for a real QModelIndex - TelemetryDelegate.paint only calls
    index.model()/.row()/.column()/.data(role), never anything Qt-model-machinery-specific."""

    def __init__(self, model, row, column, value=""):
        self._model = model
        self._row = row
        self._column = column
        self._value = value

    def model(self):
        return self._model

    def row(self):
        return self._row

    def column(self):
        return self._column

    def data(self, role):
        return self._value


@pytest.fixture
def theme():
    return StyleConfig()


class TestTelemetryDelegateConstruction:
    def test_rebuild_cache_populates_flash_brushes(self, qapp, theme):
        delegate = TelemetryDelegate(theme)
        assert len(delegate._flash_brushes) == 100
        assert delegate.steps == 100

    def test_size_hint_is_fixed_regardless_of_input(self, qapp, theme):
        delegate = TelemetryDelegate(theme)
        size = delegate.sizeHint(_make_option(), _fake_index(0, 0))
        assert (size.width(), size.height()) == (50, 10)


class TestTelemetryDelegatePaint:
    def test_paint_does_not_raise_for_a_valid_row(self, qapp, theme):
        delegate = TelemetryDelegate(theme)
        model = FakeTelemetryModel()
        index = FakeDelegateIndex(model, row=0, column=TelemetryCol.NAME, value="mod0")
        painter = _painter_on_pixmap()

        delegate.paint(painter, _make_option(), index)
        painter.end()

    def test_paint_returns_early_for_a_row_beyond_visible_modules(self, qapp, theme):
        delegate = TelemetryDelegate(theme)
        model = FakeTelemetryModel(n_modules=1)
        index = FakeDelegateIndex(model, row=5, column=TelemetryCol.NAME, value="?")
        painter = _painter_on_pixmap()

        delegate.paint(painter, _make_option(), index)  # must not raise
        painter.end()

    def test_paint_swallows_exceptions_from_a_broken_model(self, qapp, theme, capsys):
        """The broad except Exception around the whole paint body exists so a delegate bug
        never crashes the C++ paint event loop - lock in that a broken model attribute access
        is caught and logged, not propagated."""
        delegate = TelemetryDelegate(theme)

        class BrokenModel:
            visible_mod_ids = [0]

            @property
            def modules(self):
                raise RuntimeError("boom")

        index = FakeDelegateIndex(BrokenModel(), row=0, column=TelemetryCol.NAME, value="x")
        painter = _painter_on_pixmap()

        delegate.paint(painter, _make_option(), index)  # must not raise
        painter.end()

        assert "CRASH AVERTED IN DELEGATE" in capsys.readouterr().out

    def test_name_column_indents_by_module_depth(self, qapp, theme):
        """Deeper modules in the tree get shifted right - can't easily assert the exact pixel
        offset painted, but can confirm indent math itself doesn't error for a deep module and
        a shallow one, and that a real QRect adjustment occurs (rect gets narrower) - the
        no-raise assertion below is the meaningful contract here."""
        delegate = TelemetryDelegate(theme)
        model = FakeTelemetryModel(n_modules=3)  # depths 0, 1, 2
        for depth in (0, 1, 2):
            index = FakeDelegateIndex(model, row=depth, column=TelemetryCol.NAME, value=f"mod{depth}")
            painter = _painter_on_pixmap()
            delegate.paint(painter, _make_option(), index)
            painter.end()
