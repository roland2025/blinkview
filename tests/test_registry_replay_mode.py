from unittest.mock import patch

from blinkview.core.registry import Registry


def _build_registry(tmp_path, replay_mode):
    return Registry(session_name="replay_test", log_dir=tmp_path, replay_mode=replay_mode)


def test_replay_mode_builds_but_does_not_start_sources_and_pipelines(tmp_path):
    """registry.sources/registry.pipelines must still be constructed in replay mode (so their
    settings remain introspectable), but Registry.start() must never call their .start() -
    while central/reorder still start normally."""
    registry = _build_registry(tmp_path, replay_mode=True)
    registry.configure_system()

    assert registry.sources is not None
    assert registry.pipelines is not None

    # CentralStorage's config defaults logging.enabled=True (the "session" unified log
    # writer) - in replay mode this must not start, or replaying a session would
    # re-record the replayed data into a brand-new unified log for the replay run itself.
    assert registry.central.file_logger is None

    with (
        patch.object(registry.sources, "start") as sources_start,
        patch.object(registry.pipelines, "start") as pipelines_start,
        patch.object(registry.central, "start") as central_start,
        patch.object(registry.reorder, "start") as reorder_start,
    ):
        registry.start(configure=False)

    sources_start.assert_not_called()
    pipelines_start.assert_not_called()
    central_start.assert_called_once()
    reorder_start.assert_called_once()

    registry.stop()


def test_non_replay_mode_starts_sources_and_pipelines(tmp_path):
    """Control case: outside replay mode, sources/pipelines start exactly as before."""
    registry = _build_registry(tmp_path, replay_mode=False)
    registry.configure_system()

    assert registry.central.file_logger is not None

    with (
        patch.object(registry.sources, "start") as sources_start,
        patch.object(registry.pipelines, "start") as pipelines_start,
        patch.object(registry.central, "start") as central_start,
        patch.object(registry.reorder, "start") as reorder_start,
    ):
        registry.start(configure=False)

    sources_start.assert_called_once()
    pipelines_start.assert_called_once()
    central_start.assert_called_once()
    reorder_start.assert_called_once()

    registry.central.file_logger.stop()
    registry.stop()
