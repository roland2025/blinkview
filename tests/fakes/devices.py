def esp32_wifi(id_registry):
    """Returns (device, module) for the canonical esp32/wifi pair used throughout
    log-fetch/log-table-viewer tests."""
    device = id_registry.get_device("esp32")
    return device, device.get_module("wifi")
