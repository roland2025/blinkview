from PySide6.QtWidgets import QMessageBox, QStyle

from PySide6.QtWidgets import QMessageBox, QPushButton
from PySide6.QtGui import QDesktopServices
from PySide6.QtCore import QUrl


GITHUB_PROJECT = "https://github.com/roland2025/blinkview"


def set_as_in_development(target, parent_widget, feature_name=None, issue_no=None):
    """
    Instead of disabling, this keeps the action active and
    shows a 'Teaser' dialog when clicked.
    """
    current_text = target.text()
    if " (Soon™)" not in current_text:
        target.setText(f"{current_text} (Soon™)")

    # We keep it ENABLED now
    target.setEnabled(True)

    # Connect to the 'Teaser' function
    name = feature_name or current_text
    target.triggered.connect(lambda: show_feature_teaser(parent_widget, name, issue_no))

    return target


def show_feature_teaser(parent, feature_name, issue_no=None):
    msg = QMessageBox(parent)

    # Shorten the window title (keeps the OS taskbar/header clean)
    msg.setWindowTitle("Work in Progress")

    # This prevents the "squashed" look for long feature names.
    msg.setStyleSheet("QLabel{min-width: 450px;}")

    # HTML automatically handles word-wrapping much better than raw strings.
    msg.setText(f"<h3 style='margin-bottom: 0px;'>{feature_name}</h3>"
                f"<p>This feature is currently in the lab! 🧪</p>")

    msg.setInformativeText(
        "It's not quite ready for prime time yet.\n\n"
        "You can write to GitHub if you have specific requirements."
    )

    # URL Construction
    base_url = GITHUB_PROJECT.rstrip("/")
    if issue_no:
        url = f"{base_url}/issues/{issue_no}"
        button_text = f"View Issue #{issue_no}"
    else:
        url = f"{base_url}/issues"
        button_text = "Open GitHub Issues"

    msg.addButton(QMessageBox.Ok)
    github_button = msg.addButton(button_text, QMessageBox.ActionRole)

    # Use a network/web icon for the GitHub button
    icon = parent.style().standardIcon(QStyle.StandardPixmap.SP_DriveNetIcon)
    github_button.setIcon(icon)

    msg.exec_()

    if msg.clickedButton() == github_button:
        QDesktopServices.openUrl(QUrl(url))
