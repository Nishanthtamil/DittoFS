import sys, pathlib, json
from PyQt6.QtWidgets import (QApplication, QSystemTrayIcon, QMenu, QDialog,
                             QVBoxLayout, QLabel, QLineEdit, QPushButton,
                             QCheckBox)
from PyQt6.QtCore import QTimer, QThread, pyqtSignal
from PyQt6.QtGui import QIcon, QAction

class SettingsDialog(QDialog):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("DittoFS Settings")
        layout = QVBoxLayout(self)

        self.host_edit = QLineEdit("localhost")
        self.port_edit = QLineEdit("8765")
        self.auto_start = QCheckBox("Start on login")

        layout.addWidget(QLabel("Host:"))
        layout.addWidget(self.host_edit)
        layout.addWidget(QLabel("Port:"))
        layout.addWidget(self.port_edit)
        layout.addWidget(self.auto_start)

        save_btn = QPushButton("Save")
        save_btn.clicked.connect(self.accept)
        layout.addWidget(save_btn)

class TrayApp(QApplication):
    def __init__(self):
        super().__init__(sys.argv)
        self.setQuitOnLastWindowClosed(False)

        # 64×64 solid blue icon
        pixmap = QIcon().pixmap(64, 64)
        self.tray = QSystemTrayIcon(QIcon.fromTheme("folder"))
        self.tray.setVisible(True)

        # Menu
        menu = QMenu()
        open_action = QAction("Open Folder")
        open_action.triggered.connect(self.open_folder)
        menu.addAction(open_action)

        settings_action = QAction("Settings…")
        settings_action.triggered.connect(self.show_settings)
        menu.addAction(settings_action)

        quit_action = QAction("Quit")
        quit_action.triggered.connect(self.quit)
        menu.addAction(quit_action)

        self.tray.setContextMenu(menu)

        # Settings dialog
        self.settings = SettingsDialog()

    def open_folder(self):
        import webbrowser, os
        webbrowser.open("file:///tmp/ditto")

    def show_settings(self):
        self.settings.show()

if __name__ == "__main__":
    app = TrayApp()
    sys.exit(app.exec())