import sys
from PySide6.QtWidgets import (
    QApplication, QDialog, QVBoxLayout, QHBoxLayout, 
    QLabel, QLineEdit, QPushButton, QCheckBox, QGridLayout
)
from PySide6.QtCore import Qt


class CredentialsDialog(QDialog):
    """Password dialog using QDialog."""
    
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Tractor Credentials")
        self.setModal(True)
        self.resize(400, 200)
        
        self._username = ""
        self._password = ""
        
        self._setup_ui()
    
    def _setup_ui(self):
        """Setup the UI."""
        layout = QVBoxLayout(self)
        
        # Title
        title = QLabel("Please enter your credentials")
        title_font = title.font()
        title_font.setPointSize(12)
        title_font.setBold(True)
        title.setFont(title_font)
        layout.addWidget(title)
        
        # Form layout
        form_layout = QGridLayout()
        form_layout.setSpacing(10)
        
        # Username
        form_layout.addWidget(QLabel("Username:"), 0, 0)
        self.username_field = QLineEdit()
        self.username_field.setPlaceholderText("Enter username")
        form_layout.addWidget(self.username_field, 0, 1)
        
        # Password
        form_layout.addWidget(QLabel("Password:"), 1, 0)
        self.password_field = QLineEdit()
        self.password_field.setPlaceholderText("Enter password")
        self.password_field.setEchoMode(QLineEdit.Password)
        form_layout.addWidget(self.password_field, 1, 1)
        
        layout.addLayout(form_layout)
        
        # Show password checkbox
        self.show_password_check = QCheckBox("Show password")
        self.show_password_check.stateChanged.connect(self._toggle_password_visibility)
        layout.addWidget(self.show_password_check)
        
        layout.addStretch()
        
        # Buttons
        button_layout = QHBoxLayout()
        button_layout.addStretch()
        
        self.cancel_button = QPushButton("Cancel")
        self.cancel_button.clicked.connect(self.reject)
        button_layout.addWidget(self.cancel_button)
        
        self.ok_button = QPushButton("OK")
        self.ok_button.setDefault(True)
        self.ok_button.clicked.connect(self._on_accept)
        button_layout.addWidget(self.ok_button)
        
        layout.addLayout(button_layout)
        
        # Connect text changes to validate
        self.username_field.textChanged.connect(self._validate)
        self.password_field.textChanged.connect(self._validate)
        
        # Initial validation
        self._validate()
        
        # Set focus
        self.username_field.setFocus()
    
    def _toggle_password_visibility(self, state):
        """Toggle password visibility."""
        if state == Qt.Checked:
            self.password_field.setEchoMode(QLineEdit.Normal)
        else:
            self.password_field.setEchoMode(QLineEdit.Password)
    
    def _validate(self):
        """Enable OK button only if both fields have text."""
        has_username = len(self.username_field.text()) > 0
        has_password = len(self.password_field.text()) > 0
        self.ok_button.setEnabled(has_username and has_password)
    
    def _on_accept(self):
        """Handle accept."""
        self._username = self.username_field.text()
        self._password = self.password_field.text()
        self.accept()
    
    def get_credentials(self):
        """Get the entered credentials."""
        return {
            'username': self._username,
            'password': self._password
        }


def getCredentials(parent=None):
    """ Show password dialog and return credentials.
    
    Example:
        credentials = getCredentials()
        if credentials:
            print(f"Username: {credentials['username']}")
            print(f"Password: {'*' * len(credentials['password'])}")
        else:
            print("Dialog cancelled")
    
    Returns:
        dict: {'username': str, 'password': str} if accepted, None if cancelled
    """
    # Ensure QApplication exists
    app = QApplication.instance()
    if app is None:
        app = QApplication(sys.argv)
    
    dialog = CredentialsDialog(parent)
    result = dialog.exec()
    
    if result == QDialog.Accepted:
        return dialog.get_credentials()
    return None
