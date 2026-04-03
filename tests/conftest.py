"""Shared pytest fixtures and configuration for shootback tests."""

import os
import sys
import tempfile
import shutil
import socket
import threading
import time
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


@pytest.fixture
def temp_dir():
    """Create a temporary directory for test files."""
    temp_path = tempfile.mkdtemp()
    yield temp_path
    shutil.rmtree(temp_path, ignore_errors=True)


@pytest.fixture
def mock_socket():
    """Create a mock socket object."""
    mock_sock = Mock(spec=socket.socket)
    mock_sock.fileno.return_value = 1
    mock_sock.recv.return_value = b"test data"
    mock_sock.send.return_value = 9
    return mock_sock


@pytest.fixture
def mock_ssl_context():
    """Create a mock SSL context."""
    with patch('ssl.SSLContext') as mock_ctx:
        yield mock_ctx


@pytest.fixture
def free_port():
    """Find and return a free port number."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(('', 0))
        s.listen(1)
        port = s.getsockname()[1]
    return port


@pytest.fixture
def mock_config():
    """Create a mock configuration dictionary."""
    return {
        'master': {
            'listen-on': '0.0.0.0:10000',
            'communicate-url': 'https://localhost:10443',
            'ssl': False,
            'ssl-cert': None,
            'ssl-key': None
        },
        'slaver': {
            'communicate-url': 'https://localhost:10443',
            'target': 'localhost:22',
            'ssl': False,
            'ssl-cert': None
        }
    }


@pytest.fixture
def mock_logger():
    """Create a mock logger."""
    logger = Mock()
    logger.info = Mock()
    logger.error = Mock()
    logger.warning = Mock()
    logger.debug = Mock()
    logger.exception = Mock()
    return logger


@pytest.fixture
def thread_cleanup():
    """Ensure threads are cleaned up after tests."""
    threads_before = threading.active_count()
    yield
    max_wait = 5
    start_time = time.time()
    while threading.active_count() > threads_before and time.time() - start_time < max_wait:
        time.sleep(0.1)


@pytest.fixture
def mock_threading_event():
    """Create a mock threading Event."""
    event = Mock(spec=threading.Event)
    event.is_set.return_value = False
    event.wait.return_value = True
    return event


@pytest.fixture
def sample_binary_data():
    """Provide sample binary data for testing."""
    return b'\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09'


@pytest.fixture
def sample_text_data():
    """Provide sample text data for testing."""
    return "Hello, this is test data!"


@pytest.fixture
def mock_server_socket():
    """Create a mock server socket that accepts connections."""
    server_sock = Mock(spec=socket.socket)
    client_sock = Mock(spec=socket.socket)
    client_sock.recv.return_value = b"client data"
    server_sock.accept.return_value = (client_sock, ('127.0.0.1', 12345))
    return server_sock


@pytest.fixture(autouse=True)
def reset_modules():
    """Reset singleton modules between tests."""
    modules_to_reset = ['master', 'slaver', 'common_func']
    for module in modules_to_reset:
        if module in sys.modules:
            del sys.modules[module]
    yield