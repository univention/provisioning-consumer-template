# SPDX-FileCopyrightText: 2026 Univention GmbH
# SPDX-License-Identifier: AGPL-3.0-only
import os
import json
import tempfile
import pytest
from unittest.mock import MagicMock, patch


@pytest.fixture(autouse=True)
def patch_dn():
    with patch("provisioning_consumer_lib.consumer.DN") as mock_dn:
        mock_dn.side_effect = lambda dn: dn
        yield mock_dn


@pytest.fixture
def mock_logger():
    return MagicMock()


@pytest.fixture
def config_dict():
    return {
        "name": "test-consumer",
        "provisioning_url": "https://example.com/provisioning",
        "config_path": tempfile.mkdtemp(),
    }


@pytest.fixture
def temp_config_file(tmp_path):
    def _create(name="subscription_name", password="subscription_password"):
        config_file = tmp_path / "config.json"
        config_file.write_text(json.dumps({
            "subscription_name": name,
            "subscription_password": password,
        }))
        return str(config_file)
    return _create


@pytest.fixture
def sample_event():
    return {
        "uuid": "test-uuid-123",
        "timestamp": "2026-01-01T00:00:00Z",
        "sequence_number": 1234,
        "body": {
            "old": {
                "dn": "uid=testuser,cn=users,dc=example,dc=com",
                "uid": ["testuser"],
            },
            "new": {
                "dn": "uid=testuser,cn=users,dc=example,dc=com",
                "uid": ["testuser"],
                "cn": ["Test User"],
            },
        },
    }


@pytest.fixture
def sample_create_event():
    return {
        "uuid": "test-uuid-456",
        "timestamp": "2026-01-01T00:00:00Z",
        "body": {
            "old": None,
            "new": {
                "dn": "uid=newuser,cn=users,dc=example,dc=com",
                "uid": ["newuser"],
                "cn": ["New User"],
            },
        },
    }


@pytest.fixture
def sample_remove_event():
    return {
        "uuid": "test-uuid-789",
        "timestamp": "2026-01-01T00:00:00Z",
        "body": {
            "old": {
                "dn": "uid=olduser,cn=users,dc=example,dc=com",
                "uid": ["olduser"],
            },
            "new": None,
        },
    }


@pytest.fixture
def sample_move_event():
    return {
        "uuid": "test-uuid-move",
        "timestamp": "2026-01-01T00:00:00Z",
        "body": {
            "old": {
                "dn": "uid=testuser,cn=users,dc=old,dc=com",
                "uid": ["testuser"],
            },
            "new": {
                "dn": "uid=testuser,cn=users,dc=new,dc=com",
                "uid": ["testuser"],
            },
        },
    }


@pytest.fixture
def mock_session():
    session = MagicMock()
    response = MagicMock()
    response.status_code = 200
    response.json.return_value = {"sequence_number": 123}
    response.text = "OK"
    session.get.return_value = response
    session.post.return_value = response
    session.patch.return_value = response
    return session


@pytest.fixture
def ConcreteEventHandler(mock_logger):
    from provisioning_consumer_lib.consumer import UDMEventHandler

    class ConcreteEventHandler(UDMEventHandler):
        def __init__(self, logger, create_handler=None, modify_handler=None, remove_handler=None, error_handler_fn=None):
            super().__init__(logger)
            self.create_called_with = None
            self.modify_called_with = None
            self.remove_called_with = None
            self.error_called_with = None
            self._create_handler_fn = create_handler
            self._modify_handler_fn = modify_handler
            self._remove_handler_fn = remove_handler
            self._error_handler_fn = error_handler_fn

        def _handle_create(self, metadata, new):
            self.create_called_with = (metadata, new)
            if self._create_handler_fn:
                self._create_handler_fn()

        def _handle_modify(self, metadata, old, new, has_moved):
            self.modify_called_with = (metadata, old, new, has_moved)
            if self._modify_handler_fn:
                self._modify_handler_fn()

        def _handle_remove(self, metadata, old):
            self.remove_called_with = (metadata, old)
            if self._remove_handler_fn:
                self._remove_handler_fn()

        def _handle_error(self, metadata, old, new, exc_type, exc_value, exc_traceback):
            self.error_called_with = (metadata, old, new, exc_type, exc_value, exc_traceback)
            if self._error_handler_fn:
                self._error_handler_fn()
            else:
                super()._handle_error(metadata, old, new, exc_type, exc_value, exc_traceback)

    return ConcreteEventHandler
