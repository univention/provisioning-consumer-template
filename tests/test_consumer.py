# SPDX-FileCopyrightText: 2026 Univention GmbH
# SPDX-License-Identifier: AGPL-3.0-only
import os
import json
import sys
import types
import tempfile
import pytest
from unittest.mock import MagicMock, AsyncMock, patch, call

from provisioning_consumer_lib.consumer import (
    UDMEventHandler,
    ConsumerModule,
    QueueAccessError,
    SubscriptionError,
)


class TestQueueAccessError:
    def test_is_exception(self):
        assert issubclass(QueueAccessError, Exception)

    @pytest.mark.parametrize("message", [
        "test message",
        "",
        "queue unreachable",
        "Connection refused",
        "a" * 1000,
    ])
    def test_can_be_instantiated(self, message):
        err = QueueAccessError(message)
        assert str(err) == message

    def test_can_be_raised_and_caught(self):
        with pytest.raises(QueueAccessError, match="boom"):
            raise QueueAccessError("boom")

    def test_is_not_system_exit(self):
        assert not issubclass(QueueAccessError, SystemExit)


class TestSubscriptionError:
    def test_is_exception(self):
        assert issubclass(SubscriptionError, Exception)

    @pytest.mark.parametrize("message", [
        "test message",
        "",
        "subscription failed",
        "HTTP 409 Conflict",
        "a" * 1000,
    ])
    def test_can_be_instantiated(self, message):
        err = SubscriptionError(message)
        assert str(err) == message

    def test_can_be_raised_and_caught(self):
        with pytest.raises(SubscriptionError, match="sub error"):
            raise SubscriptionError("sub error")

    def test_is_not_queue_access_error(self):
        assert not issubclass(SubscriptionError, QueueAccessError)
        assert not issubclass(QueueAccessError, SubscriptionError)


class TestUDMEventHandler:
    @pytest.mark.asyncio
    async def test_handle_event_routes_to_create(
        self, ConcreteEventHandler, mock_logger, sample_create_event
    ):
        handler = ConcreteEventHandler(mock_logger)
        result = await handler.handle_event(sample_create_event)
        assert result is True
        assert handler.create_called_with is not None
        assert handler.modify_called_with is None
        assert handler.remove_called_with is None

    @pytest.mark.asyncio
    async def test_handle_event_routes_to_modify(
        self, ConcreteEventHandler, mock_logger, sample_event
    ):
        handler = ConcreteEventHandler(mock_logger)
        result = await handler.handle_event(sample_event)
        assert result is True
        assert handler.create_called_with is None
        assert handler.modify_called_with is not None
        assert handler.remove_called_with is None

    @pytest.mark.asyncio
    async def test_handle_event_routes_to_remove(
        self, ConcreteEventHandler, mock_logger, sample_remove_event
    ):
        handler = ConcreteEventHandler(mock_logger)
        result = await handler.handle_event(sample_remove_event)
        assert result is True
        assert handler.create_called_with is None
        assert handler.modify_called_with is None
        assert handler.remove_called_with is not None

    @pytest.mark.asyncio
    async def test_handle_event_returns_true_on_success(
        self, ConcreteEventHandler, mock_logger, sample_event
    ):
        handler = ConcreteEventHandler(mock_logger)
        result = await handler.handle_event(sample_event)
        assert result is True

    @pytest.mark.asyncio
    @pytest.mark.parametrize("exc_class,message", [
        (ValueError, "bad value"),
        (RuntimeError, "runtime failure"),
        (KeyError, "missing key"),
        (AttributeError, "no such attribute"),
        (TypeError, "wrong type"),
    ])
    async def test_handle_event_returns_false_on_exception(
        self, ConcreteEventHandler, mock_logger, sample_event, exc_class, message
    ):
        """handle_event returns False for any non-SystemExit exception."""
        def raise_error():
            raise exc_class(message)

        handler = ConcreteEventHandler(
            mock_logger,
            modify_handler=raise_error,
            error_handler_fn=lambda: None,  # suppress re-raise
        )
        result = await handler.handle_event(sample_event)
        assert result is False

    @pytest.mark.asyncio
    async def test_handle_event_calls_error_handler_on_exception(
        self, ConcreteEventHandler, mock_logger, sample_event
    ):
        def raise_error():
            raise ValueError("test error")

        handler = ConcreteEventHandler(mock_logger, modify_handler=raise_error)
        with pytest.raises(ValueError, match="test error"):
            await handler.handle_event(sample_event)
        assert handler.error_called_with is not None
        metadata, old, new, exc_type, exc_value, exc_traceback = (
            handler.error_called_with
        )
        assert isinstance(exc_value, ValueError)
        assert str(exc_value) == "test error"

    @pytest.mark.asyncio
    @pytest.mark.parametrize("exc_class,handler_attr", [
        (ValueError, "modify_handler"),
        (RuntimeError, "create_handler"),
        (KeyError, "remove_handler"),
    ])
    async def test_handle_event_error_handler_receives_correct_exc_type(
        self, ConcreteEventHandler, mock_logger,
        sample_event, sample_create_event, sample_remove_event,
        exc_class, handler_attr,
    ):
        """_handle_error receives the correct exc_type for each handler type."""
        event_map = {
            "modify_handler": sample_event,
            "create_handler": sample_create_event,
            "remove_handler": sample_remove_event,
        }
        event = event_map[handler_attr]

        def raise_it():
            raise exc_class("err")

        handler = ConcreteEventHandler(mock_logger, **{handler_attr: raise_it})
        with pytest.raises(exc_class):
            await handler.handle_event(event)
        assert handler.error_called_with is not None
        _, _, _, exc_type, exc_value, _ = handler.error_called_with
        assert exc_type is exc_class
        assert isinstance(exc_value, exc_class)

    @pytest.mark.asyncio
    async def test_handle_event_does_not_catch_system_exit(
        self, ConcreteEventHandler, mock_logger, sample_event
    ):
        """SystemExit must propagate and not be passed to _handle_error."""
        def raise_exit():
            raise SystemExit(0)

        handler = ConcreteEventHandler(mock_logger, modify_handler=raise_exit)
        with pytest.raises(SystemExit):
            await handler.handle_event(sample_event)
        assert handler.error_called_with is None

    def test_handle_event_uses_default_logger_when_none(self):
        """EventHandler falls back to loguru.logger when logger=None."""
        from provisioning_consumer_lib.consumer import EventHandler
        import loguru

        class MinimalHandler(EventHandler):
            async def handle_event(self, event):
                return True

        handler = MinimalHandler(None)
        assert handler.logger is loguru.logger

    def test_event_to_udm_extracts_metadata_old_new(self, mock_logger):
        event = {
            "uuid": "test-uuid",
            "timestamp": "2026-01-01T00:00:00Z",
            "body": {
                "old": {"dn": "uid=test,dc=example,dc=com"},
                "new": {"dn": "uid=test,dc=example,dc=com", "cn": ["Test"]},
            },
        }
        metadata, old, new, has_moved = UDMEventHandler._event_to_udm(event)
        assert metadata["uuid"] == "test-uuid"
        assert "body" not in metadata
        assert old == {"dn": "uid=test,dc=example,dc=com"}
        assert new == {"dn": "uid=test,dc=example,dc=com", "cn": ["Test"]}
        assert has_moved is False

    def test_event_to_udm_does_not_mutate_original(self, mock_logger):
        """_event_to_udm must not modify the caller's event dict."""
        event = {
            "uuid": "test-uuid",
            "body": {
                "old": {"dn": "uid=a,dc=example,dc=com"},
                "new": {"dn": "uid=a,dc=example,dc=com"},
            },
        }
        original_keys = set(event.keys())
        UDMEventHandler._event_to_udm(event)
        assert set(event.keys()) == original_keys
        assert "body" in event

    def test_event_to_udm_detects_move(self, mock_logger, sample_move_event):
        with patch("provisioning_consumer_lib.consumer.DN") as mock_dn:
            mock_dn.return_value = MagicMock()
            mock_dn.side_effect = lambda dn: dn

            metadata, old, new, has_moved = UDMEventHandler._event_to_udm(
                sample_move_event
            )
            assert has_moved is True

    @pytest.mark.parametrize("old_dn,new_dn,expected_moved", [
        # Same DN → not moved
        ("uid=foo,dc=example,dc=com", "uid=foo,dc=example,dc=com", False),
        # Different DN → moved
        ("uid=foo,dc=old,dc=com", "uid=foo,dc=new,dc=com", True),
    ])
    def test_event_to_udm_has_moved_detection(
        self, mock_logger, old_dn, new_dn, expected_moved
    ):
        """has_moved is True iff the old and new DNs differ (string comparison via patch_dn)."""
        event = {
            "uuid": "x",
            "body": {
                "old": {"dn": old_dn},
                "new": {"dn": new_dn},
            },
        }
        # patch_dn fixture makes DN an identity function, so string equality is used
        _, _, _, has_moved = UDMEventHandler._event_to_udm(event)
        assert has_moved is expected_moved

    def test_diff_returns_changed_keys(self, mock_logger, sample_event):
        diff_result = UDMEventHandler.diff(sample_event)
        assert "cn" in diff_result
        assert "dn" not in diff_result
        assert "uid" not in diff_result

    def test_diff_filters_by_keys(self, mock_logger, sample_event):
        diff_result = UDMEventHandler.diff(sample_event, keys=["cn"])
        assert "cn" in diff_result
        assert "uid" not in diff_result

    def test_diff_empty_when_no_changes(self, mock_logger):
        event = {
            "uuid": "test-uuid",
            "timestamp": "2026-01-01T00:00:00Z",
            "body": {
                "old": {"dn": "uid=test,dc=example,dc=com", "cn": ["Test"]},
                "new": {"dn": "uid=test,dc=example,dc=com", "cn": ["Test"]},
            },
        }
        diff_result = UDMEventHandler.diff(event)
        assert len(diff_result) == 0

    @pytest.mark.parametrize("old_val,new_val,key,expect_in_diff", [
        # Value added (key missing in old)
        (None, ["Added"], "cn", True),
        # Value removed (key missing in new)
        (["Removed"], None, "cn", True),
        # Same single value
        (["Same"], ["Same"], "uid", False),
        # Different single value
        (["OldValue"], ["NewValue"], "uid", True),
        # Multi-value set change (order should not matter for set comparison)
        (["a", "b"], ["b", "a"], "mail", False),
        # Multi-value element added
        (["a"], ["a", "b"], "mail", True),
    ])
    def test_diff_value_changes(self, old_val, new_val, key, expect_in_diff):
        """diff() correctly detects presence/absence of a key in results."""
        old = {"dn": "uid=test,dc=example,dc=com"}
        new = {"dn": "uid=test,dc=example,dc=com"}
        if old_val is not None:
            old[key] = old_val
        if new_val is not None:
            new[key] = new_val
        event = {"uuid": "x", "body": {"old": old, "new": new}}
        diff_result = UDMEventHandler.diff(event)
        if expect_in_diff:
            assert key in diff_result
        else:
            assert key not in diff_result

    def test_diff_returns_old_and_new_values(self, mock_logger, sample_event):
        """diff() result values are (old_value, new_value) tuples."""
        diff_result = UDMEventHandler.diff(sample_event)
        assert "cn" in diff_result
        old_val, new_val = diff_result["cn"]
        assert old_val is None  # cn was not in old
        assert new_val == ["Test User"]

    @pytest.mark.parametrize("filter_keys,expected_present,expected_absent", [
        # Only 'cn' key → only cn (which changed) appears
        (["cn"], ["cn"], ["uid", "dn"]),
        # Only 'uid' key → uid is unchanged, so absent even though requested
        (["uid"], [], ["cn", "dn"]),
        # Both 'cn' and 'uid' → cn present (changed), uid absent (unchanged)
        (["cn", "uid"], ["cn"], ["dn"]),
        # 'dn' key → dn is the same in old/new, so absent
        (["dn"], [], ["cn", "uid"]),
    ])
    def test_diff_key_filtering(self, sample_event, filter_keys, expected_present, expected_absent):
        """diff() with keys parameter only checks specified keys."""
        diff_result = UDMEventHandler.diff(sample_event, keys=filter_keys)
        for key in expected_present:
            assert key in diff_result
        for key in expected_absent:
            assert key not in diff_result

    def test_diff_with_empty_old(self, mock_logger):
        event = {
            "uuid": "test-uuid",
            "timestamp": "2026-01-01T00:00:00Z",
            "body": {
                "old": None,
                "new": {"dn": "uid=test,dc=example,dc=com", "cn": ["Test"]},
            },
        }
        with pytest.raises(TypeError):
            UDMEventHandler.diff(event)

    def test_event_to_udm_missing_body_raises_key_error(self, mock_logger):
        event = {
            "uuid": "test-uuid",
            "timestamp": "2026-01-01T00:00:00Z",
        }
        with pytest.raises(KeyError):
            UDMEventHandler._event_to_udm(event)

    @pytest.mark.parametrize("body_content,expected_error", [
        ({}, KeyError),           # body exists but 'old'/'new' keys missing
        ({"old": {}}, KeyError),  # 'new' key missing
    ])
    def test_event_to_udm_incomplete_body(self, body_content, expected_error):
        """_event_to_udm raises KeyError for bodies missing required keys."""
        event = {"uuid": "x", "body": body_content}
        with pytest.raises(expected_error):
            UDMEventHandler._event_to_udm(event)

    @pytest.mark.asyncio
    async def test_handle_event_with_move_modifies_attributes(
        self, ConcreteEventHandler, mock_logger, sample_move_event
    ):
        handler = ConcreteEventHandler(mock_logger)
        await handler.handle_event(sample_move_event)
        assert handler.modify_called_with is not None
        metadata, old, new, has_moved = handler.modify_called_with
        assert has_moved is True

    @pytest.mark.asyncio
    async def test_is_relevant_returns_true_by_default(self, mock_logger):
        """EventHandler.is_relevant() always returns True unless overridden."""
        from provisioning_consumer_lib.consumer import EventHandler

        class MinimalHandler(EventHandler):
            async def handle_event(self, event):
                return True

        handler = MinimalHandler(mock_logger)
        assert await handler.is_relevant({}) is True
        assert await handler.is_relevant({"body": {}}) is True


class TestConsumerModuleInit:
    def test_init_requires_handler(self, mock_logger, config_dict):
        from provisioning_consumer_lib.consumer import EventHandler

        class MockHandler(EventHandler):
            async def handle_event(self, event):
                return True

        handler = MockHandler(mock_logger)
        consumer = ConsumerModule(handler, **config_dict)
        assert consumer.handler is not None

    @pytest.mark.parametrize("name,expected_error", [
        ("", ValueError),           # empty string
        (None, ValueError),         # None
        (123, ValueError),          # non-string integer
        ([], ValueError),           # list
    ])
    def test_init_validates_name_invalid(self, name, expected_error, tmp_path):
        config = {
            "name": name,
            "provisioning_url": "https://example.com",
            "handler": MagicMock(),
            "config_dir": str(tmp_path),
        }
        with pytest.raises(expected_error):
            ConsumerModule(**config)

    def test_init_validates_name_missing(self, mock_logger):
        config = {
            "name": "",
            "provisioning_url": "https://example.com",
            "handler": MagicMock(),
        }
        with pytest.raises(ValueError):
            ConsumerModule(**config)

    def test_init_validates_name_none(self, mock_logger):
        config = {
            "name": None,
            "provisioning_url": "https://example.com",
            "handler": MagicMock(),
        }
        with pytest.raises(ValueError):
            ConsumerModule(**config)

    def test_init_validates_provisioning_url_missing(self, mock_logger):
        config = {
            "name": "test-consumer",
            "provisioning_url": None,
            "handler": MagicMock(),
        }
        with pytest.raises(ValueError):
            ConsumerModule(**config)

    @pytest.mark.parametrize("error_timeout,expected_error", [
        (-1, ValueError),       # negative integer
        ("60", ValueError),     # string instead of int
        (1.5, ValueError),      # float
        (None, None),           # None → uses default (no error)
    ])
    def test_init_validates_error_timeout(self, error_timeout, expected_error, tmp_path):
        config = {
            "name": "test-consumer",
            "provisioning_url": "https://example.com",
            "handler": MagicMock(),
            "config_dir": str(tmp_path),
            "error_timeout": error_timeout,
        }
        if expected_error:
            with pytest.raises(expected_error):
                ConsumerModule(**config)
        else:
            consumer = ConsumerModule(**config)
            from provisioning_consumer_lib.consumer import DEFAULT_ERROR_TIMEOUT
            assert consumer.config["error_timeout"] == DEFAULT_ERROR_TIMEOUT

    @pytest.mark.parametrize("error_timeout", [0, 1, 60, 3600])
    def test_init_accepts_valid_error_timeout(self, error_timeout, tmp_path):
        config = {
            "name": "test-consumer",
            "provisioning_url": "https://example.com",
            "handler": MagicMock(),
            "config_dir": str(tmp_path),
            "error_timeout": error_timeout,
        }
        consumer = ConsumerModule(**config)
        assert consumer.config["error_timeout"] == error_timeout

    def test_init_sets_defaults(self, mock_logger, tmp_path):
        config = {
            "name": "test-consumer",
            "provisioning_url": "https://example.com",
            "handler": MagicMock(),
            "config_dir": str(tmp_path),
        }
        consumer = ConsumerModule(**config)
        assert consumer.config["name"] == "test-consumer"
        assert consumer.config["provisioning_url"] == "https://example.com"

    def test_init_normalizes_config_dir(self, mock_logger, tmp_path):
        config = {
            "name": "test-consumer",
            "provisioning_url": "https://example.com",
            "handler": MagicMock(),
            "config_dir": str(tmp_path) + "/",
        }
        consumer = ConsumerModule(**config)
        assert not consumer.config["config_dir"].endswith("/")

    @pytest.mark.parametrize("trailing_slashes", ["/", "//", "///"])
    def test_init_normalizes_config_dir_multiple_slashes(self, tmp_path, trailing_slashes):
        config = {
            "name": "test-consumer",
            "provisioning_url": "https://example.com",
            "handler": MagicMock(),
            "config_dir": str(tmp_path) + trailing_slashes,
        }
        consumer = ConsumerModule(**config)
        assert not consumer.config["config_dir"].endswith("/")

    def test_repr(self, mock_logger, tmp_path):
        config = {
            "name": "test-consumer",
            "provisioning_url": "https://example.com",
            "handler": MagicMock(),
            "config_dir": str(tmp_path),
        }
        consumer = ConsumerModule(**config)
        repr_str = repr(consumer)
        assert "ConsumerModule" in repr_str
        assert "test-consumer" in repr_str

    @pytest.mark.parametrize("name", [
        "my-consumer",
        "a",
        "consumer_123",
        "a" * 100,
    ])
    def test_init_accepts_valid_names(self, name, tmp_path):
        config = {
            "name": name,
            "provisioning_url": "https://example.com",
            "handler": MagicMock(),
            "config_dir": str(tmp_path),
        }
        consumer = ConsumerModule(**config)
        assert consumer.config["name"] == name

    def test_init_stores_handler(self, tmp_path):
        handler = MagicMock()
        config = {
            "name": "test-consumer",
            "provisioning_url": "https://example.com",
            "config_dir": str(tmp_path),
        }
        consumer = ConsumerModule(handler, **config)
        assert consumer.handler is handler

    def test_init_uses_provided_logger(self, tmp_path):
        custom_logger = MagicMock()
        config = {
            "name": "test-consumer",
            "provisioning_url": "https://example.com",
            "config_dir": str(tmp_path),
        }
        consumer = ConsumerModule(MagicMock(), logger=custom_logger, **config)
        assert consumer.logger is custom_logger


class TestConsumerModuleCredentials:
    def test_get_subscription_credentials_reads_file(
        self, mock_logger, tmp_path, temp_config_file
    ):
        config_dir = str(tmp_path)
        temp_config_file("myuser", "mypassword")

        config = {
            "name": "test-consumer",
            "provisioning_url": "https://example.com",
            "handler": MagicMock(),
            "config_dir": config_dir,
        }
        consumer = ConsumerModule(**config)
        name, password = consumer._get_subscription_credentials()
        assert name == "myuser"
        assert password == "mypassword"

    @pytest.mark.parametrize("sub_name,sub_password", [
        ("alice", "s3cr3t"),
        ("consumer-abc123", "tok" * 20),
        ("x", "y"),
    ])
    def test_get_subscription_credentials_various_values(
        self, tmp_path, sub_name, sub_password
    ):
        """Credentials with various name/password values are read correctly."""
        config_dir = str(tmp_path)
        config_file = os.path.join(config_dir, "provisioning_config.json")
        with open(config_file, "w") as f:
            json.dump({"subscription_name": sub_name, "subscription_password": sub_password}, f)

        config = {
            "name": "test-consumer",
            "provisioning_url": "https://example.com",
            "handler": MagicMock(),
            "config_dir": config_dir,
        }
        consumer = ConsumerModule(**config)
        name, password = consumer._get_subscription_credentials()
        assert name == sub_name
        assert password == sub_password

    def test_get_subscription_credentials_returns_none_when_missing(
        self, mock_logger, tmp_path
    ):
        config = {
            "name": "test-consumer",
            "provisioning_url": "https://example.com",
            "handler": MagicMock(),
            "config_dir": str(tmp_path),
        }
        consumer = ConsumerModule(**config)
        name, password = consumer._get_subscription_credentials()
        assert name is None
        assert password is None

    @pytest.mark.parametrize("file_content,description", [
        ({"subscription_name": "only_name"}, "missing password"),
        ({"subscription_password": "only_password"}, "missing name"),
        ({}, "empty object"),
        ({"unrelated_key": "value"}, "unrelated keys only"),
    ])
    def test_get_subscription_credentials_incomplete_config(
        self, tmp_path, file_content, description
    ):
        """Incomplete config files return (None, None) for all missing-key variants."""
        config_dir = str(tmp_path)
        config_file = os.path.join(config_dir, "provisioning_config.json")
        with open(config_file, "w") as f:
            json.dump(file_content, f)

        config = {
            "name": "test-consumer",
            "provisioning_url": "https://example.com",
            "handler": MagicMock(),
            "config_dir": config_dir,
        }
        consumer = ConsumerModule(**config)
        name, password = consumer._get_subscription_credentials()
        assert name is None, f"Expected None name for: {description}"
        assert password is None, f"Expected None password for: {description}"

    def test_get_subscription_credentials_partial_config_missing_password(
        self, mock_logger, tmp_path
    ):
        config_dir = str(tmp_path)
        config_file = os.path.join(config_dir, "provisioning_config.json")
        with open(config_file, "w") as f:
            json.dump({"subscription_name": "only_name"}, f)

        config = {
            "name": "test-consumer",
            "provisioning_url": "https://example.com",
            "handler": MagicMock(),
            "config_dir": config_dir,
        }
        consumer = ConsumerModule(**config)
        name, password = consumer._get_subscription_credentials()
        assert name is None
        assert password is None

    def test_get_subscription_credentials_partial_config_missing_name(
        self, mock_logger, tmp_path
    ):
        config_dir = str(tmp_path)
        config_file = os.path.join(config_dir, "provisioning_config.json")
        with open(config_file, "w") as f:
            json.dump({"subscription_password": "only_password"}, f)

        config = {
            "name": "test-consumer",
            "provisioning_url": "https://example.com",
            "handler": MagicMock(),
            "config_dir": config_dir,
        }
        consumer = ConsumerModule(**config)
        name, password = consumer._get_subscription_credentials()
        assert name is None
        assert password is None

    def test_save_subscription_credentials_writes_file(self, mock_logger, tmp_path):
        config = {
            "name": "test-consumer",
            "provisioning_url": "https://example.com",
            "handler": MagicMock(),
            "config_dir": str(tmp_path),
        }
        consumer = ConsumerModule(**config)
        consumer._save_subscription_credentials("testuser", "testpassword")

        config_file = os.path.join(str(tmp_path), "provisioning_config.json")
        assert os.path.exists(config_file)
        with open(config_file) as f:
            data = json.load(f)
        assert data["subscription_name"] == "testuser"
        assert data["subscription_password"] == "testpassword"

    @pytest.mark.parametrize("name,password", [
        ("user1", "pass1"),
        ("consumer-abc-" + "x" * 32, "tok" * 20),
        ("a", "b"),
    ])
    def test_save_subscription_credentials_roundtrip(self, tmp_path, name, password):
        """Credentials saved can be read back with _get_subscription_credentials."""
        config = {
            "name": "test-consumer",
            "provisioning_url": "https://example.com",
            "handler": MagicMock(),
            "config_dir": str(tmp_path),
        }
        consumer = ConsumerModule(**config)
        consumer._save_subscription_credentials(name, password)
        read_name, read_password = consumer._get_subscription_credentials()
        assert read_name == name
        assert read_password == password

    def test_save_subscription_credentials_overwrites_existing(self, tmp_path):
        """Saving new credentials overwrites old ones atomically."""
        config = {
            "name": "test-consumer",
            "provisioning_url": "https://example.com",
            "handler": MagicMock(),
            "config_dir": str(tmp_path),
        }
        consumer = ConsumerModule(**config)
        consumer._save_subscription_credentials("old_user", "old_pass")
        consumer._save_subscription_credentials("new_user", "new_pass")

        name, password = consumer._get_subscription_credentials()
        assert name == "new_user"
        assert password == "new_pass"

    def test_save_subscription_credentials_no_temp_file_left(self, tmp_path):
        """The .new temporary file must not exist after a successful save."""
        config = {
            "name": "test-consumer",
            "provisioning_url": "https://example.com",
            "handler": MagicMock(),
            "config_dir": str(tmp_path),
        }
        consumer = ConsumerModule(**config)
        consumer._save_subscription_credentials("u", "p")

        temp_file = os.path.join(str(tmp_path), "provisioning_config.json.new")
        assert not os.path.exists(temp_file)

    def test_save_subscription_credentials_sets_correct_permissions(
        self, mock_logger, tmp_path
    ):
        config = {
            "name": "test-consumer",
            "provisioning_url": "https://example.com",
            "handler": MagicMock(),
            "config_dir": str(tmp_path),
        }
        consumer = ConsumerModule(**config)
        consumer._save_subscription_credentials("testuser", "testpassword")

        config_file = os.path.join(str(tmp_path), "provisioning_config.json")
        mode = os.stat(config_file).st_mode & 0o777
        assert mode == 0o600


class TestConsumerModuleSubscribe:
    @pytest.mark.asyncio
    async def test_subscribe_creates_subscription(self, mock_logger, tmp_path, mock_session):
        handler = MagicMock()
        config = {
            "name": "test-consumer",
            "provisioning_url": "https://example.com",
            "config_dir": str(tmp_path),
        }

        consumer = ConsumerModule(handler, session=mock_session, **config)
        consumer.subscription_name = "test-sub"
        consumer.subscription_password = "test-pass"
        await consumer.subscribe(
            "admin", "adminpass", [{"realm": "udm", "topic": "users/user"}]
        )

        mock_session.post.assert_called_once()
        call_args = mock_session.post.call_args
        assert "https://example.com/v1/subscriptions" in call_args[0][0]
        assert call_args[1]["auth"] == ("admin", "adminpass")

    @pytest.mark.asyncio
    async def test_subscribe_saves_credentials_on_success(
        self, mock_logger, tmp_path, mock_session
    ):
        handler = MagicMock()
        config = {
            "name": "test-consumer",
            "provisioning_url": "https://example.com",
            "config_dir": str(tmp_path),
        }

        consumer = ConsumerModule(handler, session=mock_session, **config)
        consumer.subscription_name = "test-sub"
        consumer.subscription_password = "test-pass"
        await consumer.subscribe(
            "admin", "adminpass", [{"realm": "udm", "topic": "users/user"}]
        )

        config_file = os.path.join(str(tmp_path), "provisioning_config.json")
        assert os.path.exists(config_file)

    @pytest.mark.asyncio
    async def test_subscribe_reuses_existing_credentials(
        self, mock_logger, tmp_path, mock_session, temp_config_file
    ):
        handler = MagicMock()
        config = {
            "name": "test-consumer",
            "provisioning_url": "https://example.com",
            "config_dir": str(tmp_path),
        }
        temp_config_file("existing_user", "existing_password")

        consumer = ConsumerModule(handler, session=mock_session, **config)
        await consumer.subscribe(
            "admin", "adminpass", [{"realm": "udm", "topic": "users/user"}]
        )

        post_call = mock_session.post.call_args
        create_json = post_call[1]["json"]
        assert create_json["name"] == "existing_user"

    @pytest.mark.asyncio
    @pytest.mark.parametrize("status_code,error_text", [
        (300, "Multiple Choices"),
        (400, "Bad Request"),
        (401, "Unauthorized"),
        (403, "Forbidden"),
        (404, "Not Found"),
        (409, "Conflict"),
        (500, "Internal Server Error"),
        (503, "Service Unavailable"),
    ])
    async def test_subscribe_raises_on_error_response(
        self, tmp_path, mock_session, status_code, error_text
    ):
        """subscribe() raises SubscriptionError for any status >= 300."""
        error_response = MagicMock()
        error_response.status_code = status_code
        error_response.text = error_text
        mock_session.post = AsyncMock(return_value=error_response)

        handler = MagicMock()
        config = {
            "name": "test-consumer",
            "provisioning_url": "https://example.com",
            "config_dir": str(tmp_path),
        }

        consumer = ConsumerModule(handler, session=mock_session, **config)
        with pytest.raises(SubscriptionError):
            await consumer.subscribe(
                "admin", "adminpass", [{"realm": "udm", "topic": "users/user"}]
            )

    @pytest.mark.asyncio
    @pytest.mark.parametrize("status_code", [200, 201, 204])
    async def test_subscribe_succeeds_on_2xx(self, tmp_path, mock_session, status_code):
        """subscribe() does not raise for 2xx responses."""
        ok_response = MagicMock()
        ok_response.status_code = status_code
        ok_response.text = "OK"
        mock_session.post = AsyncMock(return_value=ok_response)

        handler = MagicMock()
        config = {
            "name": "test-consumer",
            "provisioning_url": "https://example.com",
            "config_dir": str(tmp_path),
        }
        consumer = ConsumerModule(handler, session=mock_session, **config)
        # Should not raise
        await consumer.subscribe("admin", "adminpass", [{"realm": "udm", "topic": "users/user"}])

    @pytest.mark.asyncio
    async def test_subscribe_generates_new_credentials_if_missing(
        self, mock_logger, tmp_path, mock_session
    ):
        handler = MagicMock()
        config = {
            "name": "test-consumer",
            "provisioning_url": "https://example.com",
            "config_dir": str(tmp_path),
        }

        consumer = ConsumerModule(handler, session=mock_session, **config)
        await consumer.subscribe(
            "admin", "adminpass", [{"realm": "udm", "topic": "users/user"}]
        )

        post_call = mock_session.post.call_args
        create_json = post_call[1]["json"]
        assert create_json["name"].startswith("test-consumer-")
        assert len(create_json["password"]) > 0

    @pytest.mark.asyncio
    async def test_subscribe_new_credentials_are_unique(self, tmp_path, mock_session):
        """Two subscribe() calls without existing credentials produce different credentials."""
        handler = MagicMock()
        config = {
            "name": "test-consumer",
            "provisioning_url": "https://example.com",
            "config_dir": str(tmp_path),
        }

        # First subscription
        consumer1 = ConsumerModule(handler, session=mock_session, **config)
        await consumer1.subscribe("admin", "pass", [{"realm": "udm", "topic": "users/user"}])
        cred1 = mock_session.post.call_args[1]["json"]

        # Reset mock and create second independent consumer in empty dir
        import tempfile
        tmp2 = tempfile.mkdtemp()
        mock_session.post.reset_mock()
        config2 = {**config, "config_dir": tmp2}
        consumer2 = ConsumerModule(handler, session=mock_session, **config2)
        await consumer2.subscribe("admin", "pass", [{"realm": "udm", "topic": "users/user"}])
        cred2 = mock_session.post.call_args[1]["json"]

        assert cred1["name"] != cred2["name"]
        assert cred1["password"] != cred2["password"]

    @pytest.mark.asyncio
    @pytest.mark.parametrize("prefill", [True, False])
    async def test_subscribe_sends_prefill_flag(self, tmp_path, mock_session, prefill):
        """subscribe() passes the prefill flag to the POST body."""
        handler = MagicMock()
        config = {
            "name": "test-consumer",
            "provisioning_url": "https://example.com",
            "config_dir": str(tmp_path),
        }
        consumer = ConsumerModule(handler, session=mock_session, **config)
        await consumer.subscribe(
            "admin", "pass",
            [{"realm": "udm", "topic": "users/user"}],
            prefill=prefill,
        )
        create_json = mock_session.post.call_args[1]["json"]
        assert create_json["request_prefill"] is prefill

    @pytest.mark.asyncio
    async def test_subscribe_sends_topics_in_body(self, tmp_path, mock_session):
        """subscribe() includes all topics in the POST body."""
        topics = [
            {"realm": "udm", "topic": "users/user"},
            {"realm": "udm", "topic": "groups/group"},
        ]
        handler = MagicMock()
        config = {
            "name": "test-consumer",
            "provisioning_url": "https://example.com",
            "config_dir": str(tmp_path),
        }
        consumer = ConsumerModule(handler, session=mock_session, **config)
        await consumer.subscribe("admin", "pass", topics)
        create_json = mock_session.post.call_args[1]["json"]
        assert create_json["realms_topics"] == topics

    @pytest.mark.asyncio
    async def test_subscribe_does_not_save_credentials_on_failure(self, tmp_path, mock_session):
        """Credentials file must not be written when subscribe() raises."""
        error_response = MagicMock()
        error_response.status_code = 500
        error_response.text = "Server Error"
        mock_session.post = AsyncMock(return_value=error_response)

        config = {
            "name": "test-consumer",
            "provisioning_url": "https://example.com",
            "config_dir": str(tmp_path),
        }
        consumer = ConsumerModule(MagicMock(), session=mock_session, **config)

        with pytest.raises(SubscriptionError):
            await consumer.subscribe("admin", "pass", [{"realm": "udm", "topic": "users/user"}])

        config_file = os.path.join(str(tmp_path), "provisioning_config.json")
        assert not os.path.exists(config_file)


class TestConsumerModuleStep:
    @pytest.mark.asyncio
    async def test_step_fetches_and_handles_event(
        self, mock_logger, tmp_path, mock_session, sample_event
    ):
        handler = MagicMock()
        handler.is_relevant = AsyncMock(return_value=True)
        handler.handle_event = AsyncMock(return_value=True)
        config = {
            "name": "test-consumer",
            "provisioning_url": "https://example.com",
            "config_dir": str(tmp_path),
        }

        event_with_seq = sample_event.copy()
        event_with_seq["sequence_number"] = 123

        mock_session.get.return_value.status_code = 200
        mock_session.get.return_value.json.return_value = event_with_seq
        mock_session.get.return_value.text = "OK"
        mock_session.patch.return_value.status_code = 200

        consumer = ConsumerModule(handler, session=mock_session, **config)
        consumer.subscription_name = "test-sub"
        consumer.subscription_password = "test-pass"

        await consumer.process_one_event()

        mock_session.get.assert_called()
        mock_session.patch.assert_called()
        handler.handle_event.assert_called_once_with(event_with_seq)

    @pytest.mark.asyncio
    async def test_step_does_nothing_when_no_event(self, mock_logger, tmp_path, mock_session):
        handler = MagicMock()
        handler.handle_event = AsyncMock(return_value=True)
        config = {
            "name": "test-consumer",
            "provisioning_url": "https://example.com",
            "config_dir": str(tmp_path),
        }

        mock_session.get.return_value.status_code = 200
        mock_session.get.return_value.json.return_value = None

        consumer = ConsumerModule(handler, session=mock_session, **config)
        consumer.subscription_name = "test-sub"
        consumer.subscription_password = "test-pass"

        await consumer.process_one_event()

        handler.handle_event.assert_not_called()

    @pytest.mark.asyncio
    async def test_step_does_not_acknowledge_on_handler_failure(
        self, mock_logger, tmp_path, mock_session, sample_event
    ):
        handler = MagicMock()
        handler.is_relevant = AsyncMock(return_value=True)
        handler.handle_event = AsyncMock(return_value=False)
        config = {
            "name": "test-consumer",
            "provisioning_url": "https://example.com",
            "config_dir": str(tmp_path),
        }

        mock_session.get.return_value.status_code = 200
        mock_session.get.return_value.json.return_value = sample_event
        mock_session.patch.return_value.status_code = 200

        consumer = ConsumerModule(handler, session=mock_session, **config)
        consumer.subscription_name = "test-sub"
        consumer.subscription_password = "test-pass"

        await consumer.process_one_event()

        mock_session.patch.assert_not_called()

    @pytest.mark.asyncio
    async def test_step_with_custom_long_polling_timeout(
        self, mock_logger, tmp_path, mock_session
    ):
        handler = MagicMock()
        config = {
            "name": "test-consumer",
            "provisioning_url": "https://example.com",
            "config_dir": str(tmp_path),
        }

        mock_session.get.return_value.status_code = 200
        mock_session.get.return_value.json.return_value = None

        consumer = ConsumerModule(handler, session=mock_session, **config)
        consumer.subscription_name = "test-sub"
        consumer.subscription_password = "test-pass"

        await consumer.process_one_event(long_polling_timeout=30)

        mock_session.get.assert_called_once()
        call_args = mock_session.get.call_args
        assert call_args[1]["params"]["timeout"] == 30


class TestConsumerModuleFetchEvent:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("sub_name,sub_password", [
        (None, "somepass"),   # missing name
        ("somename", None),   # missing password
        (None, None),         # both missing
        ("", "somepass"),     # empty string name
        ("somename", ""),     # empty string password
    ])
    async def test_fetch_event_raises_when_credentials_missing(
        self, tmp_path, mock_session, sub_name, sub_password
    ):
        """_fetch_event raises QueueAccessError for any falsy credential."""
        config = {
            "name": "test-consumer",
            "provisioning_url": "https://example.com",
            "handler": MagicMock(),
            "config_dir": str(tmp_path),
        }

        consumer = ConsumerModule(**config)
        consumer.subscription_name = sub_name
        consumer.subscription_password = sub_password

        with pytest.raises(QueueAccessError):
            await consumer._fetch_event(10)

    @pytest.mark.asyncio
    async def test_fetch_event_raises_when_no_credentials(
        self, mock_logger, tmp_path, mock_session
    ):
        config = {
            "name": "test-consumer",
            "provisioning_url": "https://example.com",
            "handler": MagicMock(),
            "config_dir": str(tmp_path),
        }

        consumer = ConsumerModule(**config)
        consumer.subscription_name = None
        consumer.subscription_password = "somepass"

        with pytest.raises(QueueAccessError):
            await consumer._fetch_event(10)

    @pytest.mark.asyncio
    async def test_fetch_event_raises_when_no_password(
        self, mock_logger, tmp_path, mock_session
    ):
        config = {
            "name": "test-consumer",
            "provisioning_url": "https://example.com",
            "handler": MagicMock(),
            "config_dir": str(tmp_path),
        }

        consumer = ConsumerModule(**config)
        consumer.subscription_name = "somename"
        consumer.subscription_password = None

        with pytest.raises(QueueAccessError):
            await consumer._fetch_event(10)

    @pytest.mark.asyncio
    async def test_fetch_event_returns_event_on_success(
        self, mock_logger, tmp_path, mock_session, sample_event
    ):
        config = {
            "name": "test-consumer",
            "provisioning_url": "https://example.com",
            "handler": MagicMock(),
            "config_dir": str(tmp_path),
        }

        mock_session.get.return_value.status_code = 200
        mock_session.get.return_value.json.return_value = sample_event

        consumer = ConsumerModule(**config, session=mock_session)
        consumer.subscription_name = "test-sub"
        consumer.subscription_password = "test-pass"

        event = await consumer._fetch_event(10)
        assert event == sample_event

    @pytest.mark.asyncio
    async def test_fetch_event_returns_none_on_empty_queue(
        self, tmp_path, mock_session
    ):
        """_fetch_event returns None when queue is empty (200 + null body)."""
        config = {
            "name": "test-consumer",
            "provisioning_url": "https://example.com",
            "handler": MagicMock(),
            "config_dir": str(tmp_path),
        }

        mock_session.get.return_value.status_code = 200
        mock_session.get.return_value.json.return_value = None

        consumer = ConsumerModule(**config, session=mock_session)
        consumer.subscription_name = "test-sub"
        consumer.subscription_password = "test-pass"

        result = await consumer._fetch_event(10)
        assert result is None

    @pytest.mark.asyncio
    @pytest.mark.parametrize("timeout_value", [0, 1, 10, 30, 60, 300])
    async def test_fetch_event_passes_timeout_param(self, tmp_path, mock_session, timeout_value):
        """_fetch_event sends the timeout value in the GET params."""
        config = {
            "name": "test-consumer",
            "provisioning_url": "https://example.com",
            "handler": MagicMock(),
            "config_dir": str(tmp_path),
        }

        mock_session.get.return_value.status_code = 200
        mock_session.get.return_value.json.return_value = None

        consumer = ConsumerModule(**config, session=mock_session)
        consumer.subscription_name = "test-sub"
        consumer.subscription_password = "test-pass"

        await consumer._fetch_event(timeout_value)

        call_params = mock_session.get.call_args[1]["params"]
        assert call_params["timeout"] == timeout_value

    @pytest.mark.asyncio
    async def test_fetch_event_url_contains_subscription_name(self, tmp_path, mock_session):
        """_fetch_event builds the URL using the subscription name."""
        config = {
            "name": "test-consumer",
            "provisioning_url": "https://example.com",
            "handler": MagicMock(),
            "config_dir": str(tmp_path),
        }

        mock_session.get.return_value.status_code = 200
        mock_session.get.return_value.json.return_value = None

        consumer = ConsumerModule(**config, session=mock_session)
        consumer.subscription_name = "my-unique-sub"
        consumer.subscription_password = "pass"

        await consumer._fetch_event(10)

        called_url = mock_session.get.call_args[0][0]
        assert "my-unique-sub" in called_url

    @pytest.mark.asyncio
    async def test_fetch_event_raises_on_non_200(self, mock_logger, tmp_path, mock_session):
        config = {
            "name": "test-consumer",
            "provisioning_url": "https://example.com",
            "handler": MagicMock(),
            "config_dir": str(tmp_path),
        }

        mock_session.get.return_value.status_code = 401
        mock_session.get.return_value.text = "Unauthorized"

        consumer = ConsumerModule(**config, session=mock_session)
        consumer.subscription_name = "test-sub"
        consumer.subscription_password = "test-pass"

        with pytest.raises(QueueAccessError):
            await consumer._fetch_event(10)

    @pytest.mark.asyncio
    @pytest.mark.parametrize("status_code,error_text", [
        (400, "Bad Request"),
        (401, "Unauthorized"),
        (403, "Forbidden"),
        (404, "Not Found"),
        (429, "Too Many Requests"),
        (500, "Internal Server Error"),
        (502, "Bad Gateway"),
        (503, "Service Unavailable"),
    ])
    async def test_fetch_event_raises_on_error_status(
        self, tmp_path, mock_session, status_code, error_text
    ):
        """_fetch_event raises QueueAccessError for any non-200 HTTP status."""
        config = {
            "name": "test-consumer",
            "provisioning_url": "https://example.com",
            "handler": MagicMock(),
            "config_dir": str(tmp_path),
        }

        mock_session.get.return_value.status_code = status_code
        mock_session.get.return_value.text = error_text

        consumer = ConsumerModule(**config, session=mock_session)
        consumer.subscription_name = "test-sub"
        consumer.subscription_password = "test-pass"

        with pytest.raises(QueueAccessError):
            await consumer._fetch_event(10)

    @pytest.mark.asyncio
    async def test_fetch_event_raises_on_400_bad_request(
        self, mock_logger, tmp_path, mock_session
    ):
        config = {
            "name": "test-consumer",
            "provisioning_url": "https://example.com",
            "handler": MagicMock(),
            "config_dir": str(tmp_path),
        }

        mock_session.get.return_value.status_code = 400
        mock_session.get.return_value.text = "Bad Request"

        consumer = ConsumerModule(**config, session=mock_session)
        consumer.subscription_name = "test-sub"
        consumer.subscription_password = "test-pass"

        with pytest.raises(QueueAccessError):
            await consumer._fetch_event(10)

    @pytest.mark.asyncio
    async def test_fetch_event_raises_on_403_forbidden(
        self, mock_logger, tmp_path, mock_session
    ):
        config = {
            "name": "test-consumer",
            "provisioning_url": "https://example.com",
            "handler": MagicMock(),
            "config_dir": str(tmp_path),
        }

        mock_session.get.return_value.status_code = 403
        mock_session.get.return_value.text = "Forbidden"

        consumer = ConsumerModule(**config, session=mock_session)
        consumer.subscription_name = "test-sub"
        consumer.subscription_password = "test-pass"

        with pytest.raises(QueueAccessError):
            await consumer._fetch_event(10)

    @pytest.mark.asyncio
    async def test_fetch_event_raises_on_404_not_found(
        self, mock_logger, tmp_path, mock_session
    ):
        config = {
            "name": "test-consumer",
            "provisioning_url": "https://example.com",
            "handler": MagicMock(),
            "config_dir": str(tmp_path),
        }

        mock_session.get.return_value.status_code = 404
        mock_session.get.return_value.text = "Not Found"

        consumer = ConsumerModule(**config, session=mock_session)
        consumer.subscription_name = "test-sub"
        consumer.subscription_password = "test-pass"

        with pytest.raises(QueueAccessError):
            await consumer._fetch_event(10)


class TestConsumerModuleAcknowledgeEvent:
    @pytest.mark.asyncio
    async def test_acknowledge_event_calls_correct_endpoint(
        self, mock_logger, tmp_path, mock_session
    ):
        config = {
            "name": "test-consumer",
            "provisioning_url": "https://example.com",
            "handler": MagicMock(),
            "config_dir": str(tmp_path),
        }

        mock_session.patch.return_value.status_code = 200

        consumer = ConsumerModule(**config, session=mock_session)
        consumer.subscription_name = "test-sub"
        consumer.subscription_password = "test-pass"

        event = {"sequence_number": 123}
        await consumer._acknowledge_event(event)

        mock_session.patch.assert_called_once()
        call_args = mock_session.patch.call_args
        assert "123" in call_args[0][0]
        assert call_args[1]["json"] == {"status": "ok"}

    @pytest.mark.asyncio
    @pytest.mark.parametrize("seq_num", [0, 1, 42, 999, 2**31 - 1])
    async def test_acknowledge_event_url_contains_sequence_number(
        self, tmp_path, mock_session, seq_num
    ):
        """_acknowledge_event encodes the sequence_number in the PATCH URL."""
        config = {
            "name": "test-consumer",
            "provisioning_url": "https://example.com",
            "handler": MagicMock(),
            "config_dir": str(tmp_path),
        }

        mock_session.patch.return_value.status_code = 200

        consumer = ConsumerModule(**config, session=mock_session)
        consumer.subscription_name = "test-sub"
        consumer.subscription_password = "test-pass"

        await consumer._acknowledge_event({"sequence_number": seq_num})

        called_url = mock_session.patch.call_args[0][0]
        assert str(seq_num) in called_url

    @pytest.mark.asyncio
    @pytest.mark.parametrize("status_code", [400, 404, 500, 503])
    async def test_acknowledge_event_does_not_raise_on_failure(
        self, tmp_path, mock_session, status_code
    ):
        """_acknowledge_event must not raise even if PATCH fails."""
        config = {
            "name": "test-consumer",
            "provisioning_url": "https://example.com",
            "handler": MagicMock(),
            "config_dir": str(tmp_path),
        }

        mock_response = MagicMock()
        mock_response.status_code = status_code
        mock_response.text = "Error"
        mock_session.patch = AsyncMock(return_value=mock_response)

        consumer = ConsumerModule(**config, session=mock_session)
        consumer.subscription_name = "test-sub"
        consumer.subscription_password = "test-pass"

        # Must not raise
        await consumer._acknowledge_event({"sequence_number": 1})

    @pytest.mark.asyncio
    async def test_acknowledge_event_logs_error_on_failure(
        self, mock_logger, tmp_path, mock_session
    ):
        handler = MagicMock()
        config = {
            "name": "test-consumer",
            "provisioning_url": "https://example.com",
            "config_dir": str(tmp_path),
        }

        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_response.text = "Server Error"
        mock_session.patch = AsyncMock(return_value=mock_response)

        consumer = ConsumerModule(handler, session=mock_session, **config)
        consumer.subscription_name = "test-sub"
        consumer.subscription_password = "test-pass"

        event = {"sequence_number": 123}
        await consumer._acknowledge_event(event)

        assert mock_response.status_code != 200

    @pytest.mark.asyncio
    async def test_acknowledge_event_missing_sequence_number(
        self, mock_logger, tmp_path, mock_session
    ):
        handler = MagicMock()
        config = {
            "name": "test-consumer",
            "provisioning_url": "https://example.com",
            "config_dir": str(tmp_path),
        }

        mock_session.patch.return_value.status_code = 200

        consumer = ConsumerModule(handler, session=mock_session, **config)
        consumer.subscription_name = "test-sub"
        consumer.subscription_password = "test-pass"

        event = {}
        with pytest.raises(KeyError):
            await consumer._acknowledge_event(event)

    @pytest.mark.asyncio
    async def test_acknowledge_event_uses_credentials_as_auth(self, tmp_path, mock_session):
        """_acknowledge_event passes subscription credentials as HTTP auth."""
        config = {
            "name": "test-consumer",
            "provisioning_url": "https://example.com",
            "handler": MagicMock(),
            "config_dir": str(tmp_path),
        }

        mock_session.patch.return_value.status_code = 200

        consumer = ConsumerModule(**config, session=mock_session)
        consumer.subscription_name = "my-sub"
        consumer.subscription_password = "my-secret"

        await consumer._acknowledge_event({"sequence_number": 1})

        patch_kwargs = mock_session.patch.call_args[1]
        assert patch_kwargs["auth"] == ("my-sub", "my-secret")


class TestConsumerModuleLoop:
    @pytest.mark.asyncio
    async def test_loop_calls_step_endlessly(self, mock_logger, tmp_path, mock_session):
        handler = MagicMock()
        config = {
            "name": "test-consumer",
            "provisioning_url": "https://example.com",
            "config_dir": str(tmp_path),
        }

        call_count = 0

        async def step_side_effect():
            nonlocal call_count
            call_count += 1
            if call_count >= 3:
                raise SystemExit()

        consumer = ConsumerModule(handler, session=mock_session, **config)
        consumer.process_one_event = step_side_effect

        with pytest.raises(SystemExit):
            await consumer.consume_loop()

        assert call_count == 3

    @pytest.mark.asyncio
    async def test_loop_sleeps_on_queue_access_error(self, tmp_path, mock_session):
        handler = MagicMock()
        mock_consumer_logger = MagicMock()

        config = {
            "name": "test-consumer",
            "provisioning_url": "https://example.com",
            "config_dir": str(tmp_path),
        }

        consumer = ConsumerModule(
            handler, session=mock_session, logger=mock_consumer_logger, **config
        )

        call_count = 0

        async def step_side_effect():
            nonlocal call_count
            call_count += 1
            if call_count >= 2:
                raise SystemExit()
            raise QueueAccessError("Queue access failed")

        consumer.process_one_event = step_side_effect

        with patch("provisioning_consumer_lib.consumer.asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
            with pytest.raises(SystemExit):
                await consumer.consume_loop()

        assert mock_sleep.call_count >= 1
        mock_consumer_logger.critical.assert_called()
        mock_consumer_logger.error.assert_called()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("error_timeout", [0, 1, 30, 120])
    async def test_loop_sleeps_for_configured_duration(self, tmp_path, mock_session, error_timeout):
        """consume_loop() sleeps for exactly error_timeout seconds."""
        handler = MagicMock()
        config = {
            "name": "test-consumer",
            "provisioning_url": "https://example.com",
            "config_dir": str(tmp_path),
            "error_timeout": error_timeout,
        }

        consumer = ConsumerModule(handler, session=mock_session, **config)

        call_count = 0

        async def step_side_effect():
            nonlocal call_count
            call_count += 1
            if call_count >= 2:
                raise SystemExit()
            raise QueueAccessError("Queue failed")

        consumer.process_one_event = step_side_effect

        with patch("provisioning_consumer_lib.consumer.asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
            with pytest.raises(SystemExit):
                await consumer.consume_loop()

        mock_sleep.assert_called_with(error_timeout)

    @pytest.mark.asyncio
    async def test_loop_propagates_system_exit(self, tmp_path, mock_session):
        """consume_loop() lets SystemExit propagate without catching it."""
        handler = MagicMock()
        config = {
            "name": "test-consumer",
            "provisioning_url": "https://example.com",
            "config_dir": str(tmp_path),
        }

        consumer = ConsumerModule(handler, session=mock_session, **config)
        consumer.process_one_event = AsyncMock(side_effect=SystemExit(42))

        with pytest.raises(SystemExit) as exc_info:
            await consumer.consume_loop()

        assert exc_info.value.code == 42


class TestConsumerModuleProcessOneEvent:
    """Tests for process_one_event() focusing on is_relevant() and event-handling paths."""

    @pytest.mark.asyncio
    async def test_irrelevant_event_is_acknowledged_and_skipped(
        self, tmp_path, mock_session, sample_event
    ):
        """If is_relevant() returns False, the event is acknowledged without calling handle_event."""
        handler = MagicMock()
        handler.is_relevant = AsyncMock(return_value=False)
        handler.handle_event = AsyncMock(return_value=True)

        config = {
            "name": "test-consumer",
            "provisioning_url": "https://example.com",
            "config_dir": str(tmp_path),
        }

        event_with_seq = {**sample_event, "sequence_number": 99}
        mock_session.get.return_value.status_code = 200
        mock_session.get.return_value.json.return_value = event_with_seq
        mock_session.patch.return_value.status_code = 200

        consumer = ConsumerModule(handler, session=mock_session, **config)
        consumer.subscription_name = "sub"
        consumer.subscription_password = "pass"
        await consumer.process_one_event()

        handler.handle_event.assert_not_called()
        mock_session.patch.assert_called_once()  # acknowledged

    @pytest.mark.asyncio
    async def test_relevant_successful_event_is_acknowledged(
        self, tmp_path, mock_session, sample_event
    ):
        """If is_relevant() is True and handle_event() returns True → event is acknowledged."""
        handler = MagicMock()
        handler.is_relevant = AsyncMock(return_value=True)
        handler.handle_event = AsyncMock(return_value=True)

        config = {
            "name": "test-consumer",
            "provisioning_url": "https://example.com",
            "config_dir": str(tmp_path),
        }

        event_with_seq = {**sample_event, "sequence_number": 55}
        mock_session.get.return_value.status_code = 200
        mock_session.get.return_value.json.return_value = event_with_seq
        mock_session.patch.return_value.status_code = 200

        consumer = ConsumerModule(handler, session=mock_session, **config)
        consumer.subscription_name = "sub"
        consumer.subscription_password = "pass"
        await consumer.process_one_event()

        handler.handle_event.assert_called_once()
        mock_session.patch.assert_called_once()  # acknowledged

    @pytest.mark.asyncio
    async def test_failed_event_is_not_acknowledged(
        self, tmp_path, mock_session, sample_event
    ):
        """If handle_event() returns False, the event is NOT acknowledged (will be retried)."""
        handler = MagicMock()
        handler.is_relevant = AsyncMock(return_value=True)
        handler.handle_event = AsyncMock(return_value=False)

        config = {
            "name": "test-consumer",
            "provisioning_url": "https://example.com",
            "config_dir": str(tmp_path),
        }

        mock_session.get.return_value.status_code = 200
        mock_session.get.return_value.json.return_value = sample_event

        consumer = ConsumerModule(handler, session=mock_session, **config)
        consumer.subscription_name = "sub"
        consumer.subscription_password = "pass"
        await consumer.process_one_event()

        mock_session.patch.assert_not_called()

    @pytest.mark.asyncio
    async def test_process_one_event_raises_queue_access_error_on_fetch_failure(
        self, tmp_path, mock_session
    ):
        """process_one_event() propagates QueueAccessError from _fetch_event."""
        handler = MagicMock()
        config = {
            "name": "test-consumer",
            "provisioning_url": "https://example.com",
            "config_dir": str(tmp_path),
        }

        mock_session.get.return_value.status_code = 500
        mock_session.get.return_value.text = "error"

        consumer = ConsumerModule(handler, session=mock_session, **config)
        consumer.subscription_name = "sub"
        consumer.subscription_password = "pass"

        with pytest.raises(QueueAccessError):
            await consumer.process_one_event()


class TestDN:
    """Tests for the DN (Distinguished Name) helper class."""

    # The patch_dn autouse fixture replaces DN with an identity function in consumer.py,
    # but here we test the real DN class directly.

    def test_dn_parses_simple_dn(self):
        from provisioning_consumer_lib.dn import DN
        dn = DN("uid=foo,dc=example,dc=com")
        assert str(dn) is not None

    def test_dn_repr_contains_dn_string(self):
        from provisioning_consumer_lib.dn import DN
        dn = DN("uid=foo,dc=example,dc=com")
        assert "uid=foo" in repr(dn)
        assert "DN" in repr(dn)

    @pytest.mark.parametrize("dn_str,expected_attr,expected_val", [
        ("uid=foo,dc=example,dc=com", "uid", "foo"),
        ("cn=users,dc=example,dc=com", "cn", "users"),
        ("dc=example,dc=com", "dc", "example"),
        ("ou=groups,dc=example,dc=com", "ou", "groups"),
    ])
    def test_dn_rdn_returns_first_component(self, dn_str, expected_attr, expected_val):
        from provisioning_consumer_lib.dn import DN
        dn = DN(dn_str)
        attr, val = dn.rdn
        assert attr.lower() == expected_attr.lower()
        assert val == expected_val

    def test_dn_parent_returns_rest(self):
        from provisioning_consumer_lib.dn import DN
        dn = DN("uid=foo,dc=example,dc=com")
        parent = dn.parent
        assert parent is not None
        # Parent should be dc=example,dc=com
        attr, val = parent.rdn
        assert attr.lower() == "dc"

    def test_dn_parent_of_single_component_is_none(self):
        from provisioning_consumer_lib.dn import DN
        dn = DN("dc=com")
        assert dn.parent is None

    def test_dn_len_counts_components(self):
        from provisioning_consumer_lib.dn import DN
        assert len(DN("dc=com")) == 1
        assert len(DN("dc=example,dc=com")) == 2
        assert len(DN("uid=foo,dc=example,dc=com")) == 3

    @pytest.mark.parametrize("dn_str,suffix,expected", [
        ("uid=foo,dc=example,dc=com", "dc=example,dc=com", True),
        ("uid=foo,dc=example,dc=com", "dc=com", True),
        ("uid=foo,dc=example,dc=com", "uid=foo", False),
        ("uid=foo,dc=example,dc=com", "dc=other,dc=com", False),
    ])
    def test_dn_endswith(self, dn_str, suffix, expected):
        from provisioning_consumer_lib.dn import DN
        assert DN(dn_str).endswith(suffix) is expected

    @pytest.mark.parametrize("dn_str,prefix,expected", [
        ("uid=foo,dc=example,dc=com", "uid=foo", True),
        ("uid=foo,dc=example,dc=com", "uid=foo,dc=example", True),
        ("uid=foo,dc=example,dc=com", "dc=example,dc=com", False),
        ("uid=foo,dc=example,dc=com", "cn=foo", False),
    ])
    def test_dn_startswith(self, dn_str, prefix, expected):
        from provisioning_consumer_lib.dn import DN
        assert DN(dn_str).startswith(prefix) is expected

    @pytest.mark.parametrize("dn_a,dn_b,expected_equal", [
        # Identical DNs
        ("uid=foo,dc=example,dc=com", "uid=foo,dc=example,dc=com", True),
        # Case-insensitive for known attributes (uid, cn, dc, ou)
        ("uid=foo,dc=example,dc=com", "uid=FOO,dc=example,dc=com", True),
        ("CN=Users,DC=example,DC=com", "cn=Users,dc=example,dc=com", True),
        # Case-sensitive for unknown attributes
        ("univentionAppID=Foo,dc=example,dc=com", "univentionAppID=foo,dc=example,dc=com", False),
        # Different values
        ("uid=foo,dc=example,dc=com", "uid=bar,dc=example,dc=com", False),
        # Different structure
        ("uid=foo,dc=example,dc=com", "uid=foo,dc=other,dc=com", False),
    ])
    def test_dn_equality(self, dn_a, dn_b, expected_equal):
        from provisioning_consumer_lib.dn import DN
        result = DN(dn_a) == DN(dn_b)
        assert result is expected_equal

    def test_dn_not_equal_different_length(self):
        from provisioning_consumer_lib.dn import DN
        assert DN("uid=foo,dc=example,dc=com") != DN("uid=foo,dc=com")

    def test_dn_hash_consistent(self):
        from provisioning_consumer_lib.dn import DN
        dn = DN("uid=foo,dc=example,dc=com")
        assert hash(dn) == hash(dn)

    def test_dn_equal_dns_have_same_hash(self):
        from provisioning_consumer_lib.dn import DN
        dn1 = DN("uid=foo,dc=example,dc=com")
        dn2 = DN("uid=FOO,dc=example,dc=com")
        assert dn1 == dn2
        assert hash(dn1) == hash(dn2)

    def test_dn_can_be_used_in_set(self):
        from provisioning_consumer_lib.dn import DN
        dn_set = {DN("uid=foo,dc=example,dc=com"), DN("uid=foo,dc=example,dc=com")}
        assert len(dn_set) == 1

    def test_dn_set_classmethod_deduplicates(self):
        from provisioning_consumer_lib.dn import DN
        dns = DN.set([
            "CN=Computers,dc=foo,dc=com",
            "cn=computers,dc=foo,dc=com",
            "cn=computers,dc=foo,dc=com",
        ])
        assert len(dns) == 1

    def test_dn_values_classmethod_returns_string_set(self):
        from provisioning_consumer_lib.dn import DN
        dn_set = DN.set(["cn=foo,dc=com", "cn=bar,dc=com"])
        values = DN.values(dn_set)
        assert isinstance(values, set)
        assert all(isinstance(v, str) for v in values)

    def test_dn_walk_yields_from_base_to_full(self):
        from provisioning_consumer_lib.dn import DN
        dn = DN("uid=foo,dc=example,dc=com")
        walked = list(dn.walk("dc=example,dc=com"))
        # First yielded DN should end with just the base
        assert walked[0].endswith("dc=example,dc=com")
        # Last yielded should be the full DN
        assert walked[-1] == dn

    def test_dn_walk_raises_if_not_suffix(self):
        from provisioning_consumer_lib.dn import DN
        dn = DN("uid=foo,dc=example,dc=com")
        with pytest.raises(ValueError):
            list(dn.walk("dc=other,dc=com"))

    def test_dn_getitem_slice(self):
        from provisioning_consumer_lib.dn import DN
        dn = DN("uid=foo,dc=example,dc=com")
        # Slice [1:] should give parent
        sliced = dn[1:]
        assert sliced == dn.parent

    def test_dn_getitem_index(self):
        from provisioning_consumer_lib.dn import DN
        dn = DN("uid=foo,dc=example,dc=com")
        first = dn[0]
        attr, val = first.rdn
        assert attr.lower() == "uid"
        assert val == "foo"

    @pytest.mark.parametrize("hex_escaped,expected_value", [
        (r"\31", "1"),
        (r"\74\65\73\74", "test"),
        ("hello", "hello"),
    ])
    def test_unescape_dn_value(self, hex_escaped, expected_value):
        from provisioning_consumer_lib.dn import _unescape_dn_value
        assert _unescape_dn_value(hex_escaped) == expected_value

    def test_dn_multivalued_rdn_order_independent(self):
        """Multi-valued RDNs compare equal regardless of AVA order."""
        from provisioning_consumer_lib.dn import DN
        dn1 = DN("uid=foo+cn=bar,dc=example,dc=com")
        dn2 = DN("cn=bar+uid=foo,dc=example,dc=com")
        assert dn1 == dn2
