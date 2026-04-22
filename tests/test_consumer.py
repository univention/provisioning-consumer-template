# SPDX-FileCopyrightText: 2026 Univention GmbH
# SPDX-License-Identifier: AGPL-3.0-only
import os
import json
import sys
import types
import tempfile
import pytest
from unittest.mock import MagicMock, patch, call

from provisioning_consumer.consumer import (
    UDMEventHandler,
    ConsumerModule,
    QueueAccessError,
    SubscriptionError,
)


class TestQueueAccessError:
    def test_is_exception(self):
        assert issubclass(QueueAccessError, Exception)

    def test_can_be_instantiated(self):
        err = QueueAccessError("test message")
        assert str(err) == "test message"


class TestSubscriptionError:
    def test_is_exception(self):
        assert issubclass(SubscriptionError, Exception)

    def test_can_be_instantiated(self):
        err = SubscriptionError("test message")
        assert str(err) == "test message"


class TestUDMEventHandler:
    def test_handle_event_routes_to_create(self, ConcreteEventHandler, mock_logger, sample_create_event):
        handler = ConcreteEventHandler(mock_logger)
        result = handler.handle_event(sample_create_event)
        assert result is True
        assert handler.create_called_with is not None
        assert handler.modify_called_with is None
        assert handler.remove_called_with is None

    def test_handle_event_routes_to_modify(self, ConcreteEventHandler, mock_logger, sample_event):
        handler = ConcreteEventHandler(mock_logger)
        result = handler.handle_event(sample_event)
        assert result is True
        assert handler.create_called_with is None
        assert handler.modify_called_with is not None
        assert handler.remove_called_with is None

    def test_handle_event_routes_to_remove(self, ConcreteEventHandler, mock_logger, sample_remove_event):
        handler = ConcreteEventHandler(mock_logger)
        result = handler.handle_event(sample_remove_event)
        assert result is True
        assert handler.create_called_with is None
        assert handler.modify_called_with is None
        assert handler.remove_called_with is not None

    def test_handle_event_returns_true_on_success(self, ConcreteEventHandler, mock_logger, sample_event):
        handler = ConcreteEventHandler(mock_logger)
        result = handler.handle_event(sample_event)
        assert result is True

    def test_handle_event_calls_error_handler_on_exception(self, ConcreteEventHandler, mock_logger, sample_event):
        def raise_error():
            raise ValueError("test error")

        handler = ConcreteEventHandler(mock_logger, modify_handler=raise_error)
        with pytest.raises(ValueError, match="test error"):
            handler.handle_event(sample_event)
        assert handler.error_called_with is not None
        metadata, old, new, exc_type, exc_value, exc_traceback = handler.error_called_with
        assert isinstance(exc_value, ValueError)
        assert str(exc_value) == "test error"

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

    def test_event_to_udm_detects_move(self, mock_logger, sample_move_event):
        with patch("provisioning_consumer.consumer.DN") as mock_dn:
            mock_dn.return_value = MagicMock()
            mock_dn.side_effect = lambda dn: dn

            metadata, old, new, has_moved = UDMEventHandler._event_to_udm(sample_move_event)
            assert has_moved is True

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


class TestConsumerModuleInit:
    def test_init_requires_handler(self, mock_logger, config_dict):
        from provisioning_consumer.consumer import EventHandler

        class MockHandler(EventHandler):
            def handle_event(self, event):
                return True

        handler = MockHandler(mock_logger)
        consumer = ConsumerModule(handler, **config_dict)
        assert consumer.handler is not None

    def test_init_validates_name_missing(self, mock_logger):
        config = {
            "name": "",
            "provisioning_url": "https://example.com",
            "handler": MagicMock(),
        }
        with pytest.raises(AssertionError):
            ConsumerModule(**config)

    def test_init_validates_name_none(self, mock_logger):
        config = {
            "name": None,
            "provisioning_url": "https://example.com",
            "handler": MagicMock(),
        }
        with pytest.raises(AssertionError):
            ConsumerModule(**config)

    def test_init_validates_provisioning_url_missing(self, mock_logger):
        config = {
            "name": "test-consumer",
            "provisioning_url": None,
            "handler": MagicMock(),
        }
        with pytest.raises(AssertionError):
            ConsumerModule(**config)

    def test_init_sets_defaults(self, mock_logger, tmp_path):
        config = {
            "name": "test-consumer",
            "provisioning_url": "https://example.com",
            "handler": MagicMock(),
            "config_path": str(tmp_path),
        }
        consumer = ConsumerModule(**config)
        assert consumer.config["name"] == "test-consumer"
        assert consumer.config["provisioning_url"] == "https://example.com"

    def test_init_normalizes_config_path(self, mock_logger, tmp_path):
        config = {
            "name": "test-consumer",
            "provisioning_url": "https://example.com",
            "handler": MagicMock(),
            "config_path": str(tmp_path) + "/",
        }
        consumer = ConsumerModule(**config)
        assert not consumer.config["config_path"].endswith("/")

    def test_repr(self, mock_logger, tmp_path):
        config = {
            "name": "test-consumer",
            "provisioning_url": "https://example.com",
            "handler": MagicMock(),
            "config_path": str(tmp_path),
        }
        consumer = ConsumerModule(**config)
        repr_str = repr(consumer)
        assert "ConsumerModule" in repr_str
        assert "test-consumer" in repr_str


class TestConsumerModuleCredentials:
    def test_get_subscription_credentials_reads_file(self, mock_logger, tmp_path, temp_config_file):
        config_path = str(tmp_path)
        temp_config_file("myuser", "mypassword")

        config = {
            "name": "test-consumer",
            "provisioning_url": "https://example.com",
            "handler": MagicMock(),
            "config_path": config_path,
        }
        consumer = ConsumerModule(**config)
        name, password = consumer._get_subscription_credentials()
        assert name == "myuser"
        assert password == "mypassword"

    def test_get_subscription_credentials_returns_none_when_missing(self, mock_logger, tmp_path):
        config = {
            "name": "test-consumer",
            "provisioning_url": "https://example.com",
            "handler": MagicMock(),
            "config_path": str(tmp_path),
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
            "config_path": str(tmp_path),
        }
        consumer = ConsumerModule(**config)
        consumer._save_subscription_credentials("testuser", "testpassword")

        config_file = os.path.join(str(tmp_path), "config.json")
        assert os.path.exists(config_file)
        with open(config_file) as f:
            data = json.load(f)
        assert data["subscription_name"] == "testuser"
        assert data["subscription_password"] == "testpassword"

    def test_save_subscription_credentials_sets_correct_permissions(self, mock_logger, tmp_path):
        config = {
            "name": "test-consumer",
            "provisioning_url": "https://example.com",
            "handler": MagicMock(),
            "config_path": str(tmp_path),
        }
        consumer = ConsumerModule(**config)
        consumer._save_subscription_credentials("testuser", "testpassword")

        config_file = os.path.join(str(tmp_path), "config.json")
        mode = os.stat(config_file).st_mode & 0o777
        assert mode == 0o600


class TestConsumerModuleSubscribe:
    def test_subscribe_creates_subscription(self, mock_logger, tmp_path, mock_session):
        handler = MagicMock()
        config = {
            "name": "test-consumer",
            "provisioning_url": "https://example.com",
            "config_path": str(tmp_path),
        }

        consumer = ConsumerModule(handler, session=mock_session, **config)
        consumer.subscription_name = "test-sub"
        consumer.subscription_password = "test-pass"
        consumer.subscribe("admin", "adminpass", [{"realm": "udm", "topic": "users/user"}])

        mock_session.post.assert_called_once()
        call_args = mock_session.post.call_args
        assert "https://example.com/v1/subscriptions" in call_args[0][0]
        assert call_args[1]["auth"] == ("admin", "adminpass")

    def test_subscribe_saves_credentials_on_success(self, mock_logger, tmp_path, mock_session):
        handler = MagicMock()
        config = {
            "name": "test-consumer",
            "provisioning_url": "https://example.com",
            "config_path": str(tmp_path),
        }

        consumer = ConsumerModule(handler, session=mock_session, **config)
        consumer.subscription_name = "test-sub"
        consumer.subscription_password = "test-pass"
        consumer.subscribe("admin", "adminpass", [{"realm": "udm", "topic": "users/user"}])

        config_file = os.path.join(str(tmp_path), "config.json")
        assert os.path.exists(config_file)

    def test_subscribe_reuses_existing_credentials(self, mock_logger, tmp_path, mock_session, temp_config_file):
        handler = MagicMock()
        config = {
            "name": "test-consumer",
            "provisioning_url": "https://example.com",
            "config_path": str(tmp_path),
        }
        temp_config_file("existing_user", "existing_password")

        consumer = ConsumerModule(handler, session=mock_session, **config)
        consumer.subscribe("admin", "adminpass", [{"realm": "udm", "topic": "users/user"}])

        post_call = mock_session.post.call_args
        create_json = post_call[1]["json"]
        assert create_json["name"] == "existing_user"

    def test_subscribe_raises_on_error_response(self, mock_logger, tmp_path, mock_session):
        error_response = MagicMock()
        error_response.status_code = 500
        error_response.text = "Internal Server Error"
        mock_session.post.return_value = error_response

        handler = MagicMock()
        config = {
            "name": "test-consumer",
            "provisioning_url": "https://example.com",
            "config_path": str(tmp_path),
        }

        consumer = ConsumerModule(handler, session=mock_session, **config)
        with pytest.raises(SubscriptionError):
            consumer.subscribe("admin", "adminpass", [{"realm": "udm", "topic": "users/user"}])

    def test_subscribe_generates_new_credentials_if_missing(self, mock_logger, tmp_path, mock_session):
        handler = MagicMock()
        config = {
            "name": "test-consumer",
            "provisioning_url": "https://example.com",
            "config_path": str(tmp_path),
        }

        consumer = ConsumerModule(handler, session=mock_session, **config)
        consumer.subscribe("admin", "adminpass", [{"realm": "udm", "topic": "users/user"}])

        post_call = mock_session.post.call_args
        create_json = post_call[1]["json"]
        assert create_json["name"].startswith("test-consumer-")
        assert len(create_json["password"]) > 0


class TestConsumerModuleStep:
    def test_step_fetches_and_handles_event(self, mock_logger, tmp_path, mock_session, sample_event):
        handler = MagicMock()
        handler.handle_event.return_value = True
        config = {
            "name": "test-consumer",
            "provisioning_url": "https://example.com",
            "config_path": str(tmp_path),
        }

        event_with_seq = sample_event.copy()
        event_with_seq["sequence_number"] = 123

        mock_session.get.return_value.status_code = 200
        mock_session.get.return_value.json.return_value = event_with_seq
        mock_session.get.return_value.text = "OK"

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_session.patch.return_value = mock_response

        with patch("provisioning_consumer.consumer.requests.Session", return_value=mock_session):
            consumer = ConsumerModule(handler, **config)
            consumer.subscription_name = "test-sub"
            consumer.subscription_password = "test-pass"

            consumer.step()

        mock_session.get.assert_called()
        mock_session.patch.assert_called()
        handler.handle_event.assert_called_once_with(event_with_seq)

    def test_step_does_nothing_when_no_event(self, mock_logger, tmp_path, mock_session):
        handler = MagicMock()
        config = {
            "name": "test-consumer",
            "provisioning_url": "https://example.com",
            "config_path": str(tmp_path),
        }

        mock_session.get.return_value.status_code = 200
        mock_session.get.return_value.json.return_value = None

        handler.handle_event.return_value = True

        with patch("provisioning_consumer.consumer.requests.Session", return_value=mock_session):
            consumer = ConsumerModule(handler, **config)
            consumer.subscription_name = "test-sub"
            consumer.subscription_password = "test-pass"

            consumer.step()

        handler.handle_event.assert_not_called()

    def test_step_does_not_acknowledge_on_handler_failure(self, mock_logger, tmp_path, mock_session, sample_event):
        handler = MagicMock()
        config = {
            "name": "test-consumer",
            "provisioning_url": "https://example.com",
            "config_path": str(tmp_path),
        }

        mock_session.get.return_value.status_code = 200
        mock_session.get.return_value.json.return_value = sample_event

        handler.handle_event.return_value = False

        mock_patch_response = MagicMock()
        mock_patch_response.status_code = 200
        mock_session.patch.return_value = mock_patch_response

        with patch("provisioning_consumer.consumer.requests.Session", return_value=mock_session):
            consumer = ConsumerModule(handler, **config)
            consumer.subscription_name = "test-sub"
            consumer.subscription_password = "test-pass"

            consumer.step()

        mock_session.patch.assert_not_called()


class TestConsumerModuleFetchEvent:
    def test_fetch_event_raises_when_no_credentials(self, mock_logger, tmp_path, mock_session):
        config = {
            "name": "test-consumer",
            "provisioning_url": "https://example.com",
            "handler": MagicMock(),
            "config_path": str(tmp_path),
        }

        with patch("provisioning_consumer.consumer.requests.Session", return_value=mock_session):
            consumer = ConsumerModule(**config)
            consumer.subscription_name = None
            consumer.subscription_password = "somepass"

            with pytest.raises(QueueAccessError):
                consumer._fetch_event(10)

    def test_fetch_event_raises_when_no_password(self, mock_logger, tmp_path, mock_session):
        config = {
            "name": "test-consumer",
            "provisioning_url": "https://example.com",
            "handler": MagicMock(),
            "config_path": str(tmp_path),
        }

        with patch("provisioning_consumer.consumer.requests.Session", return_value=mock_session):
            consumer = ConsumerModule(**config)
            consumer.subscription_name = "somename"
            consumer.subscription_password = None

            with pytest.raises(QueueAccessError):
                consumer._fetch_event(10)

    def test_fetch_event_returns_event_on_success(self, mock_logger, tmp_path, mock_session, sample_event):
        config = {
            "name": "test-consumer",
            "provisioning_url": "https://example.com",
            "handler": MagicMock(),
            "config_path": str(tmp_path),
        }

        mock_session.get.return_value.status_code = 200
        mock_session.get.return_value.json.return_value = sample_event

        with patch("provisioning_consumer.consumer.requests.Session", return_value=mock_session):
            consumer = ConsumerModule(**config)
            consumer.subscription_name = "test-sub"
            consumer.subscription_password = "test-pass"

            event = consumer._fetch_event(10)
            assert event == sample_event

    def test_fetch_event_raises_on_non_200(self, mock_logger, tmp_path, mock_session):
        config = {
            "name": "test-consumer",
            "provisioning_url": "https://example.com",
            "handler": MagicMock(),
            "config_path": str(tmp_path),
        }

        mock_session.get.return_value.status_code = 401
        mock_session.get.return_value.text = "Unauthorized"

        with patch("provisioning_consumer.consumer.requests.Session", return_value=mock_session):
            consumer = ConsumerModule(**config)
            consumer.subscription_name = "test-sub"
            consumer.subscription_password = "test-pass"

            with pytest.raises(QueueAccessError):
                consumer._fetch_event(10)


class TestConsumerModuleAcknowledgeEvent:
    def test_acknowledge_event_calls_correct_endpoint(self, mock_logger, tmp_path, mock_session):
        config = {
            "name": "test-consumer",
            "provisioning_url": "https://example.com",
            "handler": MagicMock(),
            "config_path": str(tmp_path),
        }

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_session.patch.return_value = mock_response

        with patch("provisioning_consumer.consumer.requests.Session", return_value=mock_session):
            consumer = ConsumerModule(**config)
            consumer.subscription_name = "test-sub"
            consumer.subscription_password = "test-pass"

            event = {"sequence_number": 123}
            consumer._acknowledge_event(event)

        mock_session.patch.assert_called_once()
        call_args = mock_session.patch.call_args
        assert "123" in call_args[0][0]
        assert call_args[1]["json"] == {"status": "ok"}

    def test_acknowledge_event_logs_error_on_failure(self, mock_logger, tmp_path, mock_session):
        handler = MagicMock()
        config = {
            "name": "test-consumer",
            "provisioning_url": "https://example.com",
            "config_path": str(tmp_path),
        }

        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_response.text = "Server Error"
        mock_session.patch.return_value = mock_response

        with patch("provisioning_consumer.consumer.requests.Session", return_value=mock_session):
            consumer = ConsumerModule(handler, **config)
            consumer.subscription_name = "test-sub"
            consumer.subscription_password = "test-pass"

            event = {"sequence_number": 123}
            consumer._acknowledge_event(event)

        assert mock_response.status_code != 200


class TestConsumerModuleLoop:
    def test_loop_calls_step_endlessly(self, mock_logger, tmp_path, mock_session):
        handler = MagicMock()
        config = {
            "name": "test-consumer",
            "provisioning_url": "https://example.com",
            "config_path": str(tmp_path),
        }

        call_count = 0

        def step_side_effect():
            nonlocal call_count
            call_count += 1
            if call_count >= 3:
                raise KeyboardInterrupt()

        consumer = ConsumerModule(handler, session=mock_session, **config)
        consumer.step = step_side_effect

        consumer.loop()

        assert call_count == 3

    def test_loop_sleeps_on_queue_access_error(self, mock_logger, tmp_path, mock_session):
        handler = MagicMock()
        config = {
            "name": "test-consumer",
            "provisioning_url": "https://example.com",
            "config_path": str(tmp_path),
        }

        call_count = 0

        def step_side_effect():
            nonlocal call_count
            call_count += 1
            if call_count >= 2:
                raise KeyboardInterrupt()
            raise QueueAccessError("Queue access failed")

        consumer = ConsumerModule(handler, session=mock_session, **config)
        consumer.step = step_side_effect

        with patch("provisioning_consumer.consumer.time.sleep") as mock_sleep:
            consumer.loop()

        assert mock_sleep.call_count >= 1
        mock_logger.critical.assert_called()
        mock_logger.error.assert_called()