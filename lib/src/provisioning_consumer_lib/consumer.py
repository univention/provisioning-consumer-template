# SPDX-FileCopyrightText: 2026 Univention GmbH
# SPDX-License-Identifier: AGPL-3.0-only

import copy
import json
import os
import secrets
import sys
import time
import types
from collections.abc import Iterable
from typing import TYPE_CHECKING, Any, TypedDict, TypeAlias
from .dn import DN

import loguru
import requests
from typing_extensions import override

if TYPE_CHECKING:
    from loguru import Logger

AttributeMapping: TypeAlias = dict[str, Any]
FILENAME_CONFIG = "provisioning_config.json"
DEFAULT_ERROR_TIMEOUT = (
    60  # sleep duration after failed provisioning queue access in seconds
)


class QueueAccessError(Exception):
    """
    Raised if access to provisioning queue fails.
    """

    pass


class Topics(TypedDict):
    realm: str
    topic: str


class QueryEventObject(TypedDict):
    publisher_name: str
    ts: str
    realm: str
    topic: str
    body: dict[str, Any]  # pyright: ignore[reportExplicitAny]
    sequence_number: int
    num_delivered: int


class Metadata(TypedDict):
    publisher_name: str
    ts: str
    realm: str
    topic: str
    sequence_number: int
    num_delivered: int


class SubscriptionError(Exception):
    """
    Raised when a subscription fails.
    """

    pass


class EventHandler:
    def __init__(self, logger: Logger | None, *args, **kwargs) -> None:
        self.logger: Logger = logger if logger is not None else loguru.logger

    def is_relevant(self, event: QueryEventObject) -> bool:
        """
        Indicates if the event shall be processed by handle_event().
        Can be used to filter the events.
        """
        return True

    def handle_event(self, event: QueryEventObject) -> bool:
        """
        Calls the handler functions depending on the event type.

        :param dict[str, Any] event: event to be processed
        :return: If no exception is thrown by the handler functions, True is returned, else False
        """
        raise NotImplementedError()


class UDMEventHandler(EventHandler):
    @override
    def handle_event(self, event: QueryEventObject) -> bool:
        """
        Calls the handler functions depending on the event type.

        :param dict[str, Any] event: event to be processed
        :return: If no exception is thrown by the handler functions, True is returned, else False
        """
        metadata, old, new, has_moved = self._event_to_udm(event)
        try:
            if old and new:
                self._handle_modify(metadata, old, new, has_moved)
            elif old:
                self._handle_remove(metadata, old)
            else:
                self._handle_create(metadata, new)
        except SystemExit:
            raise
        except Exception:  # noqa: E722
            exc_type, exc_value, exc_traceback = sys.exc_info()
            self._handle_error(metadata, old, new, exc_type, exc_value, exc_traceback)
            return False
        return True

    @classmethod
    def _event_to_udm(
        cls, event: QueryEventObject
    ) -> tuple[Metadata, AttributeMapping, AttributeMapping, bool]:
        """
        Converts the event to UDM data objects metadata, old and new.
        :param dict[str, Any] event: the event to be converted
        :returns: metadata, old, new
        :rtype: tuple[AttributeMapping, AttributeMapping, AttributeMapping]
        """
        metadata = copy.deepcopy(event)
        del metadata["body"]
        old = event["body"]["old"]
        new = event["body"]["new"]
        has_moved = False
        if old and new:
            old_dn = DN(old["dn"])
            new_dn = DN(new["dn"])
            has_moved = old_dn != new_dn
        return metadata, old, new, has_moved

    def _handle_create(self, metadata: Metadata, new: AttributeMapping) -> None:
        """
        Called when a new object was created.

        :param str metadata: metadata of the create event
        :param dict new: new UDM objects attributes
        """
        raise NotImplementedError

    def _handle_modify(
        self,
        metadata: Metadata,
        old: AttributeMapping,
        new: AttributeMapping,
        has_moved: bool,
    ) -> None:
        """
        Called when an existing object was modified or moved.

        A move can be be detected by looking at <has_moved>. Attributes can be
        modified during a move.

        :param str metadata: metadata of the modify event
        :param dict old: previous UDM objects attributes
        :param dict new: new UDM objects attributes
        """
        raise NotImplementedError

    def _handle_remove(self, metadata: Metadata, old: AttributeMapping) -> None:
        """
        Called when an object was removed.

        :param str metadata: metadata of the remove event
        :param dict old: previous UDM objects attributes
        """
        raise NotImplementedError

    def _handle_error(
        self,
        metadata: Metadata,
        old: AttributeMapping,
        new: AttributeMapping,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        exc_traceback: types.TracebackType | None,
    ) -> None:
        """
        Will be called for unhandled exceptions in create/modify/remove.

        :param str metadata: current events metadata
        :param dict old: previous UDM objects attributes
        :param dict new: new UDM objects attributes
        :param type exc_type: exception class
        :param BaseException exc_value: exception object
        :param traceback exc_traceback: traceback object
        """
        assert exc_value is not None
        self.logger.exception("metadata=%r\n    old=%r\n    new=%r", metadata, old, new)  # noqa: LOG004
        raise exc_value.with_traceback(exc_traceback)

    @classmethod
    def diff(
        cls, event: QueryEventObject, keys: Iterable[str] | None = None
    ) -> dict[str, tuple[Any, Any]]:
        """
        Find differences in old and new. Returns dict with keys pointing to old
        and new values.

        :param dict old: previous UDM objects attributes
        :param dict new: new UDM objects attributes
        :param list keys: consider only those keys in comparison
        :return: key -> (old[key], new[key]) mapping
        :rtype: dict
        """
        _, old, new, _ = cls._event_to_udm(event)
        res = {}
        if keys:
            keyset = set(keys)
        else:
            keyset = set(old) | set(new)
        for key in keyset:
            if set(old.get(key, [])) != set(new.get(key, [])):
                res[key] = old.get(key), new.get(key)
        return res


class ConsumerModule:
    def __init__(
        self,
        handler: EventHandler,
        session: requests.Session | None = None,
        logger: Logger | None = None,
        *args,
        **kwargs,
    ):
        """
        ConsumerModule
        :param EventHandler handler:
        :param requests.Session session: optional session for HTTP requests (for testing)
        :param kwargs:
           str config_dir: path to configuration directory
           str name: name of the consumer (has to be unique)
           str provisioning_url: url of provisioning service
               (e.g. "https://FQDN-OF-PRIMARY/univention/provisioning/")
        """
        self.handler: EventHandler = handler
        self.config = kwargs
        self.validate_config()
        self.logger: Logger = logger if logger is not None else loguru.logger
        self.logger.info(f"Starting consumer {self.config['name']}")
        self.session: requests.Session = (
            session if session is not None else requests.Session()
        )
        self.subscription_name: str | None
        self.subscription_password: str | None
        self.subscription_name, self.subscription_password = (
            self._get_subscription_credentials()
        )

    def validate_config(self):
        if not (isinstance(self.config.get("name"), str) and self.config.get("name")):
            raise ValueError("'name' is not set in the config!")
        self.config["config_dir"] = os.path.abspath(
            self.config.get("config_dir", "/var/lib/univention/consumer")
        ).rstrip("/")
        if not self.config["config_dir"]:
            raise ValueError("'config_dir' is not set in the config!")
        if self.config.get("provisioning_url") is None:
            raise ValueError("'provisioning_url' is not set in the config!")
        if self.config.get("error_timeout") is None:
            self.config["error_timeout"] = DEFAULT_ERROR_TIMEOUT
        if (
            not isinstance(self.config["error_timeout"], int)
            or self.config["error_timeout"] < 0
        ):
            raise ValueError("'error_timeout' is not a valid integer >= 0!")

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.config})"

    def _get_subscription_credentials(self) -> tuple[str | None, str | None]:
        """
        Get subscription credentials from configuration file.
        :returns: (name, password)
            name and password are None if configuration file could
            not be found or values are unset.
        """
        fn = os.path.join(self.config["config_dir"], FILENAME_CONFIG)
        if os.path.isfile(fn):
            with open(fn) as fd:
                data = json.load(fd)
                if "subscription_name" in data and "subscription_password" in data:
                    self.logger.debug(f"Read configuration file {fn}")
                    return data["subscription_name"], data["subscription_password"]
                self.logger.warning(
                    (
                        f"Read configuration file {fn} but no "
                        "subscription_name or subscription_password was found"
                    )
                )
        else:
            self.logger.info(f"No configuration file {fn} found")
        return None, None

    def _save_subscription_credentials(self, name: str, password: str) -> None:
        """
        Save given subscription name and password to configuration file.
        """
        data = {
            "subscription_name": name,
            "subscription_password": password,
        }
        fn = os.path.join(self.config["config_dir"], FILENAME_CONFIG)
        fn_new = f"{fn}.new"
        fd = os.open(fn_new, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "w") as f:
            json.dump(data, f, indent=2)
        os.rename(fn_new, fn)

    def subscribe(
        self,
        admin_username: str,
        admin_password: str,
        topics: list[Topics],
        prefill: bool = True,
    ) -> None:
        """
        Creates a new subscription for the configured realm and topics at the provisioning service.
        It requires a special secret that is only accessible
        by domain administrators of the Nubus domain.

        :param str admin_username: administrator's username of provisioning service
        :param str admin_password: administrator's password of provisioning service
        :param list[dict[str, str]] topics: list of realms and topics to subscribe to
            e.g.:
            topics = [
                {"realm": "udm", "topic": "users/user"},
                {"realm": "udm", "topic": "groups/group"}
            ]
        :param bool prefill: whether to prefill the subscription queue after initial registration
        :raises: SubscriptionError in case of failure
        """
        self.subscription_name, self.subscription_password = (
            self._get_subscription_credentials()
        )
        if not self.subscription_name or not self.subscription_password:
            self.subscription_name = f"{self.config['name']}-{secrets.token_hex(16)}"
            self.subscription_password = secrets.token_urlsafe(32)

        create_sub_json = {
            "name": self.subscription_name,
            "realms_topics": topics,
            "request_prefill": prefill,
            "password": self.subscription_password,
        }
        resp = self.session.post(
            self.config["provisioning_url"] + "/v1/subscriptions",
            json=create_sub_json,
            auth=(admin_username, admin_password),
        )
        if resp.status_code >= 300:
            self.logger.error(
                f"Subscription request failed with error code {resp.status_code}: {resp.text}"
            )
            raise SubscriptionError(resp.text)

        self._save_subscription_credentials(
            self.subscription_name, self.subscription_password
        )

    def consume_loop(self):
        """
        An infinite loop in which events from the provisioning queue are processed.
        """
        self.logger.debug("Starting consumer loop...")
        while True:
            try:
                self.process_one_event()
            except QueueAccessError as e:
                self.logger.critical(f"Unable to access provisioning queue: {e}")
                self.logger.error(
                    f"Sleeping {self.config['error_timeout']}s before continuing"
                )
                time.sleep(self.config["error_timeout"])

    def process_one_event(self, long_polling_timeout: int = 10):
        """
        Fetch next event from provisioning queue. If there is no waiting event in the subscribed queue,
        the request does long polling. It either times out after the given number of seconds or
        directly returns if the next events is pushed to the queue.
        :param int long_polling_timeout: number of seconds
            to wait for new events in case the queue is empty
        :return: None
        :raise: QueueAccessError is raised, in case the
                access to the queue is denied or credentials are missing.
        """
        event = self._fetch_event(long_polling_timeout)
        if event:
            self.logger.debug(f"Event {event['sequence_number']} has been fetched.")
            if not self.handler.is_relevant(event):
                self.logger.debug(
                    f"Skipped and acknowledged event {event['sequence_number']} as requested."
                )
                self._acknowledge_event(event)
            elif self.handler.handle_event(event):
                self.logger.debug(
                    f"Event {event['sequence_number']} has not been processed successfully."
                )
                self._acknowledge_event(event)
        else:
            # If the queue is empty, it uses long polling
            # with a default timeout of <long_polling_timeout> seconds,
            # for immediate notification of new changes.
            self.logger.debug("Long polling timeout, no more events.")

    def _fetch_event(self, long_polling_timeout: int) -> QueryEventObject | None:
        """
        Fetch next item from queue.
        :return: event dictionary
        :rtype: dict
        :raise: QueueAccessError
        """
        if not self.subscription_name or not self.subscription_password:
            raise QueueAccessError("No subscription name or password")
        resp = self.session.get(
            f"{self.config['provisioning_url']}/v1/subscriptions/{self.subscription_name}/messages/next",
            params={"timeout": long_polling_timeout},
            auth=(self.subscription_name, self.subscription_password),
        )
        if resp.status_code != 200:
            raise QueueAccessError(resp.text)
        return resp.json()

    def _acknowledge_event(self, event: QueryEventObject) -> None:
        """
        Acknowledge specified event at provisioning service.
        :param event: the event to be acknowledged
        :return: None
        """
        assert self.subscription_name is not None
        assert self.subscription_password is not None
        seq_num = event["sequence_number"]
        status_json = {"status": "ok"}
        self.logger.debug(f"Acknowledging event {seq_num}")
        response = self.session.patch(
            (
                f"{self.config['provisioning_url']}/v1/"
                f"subscriptions/{self.subscription_name}/messages/{seq_num}/status"
            ),
            json=status_json,
            auth=(self.subscription_name, self.subscription_password),
        )
        if response.status_code != 200:
            self.logger.error(f"Acknowledging event {seq_num} failed: {response.text}")
