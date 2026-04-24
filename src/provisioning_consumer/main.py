# SPDX-FileCopyrightText: 2026 Univention GmbH
# SPDX-License-Identifier: AGPL-3.0-only

import asyncio

from loguru import logger
from provisioning_consumer_lib import ConsumerModule, UDMEventHandler, AttributeMapping, Metadata


class MyEventHandler(UDMEventHandler):
    async def _handle_create(self, metadata: Metadata, new: AttributeMapping) -> None:
        self.logger.info("_handle_create has been called for %s", new["dn"])

    async def _handle_modify(
        self,
        metadata: Metadata,
        old: AttributeMapping,
        new: AttributeMapping,
        has_moved: bool,
    ) -> None:
        self.logger.info("_handle_modify has been called for %s", new["dn"])

    async def _handle_remove(self, metadata: Metadata, old: AttributeMapping) -> None:
        self.logger.info("_handle_remove has been called for %s", old["dn"])


async def main() -> None:
    provisioning_admin = "provisioning_admin"
    provisioning_password = "a-secret-string"
    provisioning_topics = [
        {"realm": "udm", "topic": "users/user"},
        {"realm": "udm", "topic": "groups/group"},
    ]
    provisioning_url = "https://fqdn-of-primary-directory-node/univention/provisioning/"
    event_handler = MyEventHandler(logger)
    async with ConsumerModule(
        handler=event_handler,
        name="TestConsumer",
        provisioning_url=provisioning_url,
        config_dir=".",
    ) as consumer:
        await consumer.subscribe(
            provisioning_admin, provisioning_password, provisioning_topics, prefill=True
        )
        await consumer.consume_loop()


if __name__ == "__main__":
    asyncio.run(main())
