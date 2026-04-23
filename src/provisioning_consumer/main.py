import time

from provisioning_consumer_lib import ConsumerModule, EventHandler, DN


def main() -> None:
    print("Hello, World!")

    while True:
        time.sleep(1)


if __name__ == "__main__":
    main()
