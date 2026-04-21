import time


def main() -> None:
    print("Hello, World!")

    # Sleep indefinitely, waking every second to remain interruptible
    while True:
        time.sleep(1)


if __name__ == "__main__":
    main()
