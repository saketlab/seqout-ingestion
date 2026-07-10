import sys

from sc_enrich.counter.yuj_worker import main

if __name__ == "__main__":
    sys.exit(main() or 0)
