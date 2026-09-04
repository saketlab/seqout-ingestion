"""sc_stats runs on fleet hosts that have numpy and h5py and nothing else."""

import sys

import pytest

#: what a fleet host does NOT have. Importing any of these from sc_stats stops
#: every worker at startup: the failure is silent per-study, the watchdog keeps
#: relaunching, and `pgrep` still shows processes, so the fleet looks healthy
#: while producing nothing.
ABSENT_ON_FLEET = ("httpx", "psycopg", "pandas", "tqdm", "socksio", "lxml")


class _Blocker:
    def find_module(self, name, path=None):  # noqa: ARG002
        if name.split(".")[0] in ABSENT_ON_FLEET:
            return self
        return None

    def load_module(self, name):
        msg = f"{name} is not installed on fleet hosts"
        raise ImportError(msg)


@pytest.mark.parametrize(
    "module",
    ["sc_stats.stats", "sc_stats.yuj_worker", "sc_stats.fetch", "sc_stats.mito"],
)
def test_module_imports_without_the_wider_install(module):
    for name in list(sys.modules):
        if name.startswith("sc_stats"):
            del sys.modules[name]
    blocker = _Blocker()
    sys.meta_path.insert(0, blocker)
    try:
        __import__(module)
    finally:
        sys.meta_path.remove(blocker)
