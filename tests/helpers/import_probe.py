import importlib
import sys

_USAGE = "usage: import_probe.py TARGETS[,TARGET...] FORBIDDEN[,FORBIDDEN...] module|distribution"


def _matches(name: str, forbidden: list[str], mode: str) -> bool:
    if mode == "module":
        return any(name == prefix or name.startswith(prefix + ".") for prefix in forbidden)
    return name.split(".")[0] in forbidden


def main(argv: list[str]) -> int:
    if len(argv) < 4 or argv[3] not in {"module", "distribution"}:
        print(_USAGE)
        return 2
    targets = [name for name in argv[1].split(",") if name]
    forbidden = [name for name in argv[2].split(",") if name]
    mode = argv[3]
    if not targets:
        print(_USAGE)
        return 2

    for name in [name for name in sys.modules if _matches(name, forbidden, mode)]:
        del sys.modules[name]

    for target in targets:
        importlib.import_module(target)
    present = sorted(name for name in sys.modules if _matches(name, forbidden, mode))
    if present:
        print(f"{','.join(targets)} pulled forbidden modules: {present}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
