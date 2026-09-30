import importlib
import sys


def _matches(name: str, forbidden: list[str], mode: str) -> bool:
    if mode == "module":
        return any(name == prefix or name.startswith(prefix + ".") for prefix in forbidden)
    return name.split(".")[0] in forbidden


def main(argv: list[str]) -> int:
    targets, raw_forbidden, mode = argv[1].split(","), argv[2], argv[3]
    forbidden = [name for name in raw_forbidden.split(",") if name]

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
