"""Check that every locked Python dependency installs on linux/amd64 AND linux/arm64 from a wheel.

The code location image is built for linux/amd64 (x64) and linux/arm64 (Apple Silicon dev
machines, arm64 servers) from uv.lock without a compiler in the image. A dependency that only
ships an x86_64 wheel (or only an sdist with native code) would fail - or silently compile - on
arm64. Run: python3 scripts/check-wheels.py [uv.lock]   (part of `make lint`).
"""

import re
import sys
import tomllib

PY = "cp313"  # the image's Python (Containerfile PYTHON_IMAGE)

# Not installed on Linux, or no native code to build - reviewed by hand:
EXEMPT = {
    "psutil": "only required with marker sys_platform == 'win32' (dagster)",
    "pywin32": "only required with marker sys_platform == 'win32' (dagster, dlt)",
    "dbt-core-experimental-parser": (
        "sdist whose build backend downloads a prebuilt, sha256-pinned binary from the dbt "
        "GitHub release (manylinux_2_28 x86_64 and aarch64 both published)"
    ),
}


def linux_ok(wheels: list[str], arch: str) -> bool:
    for w in wheels:
        py, abi, plat = w[:-4].split("-")[-3:]
        if plat == "any":
            return True
        py_ok = PY in py.split(".") or abi == "abi3" or py.startswith("py3")
        if py_ok and re.search(rf"manylinux[^.]*_{arch}", plat):
            return True
    return False


def main() -> int:
    with open(sys.argv[1] if len(sys.argv) > 1 else "uv.lock", "rb") as f:
        lock = tomllib.load(f)
    bad = []
    for p in lock["package"]:
        src = p.get("source", {})
        if "editable" in src or "virtual" in src or p["name"] in EXEMPT:
            continue
        wheels = [w["url"].rsplit("/", 1)[1] for w in p.get("wheels", [])]
        for arch in ("x86_64", "aarch64"):
            if not linux_ok(wheels, arch):
                bad.append(f"{p['name']} {p['version']}: no {PY} manylinux {arch} wheel")
    for b in bad:
        print("check-wheels:", b, file=sys.stderr)
    if not bad:
        print(f"check-wheels: linux x86_64 + aarch64 wheels for every dependency ({len(EXEMPT)} reviewed exemptions)")
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
