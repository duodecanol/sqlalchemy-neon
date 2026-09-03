import tarfile
from pathlib import Path


REQUIRED_FILES = {
    "LICENSE",
    "README.md",
    "TESTING.md",
    "pyproject.toml",
}
REQUIRED_PREFIXES = {
    "src/sqlalchemy_neon/",
    "tests/",
    "testsupport/",
}
FORBIDDEN_PARTS = {"experiments", "__pycache__", ".pytest_cache"}


def main() -> None:
    archives = sorted(Path("dist").glob("*.tar.gz"))
    if len(archives) != 1:
        raise SystemExit(f"expected one source distribution, found {archives}")

    with tarfile.open(archives[0], "r:gz") as archive:
        names = {
            Path(member.name).relative_to(member.name.split("/", 1)[0]).as_posix()
            for member in archive.getmembers()
            if "/" in member.name
        }

    missing_files = REQUIRED_FILES - names
    missing_prefixes = {
        prefix for prefix in REQUIRED_PREFIXES if not any(name.startswith(prefix) for name in names)
    }
    forbidden = {
        name for name in names if any(part in FORBIDDEN_PARTS for part in Path(name).parts)
    }
    if missing_files or missing_prefixes or forbidden:
        raise SystemExit(
            "invalid source distribution contents: "
            f"missing_files={sorted(missing_files)}, "
            f"missing_prefixes={sorted(missing_prefixes)}, "
            f"forbidden={sorted(forbidden)}"
        )


if __name__ == "__main__":
    main()
