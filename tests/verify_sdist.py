import tarfile
from email.parser import Parser
from pathlib import Path
from zipfile import ZipFile


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


def _metadata(content: bytes):
    return Parser().parsestr(content.decode("utf-8"))


def main() -> None:
    archives = sorted(Path("dist").glob("*.tar.gz"))
    wheels = sorted(Path("dist").glob("*.whl"))
    if len(archives) != 1 or len(wheels) != 1:
        raise SystemExit(
            f"expected one sdist and wheel, found sdists={archives}, wheels={wheels}"
        )

    with tarfile.open(archives[0], "r:gz") as archive:
        members = archive.getmembers()
        names = {
            Path(member.name)
            .relative_to(member.name.split("/", 1)[0])
            .as_posix()
            for member in members
            if "/" in member.name
        }
        pkg_info = next(
            member for member in members if member.name.endswith("/PKG-INFO")
        )
        metadata_file = archive.extractfile(pkg_info)
        if metadata_file is None:
            raise SystemExit("sdist PKG-INFO could not be extracted")
        sdist_metadata = _metadata(metadata_file.read())

    with ZipFile(wheels[0]) as wheel:
        metadata_name = next(
            name for name in wheel.namelist() if name.endswith(".dist-info/METADATA")
        )
        wheel_metadata = _metadata(wheel.read(metadata_name))

    missing_files = REQUIRED_FILES - names
    missing_prefixes = {
        prefix
        for prefix in REQUIRED_PREFIXES
        if not any(name.startswith(prefix) for name in names)
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

    metadata_fields = (
        "Name",
        "Version",
        "Summary",
        "Author-email",
        "Requires-Dist",
        "Provides-Extra",
    )
    mismatches = {
        field: (sdist_metadata.get_all(field), wheel_metadata.get_all(field))
        for field in metadata_fields
        if sdist_metadata.get_all(field) != wheel_metadata.get_all(field)
    }
    if mismatches:
        raise SystemExit(f"sdist/wheel metadata mismatch: {mismatches}")


if __name__ == "__main__":
    main()
