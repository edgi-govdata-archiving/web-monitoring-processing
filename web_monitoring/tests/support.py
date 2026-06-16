from pathlib import Path
from typing import Iterable


FIXTURE_FILES_PATH = Path(__file__).parent / 'fixtures'


def get_fixture_bytes(filename):
    filepath = FIXTURE_FILES_PATH / filename
    with filepath.open('rb') as file:
        return file.read()


def get_fixture_paths(glob: str) -> Iterable[Path]:
    return FIXTURE_FILES_PATH.glob(glob)
