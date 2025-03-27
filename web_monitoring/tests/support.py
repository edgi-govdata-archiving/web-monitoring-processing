from pathlib import Path


def get_fixture_bytes(filename):
    filepath = Path(__file__).parent / 'fixtures' / filename
    with filepath.open('rb') as file:
        return file.read()
