"""What a notebook's stored outputs carry into a public repository: nothing of the machine that
ran it. A library's warning prints the path of the file it came from, and with it a home
directory and a user name."""

import json
import re
from pathlib import Path

import pytest

NOTEBOOKS = sorted((Path(__file__).resolve().parents[2] / "notebooks").glob("*.ipynb"))
HOME = re.compile(r"(?:/home/|/Users/|\\Users\\)[\w.-]+")


def stored_outputs(notebook: Path) -> str:
    cells = json.loads(notebook.read_text())["cells"]
    return "\n".join(json.dumps(output) for cell in cells for output in cell.get("outputs", []))


def test_the_notebooks_are_found():
    assert len(NOTEBOOKS) >= 11


@pytest.mark.parametrize("notebook", NOTEBOOKS, ids=lambda path: path.name)
def test_no_stored_output_names_a_home_directory(notebook):
    assert sorted(set(HOME.findall(stored_outputs(notebook)))) == []
