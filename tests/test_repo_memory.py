import datetime

from dataasset import model
from dataasset.repository import InMemoryRepository


def test_repo_add():
    expected = [
        model.DataAsset(
            "CC",
            as_of=datetime.datetime(2022, 2, 1),
            last_poke=datetime.datetime(2022, 2, 2),
            data_as_of=datetime.datetime(2022, 2, 3),
        ),
    ]
    repo = InMemoryRepository()
    repo.add(expected[-1])
    test = repo.get("CC")
    assert test == expected[-1]


def test_repo_update():
    expected = [
        model.DataAsset(
            "DD",
            as_of=datetime.datetime(2022, 2, 1),
            last_poke=datetime.datetime(2022, 2, 2),
            data_as_of=datetime.datetime(2022, 2, 3),
        ),
    ]
    repo = InMemoryRepository()
    repo.add(expected[-1])
    test_1 = repo.get("DD")
    test_1.as_of = datetime.datetime(2023, 2, 1)
    repo.update(test_1)
    test_2 = repo.get("DD")
    assert test_1 == test_2


def test_repo_get():
    expected = [
        model.DataAsset(
            "DD",
            as_of=datetime.datetime(2022, 2, 1),
            last_poke=datetime.datetime(2022, 2, 2),
            data_as_of=datetime.datetime(2022, 2, 3),
        ),
    ]
    repo = InMemoryRepository()
    repo.add(expected[-1])
    test_1 = repo.get("D?")
    assert test_1 is None
