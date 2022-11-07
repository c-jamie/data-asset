import datetime

from dataasset import model
from dataasset.factory import repo_from_url
from dataasset.io import FileIOContext
from dataasset.repository import SqlAlchemyRepository


def test_dataasset_orm_mapper(database_session):
    database_session.execute(
        "INSERT INTO data_asset (name, as_of, last_poke, data_as_of) VALUES "
        "('AA', '2022-01-01', '2022-01-02', '2022-01-03'),"
        "('BB', '2022-02-01', '2022-02-02', '2022-02-03')"
    )
    expected = [
        model.DataAsset(
            "AA",
            as_of=datetime.datetime(2022, 1, 1),
            last_poke=datetime.datetime(2022, 1, 2),
            data_as_of=datetime.datetime(2022, 1, 3),
        ),
        model.DataAsset(
            "BB",
            as_of=datetime.datetime(2022, 2, 1),
            last_poke=datetime.datetime(2022, 2, 2),
            data_as_of=datetime.datetime(2022, 2, 3),
        ),
    ]
    assert database_session.query(model.DataAsset).all() == expected


def test_repo_add(database_session):
    expected = [
        model.DataAsset(
            "CC",
            as_of=datetime.datetime(2022, 2, 1),
            last_poke=datetime.datetime(2022, 2, 2),
            data_as_of=datetime.datetime(2022, 2, 3),
        ),
    ]
    repo = SqlAlchemyRepository(session=database_session)
    repo.add(expected[-1])
    test = repo.get("CC")
    assert test == expected[-1]


def test_repo_update(database_session):
    expected = [
        model.DataAsset(
            "DD",
            as_of=datetime.datetime(2022, 2, 1),
            last_poke=datetime.datetime(2022, 2, 2),
            data_as_of=datetime.datetime(2022, 2, 3),
        ),
    ]
    repo = SqlAlchemyRepository(session=database_session)
    repo.add(expected[-1])
    test_1 = repo.get("DD")
    test_1.as_of = datetime.datetime(2023, 2, 1)
    repo.update(test_1)
    test_2 = repo.get("DD")
    assert test_1 == test_2


def test_repo(pytestconfig):
    dsn = pytestconfig.getoption("url")
    da = repo_from_url(dsn)
    assert da is not None