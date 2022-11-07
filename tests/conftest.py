import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from dataasset.db import metadata, start_mappers


def pytest_addoption(parser):
    parser.addoption(
        "--url",
        action="store",
        default="postgresql://postgres:postgres@postgres-db:5432/dataasset",
    )


def default_db(dsn):
    if "postgres" in dsn[0]:
        return "postgres"
    elif "mysql" in dsn[0]:
        return "mysql"
    elif "mssql" in dsn[0]:
        return "master"
    else:
        raise Exception("no def")


@pytest.fixture(scope="session")
def database(pytestconfig):
    dsn = pytestconfig.getoption("url")
    dsn = dsn.split("/")[:-1]
    database_name_options = pytestconfig.getoption("url").split("/")[-1]
    database_name = database_name_options.split("?")[0]
    options = database_name_options.split("?")[1:]
    dsn = f"{'/'.join(dsn)}/{default_db(dsn)}?{'?'.join(options)}"
    engine = create_engine(
        dsn, echo=False, connect_args={"autocommit": True} if "mssql" in dsn else {}
    )
    db = sessionmaker(bind=engine)()
    if "postgresql" in dsn:
        db.connection().connection.set_isolation_level(0)

    db.execute(f"DROP DATABASE IF EXISTS {database_name};")
    db.execute(f"CREATE DATABASE {database_name};")
    if "postgresql" in dsn:
        db.connection().connection.set_isolation_level(1)
    start_mappers()


@pytest.fixture
def database_session(database, pytestconfig):
    dsn = pytestconfig.getoption("url")
    engine = create_engine(dsn, echo=False)
    db = sessionmaker(bind=engine)()
    metadata.create_all(bind=engine)
    yield db
    db.close()
    metadata.drop_all(bind=engine)
    engine.dispose()
