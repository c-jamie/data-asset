import datetime

from dataasset.asset import DataAsset
from dataasset.factory import dataasset_from_url
from dataasset.io import DummyIOContext, FileIOContext
from dataasset.repository import SqlAlchemyRepository

FAKE_FILE = "A,B\n1,2020-01-01\n2,2021-01-01"


def test_register_poke(database_session):
    da = DataAsset(
        name="test1",
        context=DummyIOContext(),
        repo=SqlAlchemyRepository(session=database_session),
    )

    @da.register_poke()
    def dopoke(context, *args):
        assert context is None
        return 99

    assert da.poke() == 99


def test_register_poke_database(database_session):
    repo = SqlAlchemyRepository(session=database_session)
    da = DataAsset(name="test2", context=DummyIOContext(), repo=repo)

    @da.register_poke()
    def dopoke(context, *args):
        assert context is None
        return 99

    assert da.poke() == 99
    t2 = repo.get("test2")
    assert t2.last_poke is not None


def test_before_hook(database_session):
    repo = SqlAlchemyRepository(session=database_session)
    da = DataAsset(name="test2", context=DummyIOContext(), repo=repo)
    out = []

    @da.register_poke()
    def dopoke(context, *args):
        out.append(2)
        assert context is None
        return 99

    @da.hook(da.before(dopoke))
    def before_hook(*args, **kwargs):
        out.append(1)

    assert da.poke() == 99
    assert out[0] == 1
    assert out[1] == 2


def test_after_hook(database_session):
    repo = SqlAlchemyRepository(session=database_session)
    da = DataAsset(name="test2", context=DummyIOContext(), repo=repo)
    out = []

    @da.register_poke()
    def dopoke(context, *args):
        out.append(2)
        assert context is None
        return 99

    @da.hook(da.after(dopoke))
    def after_hook(*args, **kwargs):
        out.append(1)

    assert da.poke() == 99
    assert out[0] == 2
    assert out[1] == 1


def test_file_get(fs, database_session):
    fs.create_file("/data/d.csv", contents=FAKE_FILE)
    import pandas as pd

    repo = SqlAlchemyRepository(session=database_session)
    da = DataAsset(name="test-get", context=FileIOContext(base_path="/data"), repo=repo)

    @da.register_get()
    def do_get(context, *args):
        df = pd.read_csv(context / "d.csv")
        return df

    df = da.get()
    assert len(df) == 2


def test_file_get_datetime_update(fs, database_session):
    fs.create_file("/data/d.csv", contents=FAKE_FILE)
    import pandas as pd

    repo = SqlAlchemyRepository(session=database_session)
    da = DataAsset(
        name="test-get-meta", context=FileIOContext(base_path="/data"), repo=repo
    )

    @da.register_get()
    def do_get(context, da):
        df = pd.read_csv(context / "d.csv")
        df["B"] = pd.to_datetime(df["B"], format="%Y-%m-%d")
        da.data_as_of = df["B"].max()
        da.as_of = datetime.datetime.now()
        return df

    df = da.get()
    assert len(df) == 2


def test_file_get_meta_update(fs, database_session):
    fs.create_file("/data/d.csv", contents=FAKE_FILE)
    import pandas as pd

    repo = SqlAlchemyRepository(session=database_session)
    da = DataAsset(
        name="test-get-meta", context=FileIOContext(base_path="/data"), repo=repo
    )

    @da.register_get()
    def do_get(context, da):
        df = pd.read_csv(context / "d.csv")
        da.meta = {"a": 99}
        return df

    df = da.get()
    assert len(df) == 2


def test_factory(pytestconfig):
    dsn = pytestconfig.getoption("url")

    da = dataasset_from_url(name="d1", context=FileIOContext(base_path="/"), url=dsn)
    assert da.name
