from dataasset import DataAsset
from dataasset.io import AbstractIOContext
from dataasset.repository import SqlAlchemyRepository


def repo_from_url(url: str) -> SqlAlchemyRepository:
    """generate a repo from a url

    Parameters
    ----------
    url : str
        sql alchemy connection string

    Returns
    -------
    SqlAlchemyRepository
    """
    from dataasset.db import start_mappers
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker

    start_mappers()
    engine = create_engine(url, echo=False)
    database_session = sessionmaker(bind=engine)()
    repo = SqlAlchemyRepository(session=database_session)
    return repo

def dataasset_from_url(*, name: str, context: AbstractIOContext, url: str) -> DataAsset:
    """generate a dataasset from a url

    Parameters
    ----------
    name : str
        name of the asset
    context : AbstractIOContext
        io context used to access data
    url : str
        a url, like a database connection strinf

    Returns
    -------
    DataAsset

    Raises
    ------
    Exception
        if a matching url isn't founr
    """
    if "mssql" in url or "postgres" in url or "mysql" in url:
        repo = repo_from_url(url)
        return DataAsset(name=name, context=context, repo=repo)
    elif ":memory:" in url:
        return DataAsset(name=name, contect=context, repo=None)
    else:
        raise Exception("DataAsset not available for url")
