import abc
import os
from typing import List

from dataasset import model


class AbstractRepository(abc.ABC):
    @abc.abstractmethod
    def add(self, da: model.DataAsset) -> None:
        """adds a model.DataAsset struct to our repo

        Parameters
        ----------
        da : model.DataAsset
            out model.DataAsset mode

        Raises
        ------
        NotImplementedError
        """
        raise NotImplementedError

    @abc.abstractmethod
    def get(self, name: str) -> model.DataAsset:
        """loads our model.dataasset from the repo by name

        Parameters
        ----------
        name : str

        Returns
        -------
        model.DataAsset

        Raises
        ------
        NotImplementedError
        """
        raise NotImplementedError

    @abc.abstractmethod
    def list(self) -> List[model.DataAsset]:
        """list all available model.DataAssets

        Returns
        -------
        List[model.DataAsset]

        Raises
        ------
        NotImplementedError
        """
        raise NotImplementedError

    @abc.abstractmethod
    def update(self, da):
        raise NotImplementedError


class InMemoryRepository(AbstractRepository):
    """this repository is in memory

    Parameters
    ----------
    AbstractRepository : _type_
    """

    def __init__(self):
        self._repo = {}

    def add(self, da: model.DataAsset) -> model.DataAsset:
        """add an inmemory model.DataAsset

        Parameters
        ----------
        da : model.DataAsset

        Returns
        -------
        model.DataAsset
        """
        self._repo[da.name] = da
        return da

    def get(self, name: str) -> model.DataAsset:
        """get via asset name

        Parameters
        ----------
        name : str

        Returns
        -------
        model.DataAsset
        """
        return self._repo.get(name, None)

    def list(self) -> List[model.DataAsset]:
        """list all available model.DataAssets

        Returns
        -------
        List[model.DataAsset]
        """
        self._repo.values()

    def update(self, da: model.DataAsset) -> model.DataAsset:
        """update our model.DataAsset

        Parameters
        ----------
        da : model.DataAsset

        Returns
        -------
        model.DataAsset
        """
        _da = self._repo.get(da.name)
        _da.as_of = da.as_of
        _da.data_as_of = da.data_as_of
        _da.last_poke = da.last_poke

        self._repo[da.name] = _da
        return _da


class SqlAlchemyRepository(AbstractRepository):
    """model.DataAsset lives in the database

    Parameters
    ----------
    AbstractRepository : _type_
    """

    def __init__(self, session):
        self.session = session

    def add(self, da: model.DataAsset) -> model.DataAsset:
        """add a model.DataAsset to the repo

        Parameters
        ----------
        da : model.DataAsset

        Returns
        -------
        model.DataAsset
        """
        self.session.add(da)
        self.session.commit()
        return da

    def get(self, name: str) -> model.DataAsset:
        """get a data asset from the repo

        Parameters
        ----------
        name : str

        Returns
        -------
        model.DataAsset
        """
        import sqlalchemy

        try:
            return self.session.query(model.DataAsset).filter_by(name=name).one()
        except sqlalchemy.exc.NoResultFound:
            return None

    def list(self) -> List[model.DataAsset]:
        """get all available model.dataasset

        Returns
        -------
        List[model.DataAsset]
        """
        return self.session.query(model.DataAsset).all()

    def update(self, da: model.DataAsset) -> model.DataAsset:
        """update the model.DataAsset

        Parameters
        ----------
        da : model.DataAsset

        Returns
        -------
        model.DataAsset
        """
        from sqlalchemy import update

        stmt = (
            update(model.DataAsset)
            .where(model.DataAsset.name == da.name)
            .values(as_of=da.as_of, data_as_of=da.data_as_of, last_poke=da.last_poke)
            .execution_options(synchronize_session="fetch")
        )
        result = self.session.execute(stmt)
        self.session.commit()
        return result


REPO_REGISTRY = {
    "postgres": SqlAlchemyRepository,
    "memory": InMemoryRepository,
    "mssql": SqlAlchemyRepository,
    "mysql": SqlAlchemyRepository,
}


def get_repo():
    repo = os.getenv("DA_REPO")
    return REPO_REGISTRY.get(repo, InMemoryRepository)()
