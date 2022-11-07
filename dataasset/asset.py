import datetime
from ast import Call
from collections import defaultdict
from typing import Any, Callable, Tuple

from dataasset import model
from dataasset.io import AbstractIOContext
from dataasset.repository import AbstractRepository, get_repo


class FuncRegistry:
    def __init__(self) -> None:
        self.registry = {}
        self.before_registry = defaultdict(list)
        self.after_registry = defaultdict(list)

    def _call_before(self, name: str, *args: Any, **kwargs: Any) -> Any:
        for f in self.before_registry[name]:
            f(*args, **kwargs)

    def _call_after(self, name: str, *args: Any, **kwargs: Any) -> Any:
        for f in self.after_registry[name]:
            f(*args, **kwargs)

    def add(self, name: str, func: Callable) -> Any:
        """add a function to the registry

        Parameters
        ----------
        name : str
            the name of the function
        func : Callable
            the function
        """
        self.registry[name] = func

    def add_before(self, name: str, func: Callable) -> Any:
        """add a before hook

        Parameters
        ----------
        name : str
            the name of the hook
        func : Callablej
            the function to call before

        Returns
        -------
        Any
            the return value of the callable
        """
        self.before_registry[name].append(func)

    def add_after(self, name: str, func: Callable) -> Any:
        """add an after hook

        Parameters
        ----------
        name : str
            the name of the hook
        func : Callable
            the function to call after

        Returns
        -------
        Any
            the return value of the callable
        """
        self.after_registry[name].append(func)

    def call(self, name: str, *args: Any, **kwargs: Any) -> Any:
        """call a function, executes any before/after hooks

        Parameters
        ----------
        name : str
            function to call

        Returns
        -------
        Any
            the return value of the callable
        """
        func = self.registry[name]
        self._call_before(hash(func), *args, **kwargs)
        val = func(*args, **kwargs)
        if isinstance(val, tuple):
            self._call_after(hash(func), *args, *val, **kwargs)
        else:
            self._call_after(hash(func), *args, *(val,), **kwargs)
        return val


class DataAsset:
    """a data asset represents some source of data we wish to know about"""

    def __init__(
        self, *, name: str, context: AbstractIOContext, repo: AbstractRepository = None
    ):
        """_summary_

        Parameters
        ----------
        name : str
            the name of the asset
        context : AbstractIOContext
            the context used to access the asset
        repo : AbstractRepository, optional
            the backend used to store information on the asset, by default None
        """
        self.name = name
        self.context = context

        if repo is None:
            self.repo = get_repo()
        else:
            self.repo = repo
        self.registry = FuncRegistry()

    @property
    def meta(self):
        return self.repo.get(self.name).meta

    @property
    def as_of(self):
        return self.repo.get(self.name).as_of

    @property
    def data_as_of(self):
        return self.repo.get(self.name).data_as_of

    def register_poke(self) -> Callable:
        """a decorator used to register a function with the asset
        will return True, we the data needs to be refreshed
        """

        def decorator(func):
            self.registry.add("poke", func)
            return func

        return decorator

    def register_get(self) -> Callable:
        """a decorator used to register a function with the asset
        returns the asset and updates any asset metadata
        """

        def decorator(func):
            self.registry.add("get", func)
            return func

        return decorator

    def poke(self) -> bool:
        """check for a refresh

        Returns
        -------
        bool
            true if data can be refreshed
        """
        da = self.repo.get(self.name)
        if da is None:
            da = self.repo.add(
                model.DataAsset(name=self.name, last_poke=datetime.datetime.now())
            )
        return self.registry.call("poke", self.context.context(), da)

    def get(self) -> Any:
        """executes the get callable which much be registered previously

        Returns
        -------
        Any
            the data asset
        """
        da = self.repo.get(self.name)
        if da is None:
            da = self.repo.add(
                model.DataAsset(name=self.name, last_poke=datetime.datetime.now())
            )
        out = self.registry.call("get", self.context.context(), da)
        self.repo.update(da)
        return out

    def hook(self, func: Callable) -> Callable:
        """register a hook; this can be called before or after a poke or get function

        Parameters
        ----------
        func : Callable
            the function to be called

        Returns
        -------
        Callable
            the output of the function
        """
        what, when = func

        def dec(func1, what=what, when=when):
            if when == "before":
                self.registry.add_before(hash(what), func1)
            if when == "after":
                self.registry.add_after(hash(what), func1)
            return func1

        return dec

    def before(self, func: Callable) -> Tuple[Callable, str]:
        """defines the before call in a hook

        Parameters
        ----------
        func : Callable
            the function which this decorator is called befoe

        Returns
        -------
        Tuple[Callable, str]
            the function and identifier
        """
        return func, "before"

    def after(self, func: Callable) -> Tuple[Callable, str]:
        """defines the after call in a hook

        Parameters
        ----------
        func : Callable
            the function which this decorator is called after

        Returns
        -------
        Tuple[Callable, str]
            _description_
        """
        return func, "after"
