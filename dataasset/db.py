from typing import Any

import sqlalchemy
from sqlalchemy import Column, DateTime, Integer, MetaData, String, Table, null
from sqlalchemy.orm import mapper
from sqlalchemy_json import MutableJson

from dataasset import model

metadata = MetaData()

data_asset = Table(
    "data_asset",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("name", String(256), nullable=True),
    Column("as_of", DateTime, nullable=True),
    Column("last_poke", DateTime, nullable=True),
    Column("data_as_of", DateTime, nullable=True),
    Column("meta", MutableJson, nullable=True),
)


def start_mappers():

    try:
        da_mapper = mapper(model.DataAsset, data_asset)
    except sqlalchemy.exc.ArgumentError:
        pass
