from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class DataAsset:
    """reprents the key meta data for a Data Asset"""

    name: str
    meta: field(default_factory=dict) = None
    as_of: datetime = None
    data_as_of: datetime = None
    last_poke: datetime = None
