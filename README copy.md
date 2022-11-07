# Data Asset

Data teams often have a need to manage `source` 'Data Assets'.

These are external data sources which the data teams depend on.

These are often in external databases, buckets, files systems, APIs.

Data Asset is a functional API to formally declare and manage these 'Assets'.

### Example

Assume our source data is a tabular csv in a file system.

```
/datalake/sales_full_hist.csv
+------------+-------+
| date       | sales |
+------------+-------+
| 2021-03-01 | 83.4  |
+------------+-------+
| 2021-04-01 | 98.1  |
+------------+-------+
| 2021-05-01 | 75.2  |
+------------+-------+
```


```py
import pandas as pd
from dataasset import DataAsset as DA
from dataasset.io import FileIOContext
from dataasset.factory import repo_from_url

# any sqlalchemy URL
URL = "postgresql://postgres:postgres@localhost:5432/dataasset"
# Data Asset metadata can be managed in a database, or even in memory (by setting repo to None)
repo = repo_from_url(URL)

asset = DA(name="data-asset-one", context=FileIOContext(base_path="/datalake"), repo=repo)

# Data Asset exposes two functions, `poke` and `get`
# To use them, we register our own get and poke 
# functions, according to our needs

@asset.register_get():
def do_get(context, da):
    # context is passsed in by the asset object, as a path
    # da has a number of useful attributes which can be set here
    df = pd.read_csv(context / "sales_full_hist.csv")
    df['date'] = pd.to_datetime(df['date'], format="%Y-%m-%d")
    # this will be updated automatically upon every `get` call
    da.data_as_of = df['date'].max()
    return df

# poke allows us to check for new data in a consistent fashion
@asset.register_poke():
def do_poke(context):
    df = pd.read_csv(context / "sales_full_hist.csv")
    df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d")
    return da.data_as_of is None or da.data_as_of < df["b"].max()

# with `poke` and `get` registered we can now access our data
new_data_avail = asset.poke()
if new_data_avail:
    df = asset.get()

```