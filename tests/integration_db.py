import pathlib
import pandas as pd

from dataasset.factory import dataasset_from_url
from dataasset.io import FileIOContext


def main():
    BASE_PATH = pathlib.Path(__file__).parent.absolute()
    URL = "mssql+pyodbc://sa:MSS#inside00@localhost:1433/master?driver=ODBC+Driver+17+for+SQL+Server"
    print(f"writing data to {BASE_PATH}")

    # initial data
    pd.DataFrame(
        {"a": [1, 2, 3], "b": ["2022-01-01", "2023-01-01", "2024-01-01"]}
    ).to_csv(BASE_PATH / "d.csv", index=False)

    da = dataasset_from_url(
        name="da-1", context=FileIOContext(base_path=BASE_PATH), url=URL
    )

    @da.register_poke()
    def do_poke(context, da):
        df = pd.read_csv(context / "d.csv")
        df["b"] = pd.to_datetime(df["b"], format="%Y-%m-%d")
        return da.data_as_of is None or da.data_as_of < df["b"].max()

    @da.register_get()
    def do_get(context, da):
        df = pd.read_csv(context / "d.csv")
        df["b"] = pd.to_datetime(df["b"], format="%Y-%m-%d")
        df.to_pickle(context / "tmp.pkl")
        da.data_as_of = df["b"].max()
        return df, "tmp.pkl"

    @da.hook(da.after(do_get))
    def after_hook(context, da, df, file, *args, **kwargs):
        import os

        os.remove(context / file)
        print("removed ", context / file)

    df, _ = da.get()
    assert len(df) == 3
    assert da.poke() is False

    # new data loaded
    pd.DataFrame(
        {
            "a": [1, 2, 3, 4],
            "b": ["2022-01-01", "2023-01-01", "2024-01-01", "2025-01-01"],
        }
    ).to_csv(BASE_PATH / "d.csv", index=False)

    assert da.poke() is True


if __name__ == "__main__":
    main()
