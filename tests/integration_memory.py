import pathlib
from re import A

import pandas as pd

import dataasset
from dataasset.io import FileIOContext


def main():
    BASE_PATH = pathlib.Path(__file__).parent.absolute()
    print(f"writing data to {BASE_PATH}")
    pd.DataFrame(
        {"a": [1, 2, 3], "b": ["2022-01-01", "2023-01-01", "2024-01-01"]}
    ).to_csv(BASE_PATH / "d.csv", index=False)
    da = dataasset.DataAsset(
        name="test-get-meta", context=FileIOContext(base_path=BASE_PATH), repo=None
    )

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


if __name__ == "__main__":
    main()
