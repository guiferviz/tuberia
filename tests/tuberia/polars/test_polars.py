import polars

from tuberia.task import Task


class File(Task):
    path: str

    def df(self) -> polars.DataFrame:
        raise NotImplementedError()

    def run(self):
        df = self.df()
        df.write_parquet(self.path)


def test_polars():
    df = polars.DataFrame(
        {
            "id": polars.arange(0, 10, eager=True),
        }
    )
    print(df)
