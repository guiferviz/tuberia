from typing import List

from tuberia.flow import Flow
from tuberia.spark import get_spark
from tuberia.table import Table

from financial.tables.input_clients import InputClients
from financial.tables.input_products import InputProducts
from financial.tables.input_categories import InputCategories
from financial.tables.input_sells import InputSells
from financial.tables.sells_by_category import SellsByCategory


class Financial(Flow):
    """Main flow.

    The final objective of this flow is to create the following three tables:

    ```mermaid
    erDiagram
        Movie ||--|{ ActsInMovie : has
        Actor ||--|{ ActsInMovie : acts
    ```

    Attributes:
        input_credits_path: Path to credits input file.
        input_movies_path: Path to movies input file.
        database: Database name in which all the tables will be created.
        database_dir: Root path of the database. Inside this folder, a
            folder with the same name as the database will be created. Inside
            that folder all the tables will be placed.

    """
    input_clients_path: str
    input_products_path: str
    input_categories_path: str
    input_sells_path: str
    database: str
    database_dir: str

    def pre_run(self):
        get_spark().sql(f"CREATE DATABASE {self.database}")

    def define(self) -> List[Table]:
        defaults = dict(
            database=self.database,
            path=self.database_dir,
        )
        input_clients = InputClients(
            **defaults, input_file_path=self.input_clients_path
        )
        input_products = InputProducts(
            **defaults, input_file_path=self.input_products_path
        )
        input_categories = InputCategories(
            **defaults, input_file_path=self.input_categories_path
        )
        input_sells = InputSells(
            **defaults, input_file_path=self.input_sells_path
        )

        sells_by_category = SellsByCategory(
            **defaults,
            input_clients=input_clients,
            input_products=input_products,
            input_categories=input_categories,
            input_sells=input_sells
        )
        
        return [
            input_clients,
            input_products,
            input_categories,
            input_sells,
            sells_by_category
        ]
