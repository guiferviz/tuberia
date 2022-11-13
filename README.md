<p align="center">
    <a href="https://aidictive.github.io/tuberia" target="_blank">
        <img src="https://aidictive.github.io/tuberia/images/logo.png"
             alt="Tuberia logo"
             width="800">
    </a>
</p>
<p align="center">
    <a href="https://github.com/AIdictive/tuberia/actions/workflows/cicd.yaml" target="_blank">
        <img src="https://github.com/aidictive/tuberia/actions/workflows/cicd.yaml/badge.svg"
             alt="Tuberia CI pipeline status">
    </a>
    <a href="https://app.codecov.io/gh/AIdictive/tuberia/" target="_blank">
        <img src="https://img.shields.io/codecov/c/github/aidictive/tuberia"
             alt="Tuberia coverage status">
    </a>
    <a href="https://github.com/AIdictive/tuberia/issues" target="_blank">
        <img src="https://img.shields.io/bitbucket/issues/AIdictive/tuberia"
             alt="Tuberia issues">
    </a>
    <a href="https://github.com/aidictive/tuberia/graphs/contributors" target="_blank">
        <img src="https://img.shields.io/github/contributors/AIdictive/tuberia"
             alt="Tuberia contributors">
    </a>
    <a href="https://pypi.org/project/tuberia/" target="_blank">
        <img src="https://pepy.tech/badge/tuberia"
             alt="Tuberia total downloads">
    </a>
    <a href="https://pypi.org/project/tuberia/" target="_blank">
        <img src="https://pepy.tech/badge/tuberia/month"
             alt="Tuberia downloads per month">
    </a>
    <br />
    Data engineering meets software engineering
</p>

---

:books: **Documentation**:
<a href="https://aidictive.github.io/tuberia" target="_blank">
    https://aidictive.github.io/tuberia
</a>

:keyboard: **Source Code**:
<a href="https://github.com/aidictive/tuberia" target="_blank">
    https://github.com/aidictive/tuberia
</a>

---


## 🤔 What is this?

Tuberia is born from the need to bring the worlds of data and software
engineering closer together. Here is a list of common problems in data
projects:

* Loooooong SQL queries impossible to understand/test.
* A lot of duplicate code due to the difficulty of reusing it in SQL queries.
* Lack of tests, sometimes because the used framework does not facilitate
testing tasks.
* Lack of documentation.
* Discrepancies between the existing documentation and the latest deployed code.
* A set of notebooks deployed under the Databricks Share folder.
* A generic notebook with utility functions.
* Use of drag-and-drop frameworks that limit the developer's creativity.
* Months of intense work to migrate existing pipelines from one orchestrator to
another (e.g. from Airflow to Prefect, from Databricks Jobs to Data
Factory...).

Tuberia aims to solve all these problems and many others. 


## 🤓 How it works?

You can view Tuberia as if it were a compiler. Instead of compiling a
programming language, it compiles the steps necessary for your data pipeline to
run successfully.

Tuberia is not an orchestrator, but it allows you to run the code you write in
Python in any existing orchestrator: Airflow, Prefect, Databricks Jobs, Data
Factory....

Tuberia provides some abstraction of where the code is executed, but defines
very well what are the necessary steps to execute it. For example, this shows
how to create a PySpark DataFrame from the `range` function and creates a Delta
table.

```python
import pyspark.sql.functions as F

from tuberia import PySparkTable, run


class Range(PySparkTable):
    """Table with numbers from 1 to `n`.

    Attribute:
        n: Max number in table.

    """
    n: int = 10

    def df(self):
        return self.spark.range(self.n).withColumn("id", F.col(self.schema.id)


class DoubleRange(PySparkTable):
    range: Range = Range()

    def df(self):
        return self.range.read().withColumn("id", F.col("id") * 2)


run(DoubleRange())
```

!!! warning

    Previous code may not work yet and it can change. Please, notice this
    project is in an early stage of its development.

All docstrings included in the code will be used to generate documentation
about your data pipeline. That information, together with the result of data
expectations/data quality rules will help you to always have complete and up to
date documentation.

Besides that, as you have seen, Tuberia is pure Python so doing unit tests/data
tests is very easy. Programming gurus will enjoy data engineering again!
