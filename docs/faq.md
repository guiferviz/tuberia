# Frequently Asked Questions


## How does tuberia compare with dbt?

dbt is more SQL-centric and handles queries as templates. In tuberia tables
are classes that can execute arbitrary Python code (including SQL if for
example you are using PySpark).

To do unit tests in dbt you must learn how to define them and where to put them
(you must define a macro that allows you to change where the table you want to
test takes data from, you need to add a separate file with the test data...).
In tuberia, if you know how to test in Python you know how to test the classes
that create tables (mocks to the rescue!).

In dbt dynamic queries can get complicated with the use of Jinja templates.
When it comes to dynamic code there is nothing simpler than using a programming
language to define transformations.

!!! note

    Using [arbitrary Python code in
    dbt](https://docs.getdbt.com/docs/building-a-dbt-project/building-models/python-models)
    is coming but it is still to early to compare.


## How does tuberia compare with Prefect?

Tuberia is not an orchestrator. Tuberia can be seen more as a compiler,
passing Python code that anyone with Python skills can handle, to a Prefect
flow (or to any other orchestration tool) that you can deploy in your
environment.


## How does tuberia compare with Airflow?

Tuberia is not an orchestrator. Tuberia can be seen more as a compiler,
passing Python code that anyone with Python skills can handle, to an Airflow
DAG (or any other orchestration tool) that you can deploy in your environment.
