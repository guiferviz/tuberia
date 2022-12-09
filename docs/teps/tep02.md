# TEP 2 - Flexible and simple tasks

{% from "tep_header.md" import tep_header %}
{{
    tep_header(
        authors=["guiferviz"],
        status="Planned",
        created="2022-11-14",
        version="0.0.1",
    )
}}

The equivalent of source code for Tuberia is Python objects representing data
pipeline tasks and their dependencies. In Tuberia, users define their data
pipelines using Python classes and methods, rather than using a specific
language or syntax. This allows users to leverage their existing knowledge of
Python and its ecosystem of libraries and tools to create powerful and flexible
data pipelines.

In this TEP, we will discuss how to create tasks and specify dependencies
between them in Tuberia. We will describe the different types of tasks that can
be defined, and provide examples of how to write and use these tasks in your
data pipelines will look like.


## Name selection: Tasks or Steps?

In the context of data pipelines, the terms *step* and *task* are often used
interchangeably to refer to an indivisible unit of work. Both terms are used to
describe a specific action or operation that is performed as part of a data
pipeline, such as extracting data from a database, transforming the data, or
loading the data into a target system.

However, there may be some subtle differences in the way that these terms are
used. For example, the term "step" may be used to refer to a specific operation
or action that is performed in a linear, sequential manner, as part of a larger
process. On the other hand, the term "task" may be used to refer to a
standalone operation or action that can be performed independently, and may not
necessarily be part of a larger process.

I personally like the definitions provided by [this
page](https://www.pie.me/blog/task-versus-step){target=_blank}.

!!! quote

    A step is part of a group. A task is alone. A step wants to feel like it
    belongs to a bigger purpose, contributing to others. A task is selfish,
    only thinking of itself.

Overall, the difference between the terms "step" and "task" may be somewhat
subtle, and may depend on the context in which they are used. Which term will
be using Tuberia then?

In the humble opinion of the author of these lines, the word Task seems to be
the most common word in computer science. Libraries such as Prefect already
define the concept of tasks. There are hundreds of libraries that sell
themselves as task runners. It is not so common to find "step runners", for
example.

Due to the popularity of the word Task and the subtle differences it has with
Step, Task is chosen as the name for the indivisible units of work in Tuberia.


## Existing libraries

One design decision that was made in the development of Tuberia was to use
Python classes to represent the tasks or steps of a data pipeline. This
decision was based on the fact that Python is a widely-used and well-known
language, and many developers are already familiar with it. By using Python
classes to represent the tasks of a data pipeline, Tuberia can leverage the
existing knowledge and expertise of developers, and make it easy for them to
start using Tuberia without a steep learning curve.

Another potential design decision was to use an existing library, such as
Prefect, to create tasks or steps in the data pipeline. Prefect is a popular
Python library for creating and managing data pipelines, and using it to create
tasks in Tuberia could have potentially saved time and effort in the
development of the compiler.

Prefect is the library for creating
[tasks](https://docs-v1.prefect.io/api/latest/core/task.html#task){target=_blank}
which I am most familiar with. Here is an example of how to create an use a
task. In this example we are also creating a prefect Flow, equivalent to the
dependency tree that we also want to define in this TEP.

```python
from prefect import Task


class AddTask(Task):
    def run(self, x, y):
        return x + y

a = AddTask()

with Flow("My Flow") as f:
    t1 = a(1, 2) # t1 != a
    t2 = a(5, 7) # t2 != a
```

The least convincing part of this implementation is that the parameters that
define the execution are passed to the run method. You can create an `__init__`
method in your Task subclass but the parameters you pass to it must be any
other type of data than Task objects. It's quite confusing to have 2 different
ways to pass parameters to your task. I would prefer all parameters in the
`__init__` method.

Prefect also comes with decorators.

```python
from prefect import task

@task
def add_task(x, y):
    return x + y
```

For simple tasks this may be fine, but most of the time we will have a lot of
parameters. Think about PySpark table creation; we must have the database name,
the table name, the input tables, the data expectations we must apply, the
table schema... We can subclass from Task and create a PySparkTable class with
all those common table parameters and then create a decorator that creates tables
using the PySparkTable class. Pseudocode:

```python
from prefect import task

class PySparkTable(Task):
    ...

# Define decorator
def pyspark_table(...):
    ...

@pyspark_table(
    database_name="my_database",
    table_name="my_table",
    data_expectations=...
    schema=...
)
def join_tables(table0, table1):
    # Create table from tables table0 and table1.
    ...
```

Again, same problem as before, the parameters passed to the decorator are
indeed passed to the `__init__` method. The function parameters are `run`
method parameters. Task dependencies cannot be passed to `__init__`, just to
`run`. Apart from that, it is not possible to get the `database_name` or
`table_name` from the function body, which make this approach difficult to use.

There is one observation more, imagine that we have two functions `table0` and
`table1` decorated with our `pyspark_table` decorator. We need to save those
tables in a variable in order to pass them to the `join_tables` task:

```python
@pyspark_table
def table0():
    ...

@pyspark_table
def table1():
    ...

@pyspark_table
def join_tables(table0, table1):
    ...

with Flow("My Flow") as f:
    table0 = table0()
    table1 = table1()
    join_tables = join_tables(table0, table1)
```

Do you see any problem in the previous code? We are naming our functions using
the name of the tables. It makes sense to create variables with exactly the
same names, but it is a problem as we are overwriting the functions. In this
example it is not clear if we are passing `table0` and `table1` functions to
our `join_tables` or if we are passing the task objects.

There are more issues with this approach. Just looking at `join_tables`, what
can we say about `table0`? Can we use any PySpark table here or it should have
a concrete schema? Using decorators we loose type annotations. If we create
classes we have a type that we can use to annotate parameters. Besides that, we
can easily name the variables. For example:

```python
class Table0(PySparkTable):
    ...

class Table1(PySparkTable):
    ...

class JoinTables(PySparkTable):
    table0: Table0
    table1: Table1

    def __init__(self, table0: Table0, table1: Table1):
        self.table0 = table0
        self.table1 = table1

    def run(self):
        ...
```

The previous code does not work in Prefect because we are using tasks in our
`__init__` method but we can see that this approach provides typing annotations
and avoids name collisions (`Table0` can now be assigned to `table0` without
hiding any function).

!!! note

    On the other hand, Prefect requires a lot of dependencies, so if it can be
    avoided, the better.

I could not find more libraries following an approach similar to what I have in
mind. I did explore [invoke](https://github.com/pyinvoke/invoke) but it is more
related to `make` than to Prefect. [celery](https://github.com/celery/celery)
deals with distributed tasks.


## Dependencies

In this section we will discuss two different approaches to define dependencies
between tasks. One approach is using a `get_dependencies` method in our Task
objects. Another approach is to use a dependency manager that extracts the
dependencies of a pipeline from the object attribute.

### Manually define dependencies

In this approach, each Task object defines a `get_dependencies` method that
returns a list of tasks that the current task depends on. This method can be
overridden by subclasses to define the specific dependencies of each task. For
example:

```python
class Task:
    def __init__(self):
        self.dependencies = []

    def get_dependencies(self):
        return self.dependencies

class ExtractData(Task):
    def __init__(self):
        super().__init__()
        self.dependencies = []

class TransformData(Task):
    def __init__(self):
        super().__init__()
        self.dependencies = [ExtractData]

class LoadData(Task):
    def __init__(self):
        super().__init__()
        self.dependencies = [TransformData]
```

In this example, `TransformData` depends on `ExtractData`, and `LoadData`
depends on `TransformData`. This approach allows us to define the dependencies
of each task in a clear and concise way, and makes it easy to modify or update
the dependencies as needed.

However, this approach has a couple of drawbacks. If the user does not
implement the get_dependencies method correctly (because he/she forgets about
adding the object to the dependencies list, for example), the dependencies for
the Task object may not be extracted correctly, and this could lead to errors
in the generated DAG for the data pipeline.

Another potential problem is that this approach requires the user to include a
get_dependencies method on every Task object that has dependencies. This can be
a significant amount of extra code for the user to write and maintain,
especially if the user has many Task objects in their data pipeline. This extra
code can make the data pipeline compiler more difficult to use and understand,
and it may decrease its overall usability.

Overall, while this approach allows users to define custom `get_dependencies`
methods on their Step objects, it may introduce additional complexity and
potential errors in the data pipeline compiler. A simpler approach, such as
automatically extracting dependencies from the attributes of the Task objects,
may be more suitable in some cases.


### Automatically extracting dependencies

The second approach is to use a dependency manager to extract the dependencies
of a Task from the object attributes. This means that instead of defining a
list of dependencies in the Task object itself, the dependency manager would
inspect the attributes of the object and extract the dependencies from there.
For example, consider the following code:

```python
class ExtractData:
    def run(self):
        # Extract data here.

class TransformData:
    extract_data: ExtractData
    def run(self):
        # Transform data here using the extracted data.
```

In this example, the `TransformData` class depends on the `ExtractData` class.
A dependency manager could inspect the extract_data attribute of the
`TransformData` class and determine that `TransformData` depends on
`ExtractData`. This approach allows the user to define dependencies in a more
natural and intuitive way, by simply setting the attributes of the Task
objects.

However, this approach also has some drawbacks. For example, it may not always
be clear which attributes of a Task object represent dependencies, and it may
be difficult to ensure that all dependencies are properly defined (specially
when using attributes with data structures like dicts or list that contain
Tasks). Additionally, this approach may not be as flexible as the
`get_dependencies` method, as it may be difficult to define complex or dynamic
dependencies using object attributes.


### Hybrid approach

One potential solution to the limitations of the two approaches discussed above
is to use a hybrid approach that combines the best features of both. In this
approach, a Task object could define a `get_dependencies` method if it needs to
define complex or dynamic dependencies, and the dependency manager would use
this method to extract the dependencies. If the `get_dependencies` method is
not defined, the dependency manager would fall back to inspecting the object
attributes to extract the dependencies.

This hybrid approach would allow Task objects to define complex or dynamic
dependencies using the `get_dependencies` method, while still allowing for
simple and intuitive definitions of dependencies using object attributes.

Here is an example of how this hybrid approach could be implemented:

```python
class ExtractData:
    def run(self):
        # Extract data here.

class TransformData:
    extract_data: ExtractData
    def run(self):
        # Transform data here using the extracted data.

    def get_dependencies(self):
        return [self.extract_data]
```

In this example, the `TransformData` class defines a `get_dependencies` method
that returns a list of dependencies. The dependency manager would use this
method to extract the dependencies of the `TransformData` class. If the
`get_dependencies` method was not defined, the dependency manager would fall
back to inspecting the `extract_data` attribute of the `TransformData` class to
determine the dependencies.


## Task properties

A class representing a task in a data pipeline should have certain properties
to facilitate the creation and management of the pipeline. These properties can
include an ID, a name, tags...

The ID property can be used to uniquely identify a task within a data pipeline.
This is important because it allows the dependency manager to track and manage
dependencies between tasks, and it also allows the user to refer to specific
tasks in the pipeline if needed. The ID can be automatically generated by the
dependency manager, or it can be explicitly set by the user. If we
automatically generate IDs we need to be sure that the ID is consistent between
runs.

The name property can be used to provide a human-readable name for a task. This
can be useful for documentation and debugging purposes, as it can help the user
understand the purpose of a task and its place in the pipeline. The name
property can be automatically derived from the class name, or it can be
explicitly set by the user.

Tags can be a useful property of tasks in a data pipeline. Tags can be used to
group tasks by categories, such as by type, purpose, or any other relevant
criteria. For example, tasks that are part of the same data transformation or
data quality checking process can be grouped under the same tag. This can help
the user understand the organization and structure of the pipeline, and it can
also be useful for debugging and optimization purposes. Tags can also be used
to group the execution of tags, i.e. executing multiple task in just one
step of the orchestrator.
