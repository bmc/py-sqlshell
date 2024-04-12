# py-sqlshell

This is a simple SQL command shell that works with any RDBMS that's
supported by [SQLAlchemy](https://www.sqlalchemy.org/), providing a common set
of commands and a query output format that looks the same, no matter what
database you're using. In addition, it uses Python `readline` module, so
it supports history, command editing, and rudimentary completion.

Installing this software installs a `sqlshell` command in your Python
environment.

**Note:** See [Maintenance Warning](#maintenance-warning), below.

## Installation

This package is not currently in PyPI. I may add it at some point; I may
not.

In the meantime, you can install it easily enough from source. First,
check out a copy of this repository. Then, run the following commands:

```shell
$ pip install build
$ cd py-sqlshell
$ python -m build
$ pip install dist/*.whl
```

This will install the package, as well as its dependencies, and it will
create a `sqlshell` command in your Python's `bin` directory.

**Note:** `sqlshell` requires Python 3.10 or better.

## Usage

Run `sqlshell -h` to see usage output.

You'll pass `sqlshell` a SQLAlchemy-compatible database URL, such has
`mysql+mysqlconnector://user:password@localhost/mydatabase`. With databases
other than SQLite3, you'll need to install supporting packages in order to
connect to the databasae (e.g., `python-mysql-connector`).

`sqlshell` uses the Python `readline` package, so it'll use either GNU
Readline or Editline under the covers, depending on your operating system
and on how Python was compiled. If Python's `readline` uses GNU Readline,
`sqlshell` will load key bindings from `$HOME/.inputrc`. If Editline is
being used, then `sqlshell` loads bindings from `$HOME/.editrc`.

Once you've entered `sqlshell`, type `.help` at the prompt for a description
of the non-SQL commands it supports. Or just start entering SQL.

Though `sqlshell` should work with any SQLAlchemy-supported RDBMS, I have
only tested it against MySQL, PostgreSQL, and SQLite3.

See [Examples](#examples), below, for how you might use `sqlshell` with
those RDBMS systems.

### Commands

With the exception of `?`, which is an alias for `.help`, all of the non-SQL
commands `sqlshell` supports start with `.`.

Once you get into `sqlshell`, type `?` or `.help` at the prompt to see a list
of the internal commands. Anything else you type is assumed to be SQL and passed
along to the database.

#### Command Completion

`sqlshell` has some rudimentary support for command completion of its
`.` commands. In addition, the `.schema` command, which takes a single table
name parameter, supports table name completion. Again, type `.help` for a
complete explanation.

## Examples

### PostgreSQL

Suppose I have a PostgreSQL server running on my local machine, and it
contains a database called "test" accessible by user "scott" using the
(awful) password "tiger". I've decided to use the `pg8000` Python package
to access PostgreSQL from Python. So, the first step is:

```shell
$ pip install pg8000
```

Once that package is installed in my Python environment, I can use `sqlshell`
to connect to my database:

```shell
$ sqlshell postgresql+pg8000://scott:tiger@localhost/test
```

### MySQL

Suppose, instead, my "test" database is in a MySQL server running on my
local machine, with the same user ("scott") and terrible password ("tiger").
I've decided to use the `mysql-connector-python` package to access MySQL
from Python. The first step is to install that package:

```shell
$ pip install mysql-connector-python
```

Now, I can access my database using `sqlshell`:

```shell
$ sqlshell mysql+mysqlconnector://scott:tiger@localhost/test"
```

### SQLite3

In this case, my test database is in file "test.db" in my home directory.
Since Python's standard library has built-in support for SQLite3, I don't
have to install anything first.

```shell
$ cd ~
$ sqlshell sqlite:///test.db
```

## Maintenance Warning

I built this tool for my personal use. If you find it useful, as a tool
or even as an example of how to build a `readline`-based command shell,
that's great. But this isn't commercial-grade software, and I'm not
aggressively maintaining it. (That's one reason it isn't in PyPI right now.)

## License

This software is released under the
[Apache Software License](https://apache.org/licenses/LICENSE-2.0), version
2.0. A text copy of the license is available in this repository.
