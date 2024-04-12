# py-sqlshell

This is a simple SQL command line that works with any RDBMS that's supported
by [SQLAlchemy](https://www.sqlalchemy.org/), providing a common set of
commands and a query output format that looks the same, no matter what
database you're using.

## Installation

```shell
$ pip install build
$ cd py-sqlshell
$ python -m build
$ pip install dist/*.whl
```

This will install the package, as well as its dependencies, and it will
create a `sqlshell` command in your Python's `bin` directory.

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

## License

This software is released under the
[Apache Software License](https://apache.org/licenses/LICENSE-2.0), version
2.0. A text copy of the license is available in this repository.
