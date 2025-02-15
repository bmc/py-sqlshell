# py-sqlshell

This is a simple SQL command shell that works with any RDBMS that's
supported by [SQLAlchemy](https://www.sqlalchemy.org/), providing a common set
of commands and a query output format that looks the same, no matter what
database you're using. In addition, it uses Python `readline` module, so
it supports history, command editing, and rudimentary completion.

Installing this software installs a `sqlshell` command in your Python
environment.

**Note:** See [Maintenance Warnings](#maintenance-warnings), below.

## Motivation

Database command shells are common. PostgreSQL has `psql`. MySQL has
`mysql`. SQLite has `sqlite3`. Oracle has `sqlplus`.

But they're all different. They have different commands, different capabilities,
and different ways of showing query output. (And the `sqlite3` output format is
just awful.)

`sqlshell` is not as feature-rich as the database-specific command shells,
but it works the same way and looks the same, no matter what database it's
querying. (And querying a SQLite database with `sqlshell` produces output
that's actually readable.)

## Installation

This package is not currently in PyPI. I may add it at some point; I may
not.

In the meantime, you can install it easily enough from source. First,
check out a copy of this repository. Then, you'll use the Python
[build](https://build.pypa.io/en/stable/index.html) tool to build the
`sqlshell` package, which you can then use `pip` to install.

```shell
$ pip install build
$ cd py-sqlshell
$ ./build.sh clean build
$ pip install dist/sqlshell-0.4.0-py3-none-any.whl
```

(Alter the version stamp in the `.whl` file as necessary.)

This will install the package, as well as its dependencies, and it will
create a `sqlshell` command in your Python's `bin` directory.

**Recommendation:** Don't install `sqlshell` in your main Python installation.
Use a [Python virtual environment](https://docs.python.org/3/library/venv.html),
instead.

`sqlshell` uses the newer
[Python package standard](https://packaging.python.org/en/latest/overview/)
for building and packaging. See `pyproject.toml` in this repo for package
and build settings.

**Warning:** `sqlshell` requires Python 3.11 or better.

## Usage

Run `sqlshell -h` to see usage output.

You'll pass `sqlshell` a SQLAlchemy-compatible database URL or the name of a
section in the [configuration file](#configuration-file). With databases
other than SQLite3, you'll need to install supporting packages in order to
connect to the database. See [Examples](#examples), below.

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

The currently supported commands are as follows. `.help` from within `sqlshell`
provides more information on each one.

* `.connect`: Connect to a different database
* `.exit`, `.quit`, Ctrl-D: Quit `sqlshell`
* `.export`: Export a table to a CSV or JSON file
* `.fk`: Show foreign key constraints
* `.help` or `?`: Show help
* `.history`: Show the command history
* `.import`: Import a CSV or JSON file into a table (with restrictions)
* `.indexes`: Show the indexes for a table
* `.limit`: Set or clear the maximum number of rows shown from a query
* `.schema`: Show the schema for a table
* `.tables`: Show the names of the tables in the current database
* `.url`: Show the SQLAlchemy URL for the current database

Anything else you type is assumed to be SQL. SQL statements _must_ end with
a semicolon (";"), and multi-line input is supported. Newlines are not
preserved in the input, and a multi-line statement is sent to the database and
written to the history as a single line.

#### Command Completion

`sqlshell` has some rudimentary support for command completion of its
`.` commands. In addition, the `.schema`, `.indexes`, and `.fk` commands, which
take a single table name parameter, support table name completion. Again, type
`.help` for a complete explanation.

There is _no_ support for SQL completion.

**Note:** On some Linux distributions, it can be difficult to get Python's
GNU readline package to honor tab-completion, even though it works fine in
(say) `bash`. But hitting the ESC key twice does generally work, though it's
less convenient. This may be an artifact of how the Python distribution is
compiled.

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

If you prefer `psycopg2`, the process is similar:

```shell
$ pip install psycopg2
$ sqlshell postgresql+psycopg2://scott:tiger@localhost/test
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
$ sqlshell mysql+mysqlconnector://scott:tiger@localhost/test
```

### SQLite3

In this case, my test database is in file "test.db" in my home directory.
Since Python's standard library has built-in support for SQLite3, I don't
have to install anything first.

```shell
$ cd ~
$ sqlshell sqlite:///test.db
```

If you get tired of typing the same URL all the time, you can record
commonly-used connection information in the optional
[configuration file](#configuration-file).

## Configuration File

`sqlshell` supports an optional configuration file, located in
`.sqlshell.cfg` by default. You can override the location of the configuration
file with the `-c` (`--config`) command line option.

The configuration file provides two benefits:

- Rather than pass the SQLAlchemy database URL on the command line, you can
  store it in a section in the configuration file, and then simply pass the
  section name into `sqlshell`.
- You can also specify an alternate history file to use for the connection,
  allowing you to have a history file per connection, if you want.

Here's an example:

```toml
[postgres-test]
url = "postgresql+pg8000://user:password@localhost/test"
history = "~/.sqlshell-pg-test-history"

[postgres-prod]
url = "postgresql+pg8000://user:password@localhost/production"
history = "~/.sqlshell-pg-prod-history"

[mysql-test]
url = "mysql+mysqlconnector://scott:tiger@localhost/test"
history = "~/.sqlshell-mysql-test-history"
```

With that configuration in place, you can simply invoke `sqlshell` as follows,
to connect to your test PostgreSQL database:

```shell
$ sqlshell postgres-test
```

You can also use the shortest string that unique matches the prefix of a
configuration section. So, given the above configuration, you can use
`sqlshell postgres-p` as a shorthand for `sqlshell postgres-prod`, and
you can even use `sqlshell m` to match the `mysql-test` instance. If your
prefix matches multiple entries, `sqlshell` will tell you. (The match is done
case-blind.)

The configuration file is in [TOML](https://toml.io/en) format. You can
have as many sections as you want. In each section:

- `url` is required
- `history` is optional; if omitted, `sqlshell` will use the main history
  file (or whatever alternate file you specify with `-H`).

You can reference environment variables in both `history` and `url`; they will
be substituted accordingly. If you refer to a nonexistent environment variable,
an empty string will be substitute.

In `history`, you can also use a leading `~/` to refer to your home directory.
The following three history entries are equivalent:

```toml
history = "~/.sqlshell-history"
history = "$HOME/.sqlshell-history"
history = "${HOME}/.sqlshell-history"
```

See `sample.cfg` in this repository for an example.

## Maintenance Warnings

I built this tool for my personal use. If you find it useful, as a tool
or even as an example of how to build a `readline`-based command shell in
less than 1,000 lines of code, that's great. But this isn't intended to be
commercial-grade software, and I'm not aggressively maintaining it. (That's
one reason it isn't in PyPI right now.) Don't expect me to jump on feature
requests.

**Windows users**: You're on your own. Sorry. I haven't tested `sqlshell`
on Windows, and I likely won't. It runs fine for me on Linux and MacOS. If it
works for you on Windows, that's terrific (but unlikely). In any case, I'm
unwilling to spend time getting this to work on Windows.

## License

`sqlshell` is copyright © 2024-2025 Brian M. Clapper and is released under the
[Apache Software License](https://apache.org/licenses/LICENSE-2.0), version
2.0. A text copy of the license is available in this repository.
