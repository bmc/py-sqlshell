"""
This is a simple SQL command shell that works with any RDBMS that's supported
by SQLAlchemy, providing a common set of commands and a query output format
that looks the same, no matter what database you're using. In addition, it uses
Python `readline` module, so it supports history, command editing, and
rudimentary completion.

Run with -h or --help for an extended usage message.
"""
# pylint: disable=too-many-lines,fixme,too-few-public-methods

import atexit
import csv
import json
import os
import re
import readline
import shlex
import sys
import textwrap
import traceback
from contextlib import suppress
from dataclasses import dataclass
from datetime import date, datetime
from enum import StrEnum
from pathlib import Path
from time import perf_counter
from typing import Any, Callable, cast, Dict, Self, Sequence as Seq, Tuple

import click
import sqlalchemy
from sqlalchemy.engine import Engine
from sqlalchemy.engine.result import MappingResult
from sqlalchemy.orm import Session
from sqlalchemy.schema import CreateTable
from termcolor import colored

NAME = "sqlshell"
VERSION = "0.3.0"
CLICK_CONTEXT_SETTINGS = {"help_option_names": ["-h", "--help"]}
HISTORY_LENGTH = 10000
# Note that Python's readline library can be based on GNU Readline
# or the BSD Editline library, and it's not selectable. It's whatever
# has been compiled in. They use different initialization files, so we'll
# load whichever one is appropriate.
EDITLINE_BINDINGS_FILE = Path("~/.editrc").expanduser()
READLINE_BINDINGS_FILE = Path("~/.inputrc").expanduser()
DEFAULT_SCREEN_WIDTH = 79
DEFAULT_HISTORY_FILE = Path("~/.sqlshell-history").expanduser()
DEFAULT_CONFIG_FILE = Path("~/.sqlshell.cfg").expanduser()


class EngineName(StrEnum):
    """
    Explicitly supported SQLAlchemy engines. Other engines will work, but
    there's explicit, database-specific logic that's implemented for these.
    """
    POSTGRES = "postgresql"
    MYSQL = "mysql"
    SQLITE = "sqlite"


class Command(StrEnum):
    """
    Non-SQL commands the shell supports.
    """
    CONNECT = ".connect"
    EXPORT = ".export"
    IMPORT = ".import"
    FKEYS = ".fk"
    HELP1 = ".help"
    HELP2 = "?"
    HISTORY = ".history"
    INDEXES = ".indexes"
    LIMIT = ".limit"
    QUIT1 = ".exit"
    QUIT2 = ".quit"
    SCHEMA = ".schema"
    TABLES = ".tables"
    URL = ".url"


@dataclass(frozen=True)
class ConnectionConfig:
    """
    A single connection configuration from the configuration file.
    """
    name: str
    url: str
    history_file: Path | None


class Configuration:
    """
    Represents the parsed configuration data.
    """
    def __init__(
        self: Self, configs: list[ConnectionConfig], path: Path
    ) -> None:
        """
        Initialize a Configuration object.
        """
        self._configs = configs
        self._path = path

    @property
    def path(self: Self) -> Path:
        """
        Returns the path associated with the configuration.
        """
        return self._path

    def lookup(self: Self, spec: str) -> list[ConnectionConfig] | None:
        """
        Uses a string to look up a configuration. Returns a list of matching
        configurations, or None if no match.
        """
        matches = [
            c for c in self._configs
            if c.name.lower().startswith(spec.lower())
        ]

        if len(matches) == 0:
            return None

        return matches


class SQLShellException(Exception):
    """
    Base class for exceptions thrown by the SQL shell. Also thrown explicitly
    for certain errors in the SQL shell.
    """


class ConfigurationError(SQLShellException):
    """
    Thrown to indicate a configuration error.
    """


class AbortError(SQLShellException):
    """
    Thrown to force an abort with a non-zero exit code.
    """


class TooManyMatchesError(SQLShellException):
    """
    Thrown to indicate that a database URL specification matched too many
    entries in the configuration file.
    """


# This is a series of (command(s), explanation) tuples. print_help() will wrap
# the explanations.
HELP: Seq[Tuple[Seq[str], str, str]] = (
    (
        (Command.QUIT1.value, Command.QUIT2.value),
        f"{Command.QUIT1.value}, {Command.QUIT2.value}, or Ctrl-D",
        f"Quit {NAME}."
    ),
    (
        (Command.CONNECT.value,),
        f"{Command.CONNECT.value} <name>",
        "Connect to a different database. <name> is either a full SQLAlchemy "
        "URL or the name of a section in the configuration file. If <name> is "
        "a configuration file section, you only need to specify enough of the "
        "string to be unique. If it's not unique, you'll see an error message, "
        "and the current database will not be changed."
    ),
    (
        (Command.EXPORT.value,),
        f"{Command.EXPORT.value} <table> <path>",
        'Export the contents of table to a file. If <path> ends in ".csv", '
        'the table will be exported to a CSV file. If <path> ends in ".json", '
        "the table will be dumped in JSON Lines format, with each row as a "
        "JSON object in the file. You can use ~ in your paths as a shorthand "
        'for your home directory (e.g., "~/table.json")',
    ),
    (
        (Command.FKEYS.value,),
        f"{Command.FKEYS.value} <table_name>",
        "Display the list of foreign keys for a table. Note: <table_name> is "
        "the table with the foreign key constraints, not the table the "
        "foreign key(s) reference."
    ),
    (
        (Command.HELP1.value, Command.HELP2.value),
        f"{Command.HELP1.value} or {Command.HELP2.value} [<command>]",
        "Show help for <command>. If <command> is omitted, show help for "
        "all commands."
    ),
    (
        (Command.HISTORY.value,),
        f"{Command.HISTORY.value} [<n>]",
        "Show the history. If <n> is supplied, show the last <n> history "
        "items. <n> of 0 is the same as omitting <n>.",
    ),
    (
        (Command.HISTORY.value,),
        f"{Command.HISTORY.value} re",
        "Show all history items matching regular expression <re>. If your "
        "pattern contains spaces or regular expression backslash sequences "
        r"(e.g., \s), be sure to enclose it in quotes.",
    ),
    (
        (Command.IMPORT.value,),
        f"{Command.IMPORT.value} [-n] <table> <path>",
        "Import a CSV or JSON file into a table. If the table exists, "
        f"f{Command.IMPORT.value} will append to it. If the table does not "
        'exist, it will be created. If -n (for "new-only") is specified, the '
        "table must not already exist; the command will abort if it does. If "
        '<path> ends in ".csv", the file is assumed to be a CSV file. If '
        '<path> ends in ".json", the file is assumed to be a JSON Lines file, '
        f"as if it were produced by the f{Command.EXPORT.value} command. You "
        "use ~ as a shorthand for your home directory. This command uses "
        "Pandas import the file, so it will attempt to infer a schema. It "
        "can't however, infer a primary key or any foreign keys. If you need "
        "those, you can add them manually after the import, or you can "
        "precreate an empty table with appropriate constraints. Note also "
        "that Pandas' schema inference isn't perfect, especially with a column "
        "where all the incoming values are NULL. You may need to alter the "
        "table's column types after the import. On import, all column names "
        "are forced to lower case, so that column names don't require quoting "
        "in databases like Postgres. Also, if the column names in the incoming "
        "data are incompatible with a database's naming format, the import "
        "will fail.",
    ),
    (
        (Command.INDEXES.value,),
        f"{Command.INDEXES.value} <table_name>",
        "Display the indexes for <table_name>. Uses database-native commands, "
        "where possible. Otherwise, SQLAlchemy index information is displayed."
    ),
    (
        (Command.LIMIT.value,),
        f"{Command.LIMIT.value} <n>",
        "Show only <n> rows from a SELECT. 0 means unlimited.",
    ),
    (
        (Command.LIMIT.value,),
        f"{Command.LIMIT.value}",
        "Show the current limit setting"
    ),
    (
        (Command.SCHEMA.value,),
        f"{Command.SCHEMA.value} <table>",
        "Show the schema for table <table>"
    ),
    (
        (Command.TABLES.value,),
        f"{Command.TABLES.value}",
        "Show all tables in the database"
    ),
    (
        (Command.TABLES.value,),
        f"{Command.TABLES.value} <re>",
        "Show all tables in the database whose names match the specified "
        "regular expression. Matching is case-blind. If your pattern contains "
        r"spaces or regular expression backslash sequences (e.g., \s), be sure "
        "to enclose it in quotes.",
    ),
    (
        (Command.URL.value,),
        f"{Command.URL.value}",
        "Show the current database URL"
    ),
)

HELP_EPILOG = (
    "Anything else is interpreted as SQL. Multi-line SQL statements are not "
    "currently supported; a newline ends the statement. You do not need to end "
    "SQL statements with a semicolon, though you can do so, if you wish.",
    "",
    (
        "Note that you can use tab-completion on the dot-commands. Also, "
        "as a special case, you can tab-complete available table names after "
        f'typing "{Command.SCHEMA.value}" or "{Command.INDEXES.value}". '
        "Completion for SQL statements is not available."
    ),
)


# This is an engine cache, indexed by SQLAlchemy URL. It's used to avoid
# creating an engine for the same URL multiple times, which can happen if
# the .connect command is used multiple times with the same URL.
engine_cache: dict[str, Engine] = {}


match os.environ.get("COLUMNS"):
    case None:
        SCREEN_WIDTH = DEFAULT_SCREEN_WIDTH
    case s_width:
        try:
            SCREEN_WIDTH = int(s_width)
        except ValueError:
            SCREEN_WIDTH = DEFAULT_SCREEN_WIDTH
            print(
                "The COLUMNS environment variable has an invalid value of "
                f'"{s_width}". Using screen width of {DEFAULT_SCREEN_WIDTH}.'
            )


def error(msg: str) -> None:
    """
    Print error messages in a consistent way.
    """
    print(f"{colored('Error:', 'red')} {msg}", file=sys.stderr)


def get_tables(engine: Engine) -> list[sqlalchemy.Table]:
    """
    Get the list of Table objects in the database. The list is returned sorted
    by table name, case-blind.

    :param engine: The SQLAlchemy engine for the database

    :returns: A (possibly empty) list of SQLAlchemy Table objects
    """
    metadata = sqlalchemy.MetaData()
    metadata.reflect(bind=engine)
    return sorted(list(metadata.tables.values()), key=lambda t: t.name.lower())


def init_history(
    history_path: Path,
    prev_func: Callable[[], None] | None = None
) -> Callable[[], None]:
    """
    Load the local readline history file.

    :param history_path: Path of the history file. It doesn't have to exist.
    :param prev_func: The previously registered history function, if any.
        This function will be called before being unregistered and replaced.

    Returns the registered history function.
    """

    if prev_func is not None:
        prev_func()
        atexit.unregister(prev_func)
        readline.clear_history()

    with suppress(FileNotFoundError):
        print(f'Loading history from "{history_path}".')
        readline.read_history_file(str(history_path))
        # default history len is -1 (infinite), which may grow unruly

    readline.set_history_length(HISTORY_LENGTH)

    # Use a lambda here to capture the history_path variable. This ensures
    # we get a different lambda each time.
    # pylint: disable=unnecessary-lambda,unnecessary-lambda-assignment
    f = lambda: readline.write_history_file(str(history_path))
    atexit.register(f)

    return f


def init_bindings_and_completion(engine: Engine) -> None:
    """
    Initialize readline bindings.

    :param engine: The SQLAlchemy engine for the database, for table completion
    """

    def complete_tables(text: str, full_line: str) -> list[str]:
        """
        A readline completer for table names. Used for the SCHEMA, INDEXES,
        and FKEYS commands.
        """
        if full_line.endswith(" "):
            # Already fully completed
            options = []
        else:
            options = [t.name for t in get_tables(engine)
                       if t.name.lower().startswith(text.lower())]

        return options

    def command_completer(text: str, state: int) -> str | None:
        """
        This is a readline completer that will complete any command other
        than SQL.
        """
        commands = [cmd.value for cmd in Command]
        full_line = readline.get_line_buffer()

        # Get the first token. If it matches a complete command, handle it
        # differently.
        tokens = full_line.lstrip().split()
        match tokens:
            case []:
                options = commands
            case [s, *_] if s in (
                Command.SCHEMA.value,
                Command.INDEXES.value,
                Command.FKEYS.value
            ):
                options = complete_tables(text, full_line)
            case [s, *_] if s in (Command.HELP1.value, Command.HELP2.value):
                options = [c for c in commands if c.startswith(text)]

            case [s, *_] if s in commands:
                # An already completed command. There's nothing to complete.
                options = []
            case [s]:
                options = [c for c in commands if c.startswith(text)]
            case _:
                options = []

        if state < len(options):
            return options[state]

        return None

    if (readline.__doc__ is not None) and ("libedit" in readline.__doc__):
        init_file = EDITLINE_BINDINGS_FILE
        print("Using editline (libedit).")
        completion_binding = "bind '^I' rl_complete"
    else:
        print("Using GNU readline.")
        init_file = READLINE_BINDINGS_FILE
        completion_binding = "Control-I: rl_complete"

    if init_file.exists():
        print(f'Loading bindings from "{init_file}"')
        readline.read_init_file(init_file)

    # Ensure that tab = complete
    readline.parse_and_bind(completion_binding)
    # Set up the completer.
    readline.set_completer(command_completer)


# pylint: disable=too-many-arguments,too-many-positional-arguments
def display_results(
    columns: list[str],
    data: list[Dict[str, Any]],
    limit: int,
    total: int,
    no_results_message: str | None = None,
    elapsed: float | None = None,
) -> None:
    """
    Display the results of a select.

    Parameters:

    :param columns: the names of the columns, in order
    :param data: list of rows. Each row is a dictionary of (column -> value)
    :param limit: the current row limit (for display), or 0
    :param total: the total number of rows. If limit is 0, total must match
        the length of data. Otherwise, total is the number of rows that would
        have been displayed, if limit were 0.
    :param no_results_message: the message to display if the results are empty,
        or None for the default message
    :param elapsed: the elapsed time to run the query that produced the results,
        or None not to display an elapsed time
    """
    # pylint: disable=too-many-locals

    if len(data) == 0:
        print(no_results_message or "No data.")
        return

    def get_datum_as_string(row: Dict[str, Any], col: str) -> str:
        """
        Get the value of a column as a string. If SQLAlchemy returned None
        for a value, substitute "NULL".
        """
        if (val := row.get(col)) is None:
            return "NULL"

        return str(val)

    def make_output_line(
        fields: list[str], delim: str = "|", pad_char: str = " "
    ) -> str:
        """
        Format a single output line from a result set, ensuring that the
        output is suitably padding and aligned.
        """
        return (
            f"{delim}{pad_char}"
            + f"{pad_char}{delim}{pad_char}".join(fields)
            + f"{pad_char}{delim}"
        )

    # For each column, figure out how wide to make it in the display, based on
    # the data.
    widths: dict[str, int] = {}
    for col in columns:
        widths[col] = len(col)

    for col in columns:
        for row in data:
            datum = get_datum_as_string(row, col)
            current = widths.get(col, 0)
            widths[col] = max(current, len(str(datum)))

    # Display the header, padding each column name appropriately. Also,
    # calculate the header separators as we go.
    fields = []
    sep = []
    for col in columns:
        width = widths[col]
        fields.append(col.ljust(width))
        sep.append("-" * width)

    print(make_output_line(sep, "+", "-"))
    print(make_output_line(fields))
    print(make_output_line(sep, "+", "-"))

    # Now, the rows. Using the calculated width, we pad each value
    # appropriately.
    for row in data:
        fields = []
        for col in columns:
            datum = get_datum_as_string(row, col)
            width = widths[col]
            fields.append(datum.ljust(width))

        print(make_output_line(fields))

    print(make_output_line(sep, "+", "-"))

    if limit > 0:
        suffix = "s" if total > 1 else ""
        epilog = f"{len(data):,} of {total:,} row{suffix}"
    else:
        assert total == len(data)
        suffix = "s" if len(data) > 1 else ""
        epilog = f"{len(data):,} row{suffix}"

    if elapsed is not None:
        epilog = f"{epilog} ({elapsed:.03f} seconds)"

    print(f"{epilog}\n")
    return


def run_sql(
    sql: str,
    engine: sqlalchemy.Engine,
    limit: int = 0,
    echo_statement: bool = False,
    no_results_message: str | None = None,
) -> None:
    """
    Run a SQL statement.

    :param sql: The SQL statement to run
    :param engine: The SQLAlchemy engine for the database
    :param limit: The current row limit setting, or 0 for no limit
    :param echo_statement: Whether or not to echo the statement before
        running it
    :param no_results_message: The message to display if there are no results,
        or None for the default
    """

    try:
        if echo_statement:
            print(f"{sql}\n")

        start = perf_counter()
        with Session(engine) as session:
            try:
                with session.execute(sqlalchemy.text(sql)) as cursor:
                    mappings = cursor.mappings()
                    columns = list(mappings.keys())

                    data = []
                    total = 0
                    while (row := mappings.fetchone()) is not None:
                        total += 1
                        if (limit == 0) or (total <= limit):
                            data.append(row)

                    elapsed = perf_counter() - start
                    display_results(
                        columns=columns,
                        data=data,
                        limit=limit,
                        total=total,
                        elapsed=elapsed,
                        no_results_message=no_results_message
                    )
                session.commit()

            except sqlalchemy.exc.ResourceClosedError:
                # Thrown when attempting to get a result from something that
                # doesn't produce results, such as an INSERT, UPDATE, or DELETE.
                # Just return quietly. Make sure to commit any work, though.
                session.commit()

    except sqlalchemy.exc.SQLAlchemyError as e:
        error(str(e))

    # pylint: disable=broad-except
    except Exception as e:
        error(f"{type(e)}: {e}")
        traceback.print_exception(e, file=sys.stdout)


def print_help(command: str | None = None) -> None:
    """
    Display the help output.

    :param command: The command for which help is being requested, or None
         for general help on all commands
    """
    if command is None:
        help_topics = list(HELP)
    else:
        help_topics = []
        for commands, prefix, text in HELP:
            if command in commands:
                help_topics.append((commands, prefix, text))
        if len(help_topics) == 0:
            error(f'Unknown command "{command}".')
            return

    prefix_width = 0
    for _, prefix, _ in help_topics:
        prefix_width = max(prefix_width, len(prefix))

    # How much room do we have left for text? Allow for separating " - ".

    max_width = SCREEN_WIDTH - 1 # 1-character right margin
    separator = " - "
    text_width = max_width - len(separator) - prefix_width
    if text_width < 0:
        # Screw it. Just pick some value.
        text_width = DEFAULT_SCREEN_WIDTH // 2

    for _, prefix, text in help_topics:
        padded_prefix = prefix.ljust(prefix_width)
        text_lines = textwrap.wrap(text, width=text_width)
        print(f"{padded_prefix}{separator}{text_lines[0]}")
        for text_line in text_lines[1:]:
            padding = " " * (prefix_width + len(separator))
            print(f"{padding}{text_line}")

    if command is None:
        print("")
        for line in HELP_EPILOG:
            wrapped = textwrap.fill(line, width=SCREEN_WIDTH)
            print(wrapped)


def show_schema(table_name: str, engine: Engine) -> None:
    """
    Print the schema for a table.

    :param table_name: The table to describe
    :param engine: The SQLAlchemy engine for the database
    """

    def show_create_table_ddl(t: sqlalchemy.Table) -> None:
        """
        Get the CREATE TABLE statement from SQLAlchemy, and display that.
        """

        schema_str = str(CreateTable(t).compile(engine)).strip()
        # Replace any hard tabs with 2 spaces.
        schema_str = schema_str.replace("\t", "  ")
        print(f"\n{schema_str}\n")

    # Validate that the table exists first, using SQLAlchemy. This strategy
    # ensures a consistent "not found" message across database types. It's
    # especially helpful with SQLite, where issuing the "pragma" statement
    # for a non-existent table just returns nothing, not even an error message
    # (even in the sqlite3 shell).
    tables = get_tables(engine)
    match [t for t in tables if t.name.lower() == table_name.lower()]:
        case []:
            error(f'Table "{table_name}" does not exist.')
            return
        case [t]:
            table = t
        case many:
            raise SQLShellException(
                f'Too many matches for "{table_name}": {many}'
            )

    # If there's a SQL statement that will generate a nice tabular result,
    # use that. Otherwise, just pull the "CREATE TABLE" statement out of
    # SQLAlchemy, and display that.
    #
    # TODO: Extend for other database types.
    sql = None
    match engine.name:
        case EngineName.SQLITE:
            sql = f"pragma table_info([{table_name}])"
        case EngineName.MYSQL:
            sql = f"desc {table_name}"
        case EngineName.POSTGRES:
            sql = (
                "select column_name, data_type, character_maximum_length, "
                "is_nullable, column_default from information_schema.columns "
                f"where table_name = '{table_name}'"
            )
        case _:
            pass

    if sql is None:
        show_create_table_ddl(table)
    else:
        run_sql(sql=sql, engine=engine, echo_statement=True)


def show_tables(engine: Engine) -> None:
    """
    Show all tables in the open database.

    :param engine: The SQLAlchemy engine for the database
    """
    for t in get_tables(engine):
        print(t.name)


def show_tables_matching(line: str, engine: Engine) -> None:
    """
    Show all table names matching a specified regular expression. This
    function takes the entire input line, including the ".tables" command.
    It re-splits it using shlex, which allows quoting of the regular expression.

    :param line: The entire input line
    :param engine: The SQLAlchemy engine for the database
    """

    try:
        tokens = shlex.split(line)
        assert tokens[0] == Command.TABLES.value

        if len(tokens) != 2:
            raise ValueError("Too many parameters.")

        pat = re.compile(tokens[1], re.I)
        for t in get_tables(engine):
            if pat.search(t.name) is not None:
                print(t.name)

    except re.error as e:
        error(f"Bad regular expression: {e}")

    except ValueError as e:
        error(str(e))


def show_indexes(table_name: str, engine: Engine) -> None:
    """
    Given a table name, display the indexes (if any), associated with the
    table.

    :param table_name: The name of the table for which indexes are to be shown
    :param engine: The SQLAlchemy engine for the database
    """
    # pylint: disable=invalid-name
    NO_RESULTS_MESSAGE = "No indexes."

    def show_generic_indexes() -> None:
        """
        Use SQLAlchemy's generic methods to display indexes.
        """
        inspector = sqlalchemy.inspect(engine)
        indexes = inspector.get_indexes(table_name)
        adjusted_data: list[dict[str, str]] = []
        for idx in indexes:
            adj_dict: dict[str, str] = {}
            adj_dict["table"] = table_name
            adj_dict["name"] = idx.get("name") or "?"
            columns = (cast(list, idx.get("column_names")) or [])
            adj_dict["columns"] = ", ".join(columns)
            unique = "true" if idx.get("unique", False) else "false"
            adj_dict["unique"] = unique
            adjusted_data.append(adj_dict)

        display_results(
            columns=["table", "name", "columns", "unique"],
            data=adjusted_data,
            limit=0,
            total=len(adjusted_data),
            no_results_message=NO_RESULTS_MESSAGE
        )

    # Validate that the table exists first, using SQLAlchemy. This strategy
    # ensures a consistent "not found" message across database types. It's
    # especially helpful with SQLite, where issuing the "pragma" statement
    # for a non-existent table just returns nothing, not even an error message
    # (even in the sqlite3 shell).
    tables = get_tables(engine)
    match [t for t in tables if t.name.lower() == table_name.lower()]:
        case []:
            error(f'Table "{table_name}" does not exist.')
            return
        case [_]:
            pass
        case many:
            raise SQLShellException(
                f'Too many matches for "{table_name}": {many}'
            )

    # If there's a SQL statement that will generate a nice tabular result,
    # use that. Otherwise, just pull the "CREATE TABLE" statement out of
    # SQLAlchemy, and display that.
    #
    # TODO: Extend for other database types.
    sql = None
    match engine.name:
        case EngineName.SQLITE:
            sql = (
                "select * from sqlite_master where type = 'index' and "
                f"tbl_name = '{table_name}'"
            )
        case EngineName.MYSQL:
            sql = f"show index from {table_name}"
        case EngineName.POSTGRES:
            sql = f"select * from pg_indexes where tablename = '{table_name}'"
        case _:
            pass

    if sql is None:
        show_generic_indexes()
    else:
        run_sql(
            sql=sql,
            engine=engine,
            echo_statement=True,
            no_results_message=NO_RESULTS_MESSAGE
        )


def show_foreign_keys(table_name: str, engine: Engine) -> None:
    """
    Show the foreign keys defined on a table, if any. Uses database-specific
    SQL, if known; otherwise, uses SQLAlchemy generic methods.

    :param table_name: The name of the table for which foreign keys are to be
        shown
    :param engine: The SQLAlchemy engine for the database
    """
    # pylint: disable=invalid-name
    NO_RESULTS_MESSAGE = "No foreign keys."

    def show_generic_indexes() -> None:
        """
        Display foreign keys using SQLAlchemy's generic methods.
        """
        inspector = sqlalchemy.inspect(engine)
        indexes = inspector.get_foreign_keys(table_name)
        adjusted_data: list[dict[str, str]] = []
        for idx in indexes:
            adj_dict: dict[str, str] = {}
            adj_dict["name"] = idx.get("name") or "?"
            adj_dict["columns"] = ", ".join(
                idx.get("constrained_columns") or []
            )
            adj_dict["references"] = idx.get("referred_table") or "?"
            adj_dict["references_columns"] = ", ".join(
                idx.get("referred_columns") or []
            )
            adjusted_data.append(adj_dict)

        display_results(
            columns=["name", "columns", "references", "references_columns"],
            data=adjusted_data,
            limit=0,
            total=len(adjusted_data),
            no_results_message=NO_RESULTS_MESSAGE
        )

    # Validate that the table exists first, using SQLAlchemy. This strategy
    # ensures a consistent "not found" message across database types. It's
    # especially helpful with SQLite, where issuing the "pragma" statement
    # for a non-existent table just returns nothing, not even an error message
    # (even in the sqlite3 shell).
    tables = get_tables(engine)
    match [t for t in tables if t.name.lower() == table_name.lower()]:
        case []:
            error(f'Table "{table_name}" does not exist.')
            return
        case [_]:
            pass
        case many:
            raise SQLShellException(
                f'Too many matches for "{table_name}": {many}'
            )

    # If there's a SQL statement that will generate a nice tabular result,
    # use that. Otherwise, just pull the "CREATE TABLE" statement out of
    # SQLAlchemy, and display that.
    #
    # TODO: Extend for other database types.
    sql = None
    match engine.name:
        case EngineName.SQLITE:
            sql = f"pragma foreign_key_list([{table_name}])"
        case EngineName.MYSQL:
            sql = (
                # Note: Must escape (with double quotes) "table", "column",
                # and "references", as they are reserved words.
                "select constraint_name as name, "
                'constraint_schema as "database", '
                'table_name as "table", '
                'column_name as "column", '
                'table_schema as referenced_database, '
                'referenced_table_name as references_table, '
                "referenced_column_name as references_column "
                "from information_schema.key_column_usage "
                "where referenced_table_schema = (select database()) and "
                f"table_name = '{table_name}'"
            )
        case EngineName.POSTGRES:
            sql = (
                "SELECT conname AS constraint_name, "
                "conrelid::regclass AS table_name, "
                "a.attname AS column_name,"
                "confrelid::regclass AS foreign_table_name, "
                "af.attname AS foreign_column_name "
                "FROM pg_constraint AS c "
                "JOIN pg_attribute AS a ON a.attnum = ANY(c.conkey) "
                "AND a.attrelid = c.conrelid "
                "JOIN pg_class AS cl ON cl.oid = c.conrelid "
                "JOIN pg_namespace AS nsp ON nsp.oid = cl.relnamespace "
                "JOIN pg_attribute AS af ON af.attnum = ANY(c.confkey) "
                "AND af.attrelid = c.confrelid "
                "WHERE c.contype = 'f' "
                f"AND cl.relname = '{table_name}' "
                "AND nsp.nspname = 'public'"
            )
        case _:
            pass

    if sql is None:
        show_generic_indexes()
    else:
        run_sql(
            sql=sql,
            engine=engine,
            echo_statement=True,
            no_results_message=NO_RESULTS_MESSAGE
        )


def format_history_item(line: str, index: int) -> str:
    """
    Format a single history line.

    :param line: The history line
    :param index: The index (number) of the history line
    """
    return f"{index:5d}. {line}"


def show_history(total: int = 0) -> None:
    """
    Display the history.

    :param total: How many history lines to show, or 0 for all of them.
    """
    history_length = readline.get_current_history_length()
    # History indexes are 1-based, not 0-based.
    history_items = [
        (i, readline.get_history_item(i)) for i in range(1, history_length)
    ]

    match total:
        case n if n <= 0:
            pass
        case n if n > 0:
            history_items = history_items[-n:]

    for i, line in history_items:
        print(format_history_item(line, i))


def show_history_matching(line: str) -> None:
    """
    Show all history items matching a specified regular expression. This
    function takes the entire input line, including the ".history" command.
    It re-splits it using shlex, which allows quoting of the regular expression.

    :param line: the input line containing the regular expression
    """

    try:
        tokens = shlex.split(line)
        assert tokens[0] == Command.HISTORY.value

        if len(tokens) != 2:
            raise ValueError("Too many parameters.")

        pat = re.compile(tokens[1])
        history_length = readline.get_current_history_length()
        for i in range(1, history_length + 1):
            hist_line = readline.get_history_item(i)
            if pat.search(hist_line) is not None:
                print(format_history_item(hist_line, i))

    except re.error as e:
        error(f"Bad regular expression: {e}")

    except ValueError as e:
        error(str(e))


def import_table(
    table_name: str,
    import_file: Path,
    engine: Engine,
    exist_ok: bool
) -> None:
    """
    Import a file into a table. If the table doesn't exist, it is created.
    If it does exist, the code tries to append to the table.

    If the file ends in ".csv", the table is imported from a CSV file. If the
    file ends in ".json", it is assumed to be a JSON Lines file, as would be
    produced by the export_table() function.

    The code uses Pandas to import the file.

    :param table_name:  the name of the table to import into
    :param import_file: the path to the file to import
    :param engine:      the SQLAlchemy engine of the database
    :param exist_ok:    if True, the table can already exist. Otherwise, it's
                        an error if the table exists.
    """
    # Pandas is a big library that can take a noticeable amount of time to
    # load, so we only import it if we need it.
    print("Loading Pandas...")
    # pylint: disable=import-outside-toplevel
    import pandas as pd

    match import_file.suffix:
        case ".csv":
            df = pd.read_csv(import_file)

        case ".json":
            df = pd.read_json(import_file, lines=True)

        case ext:
            error(f'"{ext}" is not a valid file extension for import.')
            return

    tables = get_tables(engine)
    exists = any(t.name.lower() == table_name.lower() for t in tables)
    if exists and not exist_ok:
        error(f'Table "{table_name}" already exists, and you specified -n.')
        return

    # With Postgres, if the column names in the incoming Pandas data frame
    # are mixed case, Pandas will create the columns in the table as mixed
    # case, which means they'll have to be quoted in SQL. Force them all to
    # lower case, first.
    column_map: dict[str, str] = {}
    for col in df.columns:
        column_map[col] = col.lower()
    df.rename(column_map, axis="columns", inplace=True)

    df.to_sql(
        name=table_name,
        if_exists="append" if exist_ok else "fail",
        con=engine,
    )


def export_table(table_name: str, where: Path, engine: Engine) -> None:
    """
    Export a table to a text file. If the file ("where") ends in ".csv",
    the table is exported to a CSV file. If the file ends in ".json",
    the table is exported in JSON Lines format.

    :param table_name: the name of the table to export
    :param where: the path of the CSV file to overwrite
    :param engine: the SQLAlchemy engine of the database
    """

    def export_csv(mappings: MappingResult) -> None:
        """
        Export a table as CSV to the file.
        """
        print(f"Exporting {table_name} as CSV to {where} ...")
        with open(where, mode="w", encoding="utf-8") as f:
            columns = list(mappings.keys())
            c_out = csv.DictWriter(f, fieldnames=columns)
            c_out.writeheader()
            while (row := mappings.fetchone()) is not None:
                # pylint: disable=use-dict-literal
                c_out.writerow(dict(row))

    def export_json(mappings: MappingResult) -> None:
        """
        Export the table as JSON (lines) to a file.
        """
        print(f"Exporting {table_name} as JSON (lines) to {where} ...")
        with open(where, mode="w", encoding="utf-8") as f:
            while (row := mappings.fetchone()) is not None:
                out: dict[str, Any] = {}
                for key, value in row.items():
                    if isinstance(value, datetime):
                        out[key] = value.isoformat()
                    elif isinstance(value, date):
                        out[key] = value.strftime("%Y-%m-%d")
                    else:
                        out[key] = value

                print(json.dumps(out), file=f)

    sql = f"select * from {table_name}"

    match where.suffix:
        case ".csv":
            export = export_csv
        case ".json":
            export = export_json
        case "":
            error(
                "Cannot determine export format, because export file "
                "has no extension."
            )
            return
        case ext:
            error(
                "Cannot determine export format, because file extension "
                f'"{ext}" is not ".csv" or ".json".'
            )
            return

    try:
        where = where.expanduser()
        with Session(engine) as session:
            with session.execute(sqlalchemy.text(sql)) as cursor:
                export(cursor.mappings())
    # pylint: disable=broad-except
    except Exception as e:
        print(f"Export failed: {e}")
        traceback.print_exception(e, file=sys.stdout)


def lookup_db_url(configuration: Configuration | None,
                  name: str,
                  history: Path) -> Tuple[str, Path]:
    """
    Look up a database URL in the configuration. The passed name might be
    a complete URL, or it might be a name that matches a section in the
    configuration file.

    :param configuration: configuration object or None
    :param name:          (partial or full) name of config section, or a
                          complete URL
    :param history:       path to the history file to use (from the command
                          line) or the default history file

    :returns: a tuple of the URL and the history file path

    :raises: TooManyMatchesError if `name` matches more than one config section
    """

    if configuration is None:
        return (name, history)

    match configuration.lookup(name):
        case None:
            # Use the db_spec as the URL, with the default history.
            return (name, history)

        case [cfg]:
            url = cfg.url
            if cfg.history_file is not None:
                history_file = cfg.history_file
            else:
                history_file = history

            return (url, history_file)

        case []:
            # Should not happen.
            assert False

        case configs:
            match_str = ", ".join([c.name for c in configs])
            raise TooManyMatchesError(textwrap.fill(
                f'"{name}" matches more than one section in '
                f'"{configuration.path}": {match_str}',
                width=SCREEN_WIDTH
            ))



def connect_to_new_db(
    db_spec: str,
    configuration: Configuration | None,
    history_file: Path
) -> Tuple[Engine | None, Path]:
    """
    Connect to a database. Intended to be called only as a result of the
    Command.CONNECT command.

    :param db_spec: the string representing the config section or URL to which
        to connect
    :param configuration: the loaded configuration, or None
    :param history_file: The path to the history file to use by default

    :returns: the SQLAlchemy engine for the database and the history file path.
        If the connection fails, None is returned and the current engine is
        unchanged.
    """
    try:
        url, history_file = lookup_db_url(configuration, db_spec, history_file)
    except TooManyMatchesError as e:
        error(str(e))
        return (None, history_file)

    assert url is not None
    try:
        print(f"Connecting to {url} ...")
        engine = engine_cache.get(url)
        if engine is None:
            engine = sqlalchemy.create_engine(url)

        # Some databases don't complain about a bad URL until the first
        # operation, so let's try to get the list of tables.
        get_tables(engine)
        engine_cache[url] = engine
        return (engine, history_file)
    except sqlalchemy.exc.SQLAlchemyError as e:
        error(f"Unable to connect to {url}: {e}")
        return (None, history_file)


# pylint: disable=too-many-statements
def run_command_loop(
    db_spec: str,
    configuration: Configuration | None,
    history_path: Path
) -> None:
    """
    Read and process commands.

    :param db_url: the SQLAlchemy URL of the database to which to connect
    :param history_path: the path to the history file to use, which does not
        have to exist
    """
    # pylint: disable=too-many-locals
    def make_prompt(engine: Engine) -> str:
        """
        Make the prompt for the command loop.

        :param engine: the SQLAlchemy engine
        """
        return f"({engine.name}) > "

    def prepare_readline(
        engine: Engine,
        history_file: Path,
        save_history: Callable[[], None] | None = None
    ) -> Callable[[], None]:
        """
        Prepare readline for the command loop.

        :param engine: the SQLAlchemy engine
        :param history_file: the path to the history file
        """
        save_history = init_history(history_file, save_history)
        init_bindings_and_completion(engine)
        return save_history


    print(f"{NAME}, version {VERSION}\n")

    e: Engine | None = None
    e, history_path = connect_to_new_db(db_spec, configuration, history_path)
    if e is None:
        # Already reported.
        return

    current_engine: Engine = e

    save_history = prepare_readline(current_engine, history_path)
    prompt = make_prompt(current_engine)

    print()
    print(f".help for help on {NAME} commands")

    digits = re.compile(r"^\d+$")

    limit = 0
    while True:
        try:
            # input() automatically uses the readline library, if it's
            # been loaded.
            line = input(prompt)
            match line.split():
                case []:
                    pass

                case [(Command.QUIT1.value | Command.QUIT2.value)]:
                    break

                case [(Command.QUIT1.value | Command.QUIT2.value), *_]:
                    print(f"{Command.QUIT1.value} and {Command.QUIT2.value} "
                          "take nor parameters.")

                case [Command.CONNECT.value, spec]:
                    e, history_path = connect_to_new_db(
                        spec, configuration, history_path
                    )
                    if e is not None:
                        # Connection successful.
                        current_engine = e
                        prompt = make_prompt(current_engine)
                        # Save the existing history, and start a new one.
                        save_history = prepare_readline(
                            current_engine, history_path, save_history
                        )

                case [Command.CONNECT.value, *_]:
                    print(f"Usage: {Command.CONNECT.value} <db_spec>")

                case [(Command.HELP1.value | Command.HELP2.value)]:
                    print_help()

                case [(Command.HELP1.value | Command.HELP2.value), topic]:
                    print_help(topic)

                case [(Command.HELP1.value | Command.HELP2.value), *_]:
                    print(f"{Command.HELP1.value} and {Command.HELP2.value} "
                          "take no parameters.")

                case [Command.FKEYS.value, table_name]:
                    show_foreign_keys(table_name, current_engine)

                case [Command.FKEYS.value, *_]:
                    print(f"Usage: {Command.FKEYS.value} <table_name>")

                case [Command.IMPORT.value, table_name, path]:
                    import_table(
                        table_name=table_name,
                        import_file=Path(path),
                        engine=current_engine,
                        exist_ok=True
                    )

                case [Command.IMPORT.value, "-n", table_name, path]:
                    import_table(
                        table_name=table_name,
                        import_file=Path(path),
                        engine=current_engine,
                        exist_ok=False
                    )

                case [Command.IMPORT.value, *_]:
                    print(f"Usage: {Command.IMPORT.value} [-n] <table> <path>")

                case [Command.INDEXES.value, table_name]:
                    show_indexes(table_name, current_engine)

                case [Command.INDEXES.value, *_]:
                    print(f"Usage: {Command.INDEXES.value} <table_name>")

                case [Command.LIMIT.value]:
                    print(f"Limit is currently {limit:,}.")

                case [Command.LIMIT.value, s] if digits.match(s) is not None:
                    limit = int(s)

                case [Command.LIMIT.value, _]:
                    print(".limit takes a non-negative integer")

                case [Command.LIMIT.value, *_]:
                    print("Usage: .limit <n>")

                case [Command.TABLES.value]:
                    show_tables(current_engine)

                case [Command.TABLES.value, *_]:
                    show_tables_matching(line, current_engine)

                case [Command.SCHEMA.value, table_name]:
                    show_schema(table_name, current_engine)

                case [Command.SCHEMA.value, *_]:
                    print(f"Usage: {Command.SCHEMA.value} <table_name>")

                case [Command.EXPORT.value, table_name, path]:
                    export_table(
                        table_name=table_name,
                        where=Path(path),
                        engine=current_engine
                    )

                case [Command.EXPORT.value, *_]:
                    print(f"Usage: {Command.EXPORT.value} <table> <path>")

                case [Command.URL.value]:
                    print(current_engine.url)

                case [Command.URL.value, *_]:
                    print("{Command.URL.value} takes no arguments.")

                case [Command.HISTORY.value]:
                    show_history()

                case [Command.HISTORY.value, n] if digits.match(n) is not None:
                    show_history(int(n))

                case [Command.HISTORY.value, *_]:
                    # show_history_matching() will re-split the line using
                    # shell semantics.
                    show_history_matching(line)

                case [Command.HISTORY.value, *_]:
                    print("Usage: .history n | pattern")

                case [cmd, *_] if cmd.startswith("."):
                    print(f'"{cmd}" is an unknown "." command.')

                case _:
                    run_sql(sql=line, engine=current_engine, limit=limit)

        except EOFError:
            # Ctrl-D to input().
            print()
            break


def load_config(config: Path) -> Configuration | None:
    """
    Reads the configuration file, if it exists. Returns a dictionary
    where the keys are names (sections) from the configuration and the
    values are ConnectionConfig objects. The dictionary will be empty,
    if there is no configuration file or if the configuration file is
    empty. Raises ConfigurationError on error.

    :param config: Path to the configuration file, which does not have to
        exist
    """
    # These two imports are deliberately inside the function, because
    # they are implementation-dependent. We don't want to import them at
    # the top, because if we change how the configuration is loaded, it
    # should be isolated completely in here.
    # pylint: disable=import-outside-toplevel
    import tomllib
    from string import Template

    assert config.exists()

    try:
        with open(config, mode='rb') as f:
            data = tomllib.load(f)
    except Exception as e:
        # pylint: disable=raise-missing-from
        raise ConfigurationError(f'Unable to read "{config}": {e}')

    class EnvDict(dict):
        """
        For environment substitution, we want a reference to a non-existent
        variable to substitute "", rather than throw an error (as with
        Template.substitute()) or leave the reference intact (as with
        Template.safe_substitute()). To do that, we simply use a custom
        dictionary class.
        """
        def __init__(self: Self, *args, **kw) -> None:
            """Initialize the dictionary"""
            self.update(*args, **kw)

        def __getitem__(self: Self, key: Any) -> Any:
            """Get an item from the dictionary"""
            return super().get(key, "")

    env = EnvDict(**os.environ)

    configs: list[ConnectionConfig] = []
    for key, values in data.items():
        url = values.get("url")
        if url is None:
            raise ConfigurationError(
                f'"{config}": Section "{key}" has no "url" setting.'
            )

        t = Template(url)
        url = t.substitute(env)

        history = values.get("history")
        if history is not None:
            t = Template(history)
            history = Path(t.substitute(env)).expanduser()

        configs.append(ConnectionConfig(
            name=key,
            url=url,
            history_file=history
        ))

    return Configuration(configs=configs, path=config)


@click.command(
    name=NAME,
    context_settings=CLICK_CONTEXT_SETTINGS
)
@click.option(
    "-H",
    "--history",
    is_flag=False,
    default=str(DEFAULT_HISTORY_FILE),
    show_default=True,
    help="Specify location of the default history file. This can be "
         "overridden, on a per-connection basis, in the configuration."
)
@click.option(
    "-c",
    "--config",
    is_flag=False,
    default=str(DEFAULT_CONFIG_FILE),
    show_default=True,
    type=click.Path(dir_okay=False),
    help="The location of the optional configuration file."
)
@click.version_option(VERSION)
@click.argument("db_spec", required=True, type=str)
def main(db_spec: str, history: str, config: str) -> None:
    """
    Prompt for SQL statements and run them against the specified database.
    The <DB_SPEC> parameter is either a SQLAlchemy-compatible URL or the name
    of a section in the configuration file from which the URL can be read.
    Note: To connect to some databases, you will need to install support
    packages.

    Examples:

    MySQL:      mysql+mysqlconnector://user:password@localhost/mydatabase

                Requires "pip install mysql-connector-python". There are other
                MySQL connection libraries you can use, as well. See
                https://docs.sqlalchemy.org/en/20/dialects/mysql.html

    PostgreSQL: postgresql+pg8000://user:password@localhost/mydatabase

                Requires "pip install pg8000". There are other PostgreSQL
                connection libraries you can use, as well. See
                https://docs.sqlalchemy.org/en/20/dialects/postgresql.html

    SQLite3:    sqlite:///mydatabase.db

                Support for SQLite3 is built into Python, so you don't need
                to install anything additional to work with SQLite.

    Other databases supported by SQLAlchemy should work fine, though this
    tool has only been tested with MySQL, PostgreSQL, and SQLite3.

    This SQL shell uses the Python readline library, which may use libedit
    (editline) or GNU Readline under the covers, depending on the operating
    system and how Python was compiled. If using GNU Readline, the shell will
    read bindings and settings from ".inputrc" in your home directory. If using
    editline, it will read those values from ".editrc" in your home directory.
    The shell will display the path of the file it is loading.
    """

    try:
        configuration: Configuration | None = None
        p_config = Path(config)
        if not p_config.exists():
            print(f'WARNING: Configuration file "{config}" does not exist.')
        elif not p_config.is_file():
            raise AbortError(f'Configuration file "{config}" is not a file.')
        else:
            configuration = load_config(Path(config))

        run_command_loop(db_spec, configuration, Path(history))

    except (AbortError, ConfigurationError, TooManyMatchesError) as e:
        print(str(e), file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    # pylint: disable=no-value-for-parameter
    main()
