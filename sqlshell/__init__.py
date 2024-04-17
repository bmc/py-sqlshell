"""
This is a simple SQL command shell that works with any RDBMS that's supported
by SQLAlchemy, providing a common set of commands and a query output format
that looks the same, no matter what database you're using. In addition, it uses
Python `readline` module, so it supports history, command editing, and
rudimentary completion.

Run with -h or --help for an extended usage message.
"""

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
from typing import Any, Dict, cast

import click
import sqlalchemy
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

NAME = "sqlshell"
VERSION = "0.1.8"
CLICK_CONTEXT_SETTINGS = dict(help_option_names=["-h", "--help"])
HISTLEN = 10000
EDITLINE_BINDINGS_FILE = Path("~/.editrc").expanduser()
READLINE_BINDINGS_FILE = Path("~/.inputrc").expanduser()
DEFAULT_SCREEN_WIDTH = 79
DEFAULT_HISTORY_FILE = Path("~/.sqlshell-history").expanduser()
DEFAULT_CONFIG_FILE = Path("~/.sqlshell.cfg").expanduser()


class EngineName(StrEnum):
    POSTGRES = "postgresql"
    MYSQL = "mysql"
    SQLITE = "sqlite"


class Command(StrEnum):
    """
    Non-SQL commands the shell supports.
    """

    EXPORT = ".export"
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
    url: str
    history_file: Path | None


class ConfigurationError(Exception):
    pass


# This is a series of (command, explanation) tuples. show_help() will wrap
# the explanations.
HELP = (
    (
        f"{Command.QUIT1.value}, {Command.QUIT2.value}, or Ctrl-D",
        f"Quit {NAME}."
    ),
    (
        f"{Command.EXPORT.value} <table> <path>",
        'Export the contents of table to a file. If <path> ends in ".csv", '
        'the table will be exported to a CSV file. If <path> ends in ".json", '
        "the table will be dumped in JSON Lines format, with each row as a "
        "JSON object in the file. You can use ~ in your paths as a shorthand "
        'for your home directory (e.g., "~/table.json")',
    ),
    (
        f"{Command.FKEYS.value} <table_name>",
        "Display the list of foreign keys for a table. Note: <table_name> is "
        "the table with the foreign key constraints, not the table the "
        "foreign key(s) reference."
    ),
    (f"{Command.HELP1.value} or {Command.HELP2.value}", "Show this help."),
    (
        f"{Command.HISTORY.value} [<n>]",
        "Show the history. If <n> is supplied, show the last <n> history "
        "items. <n> of 0 is the same as omitting <n>.",
    ),
    (
        f"{Command.HISTORY.value} re",
        "Show all history items matching regular expression <re>. If your "
        "pattern contains spaces or regular expression backslash sequences "
        r"(e.g., \s), be sure to enclose it in quotes.",
    ),
    (
        f"{Command.INDEXES.value} <table_name>",
        "Display the indexes for <table_name>. Uses database-native commands, "
        "where possible. Otherwise, SQLAlchemy index information is displayed."
    ),
    (
        f"{Command.LIMIT.value} <n>",
        "Show only <n> rows from a SELECT. 0 means unlimited.",
    ),
    (f"{Command.LIMIT.value}", "Show the current limit setting"),
    (f"{Command.SCHEMA.value} <table>", "Show the schema for table <table>"),
    (f"{Command.TABLES.value}", "Show all tables in the database"),
    (
        f"{Command.TABLES.value} <re>",
        "Show all tables in the database whose names match the specified "
        "regular expression. Matching is case-blind. If your pattern contains "
        r"spaces or regular expression backslash sequences (e.g., \s), be sure "
        "to enclose it in quotes.",
    ),
    (f"{Command.URL.value}", "Show the current database URL"),
)

HELP_EPILOG = (
    "Anything else is interpreted as SQL.",
    "",
    (
        "Note that you can use tab-completion on the dot-commands. Also, "
        "as a special case, you can tab-complete available table names after "
        f'typing "{Command.SCHEMA.value}" or "{Command.INDEXES.value}". '
        "Completion for SQL statements is not available."
    ),
)


def get_tables(engine: Engine) -> list[sqlalchemy.Table]:
    """
    Get the list of Table objects in the database. The list is returned sorted
    by table name, case-blind.
    """
    metadata = sqlalchemy.MetaData()
    metadata.reflect(bind=engine)
    return sorted(list(metadata.tables.values()), key=lambda t: t.name.lower())


def init_history(history_path: Path) -> None:
    """
    Load the local readline history file.
    """
    with suppress(FileNotFoundError):
        print(f'Loading history from "{history_path}".')
        readline.read_history_file(str(history_path))
        # default history len is -1 (infinite), which may grow unruly

    readline.set_history_length(HISTLEN)
    atexit.register(readline.write_history_file, str(history_path))


def init_bindings_and_completion(engine: Engine) -> None:
    """
    Initialize readline bindings.
    """

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
            case [s, *rest] if s in (
                Command.SCHEMA.value,
                Command.INDEXES.value,
                Command.FKEYS.value
            ):
                if full_line.endswith(" "):
                    # Already fully completed.
                    options = []
                else:
                    # Special case: Options in this case are the tables in the
                    # database.
                    options = [
                        t.name
                        for t in get_tables(engine)
                        if t.name.lower().startswith(text.lower())
                    ]
            case [s, *_] if s in commands:
                # An already completed command. There's nothing to complete.
                options = []
            case [s]:
                options = [c for c in commands if c.startswith(text)]
            case _:
                options = []

        if state < len(options):
            return options[state]
        else:
            return None

    if (readline.__doc__ is not None) and ("libedit" in readline.__doc__):
        init_file = EDITLINE_BINDINGS_FILE
        print(f"Using editline (libedit).")
        completion_binding = "bind '^I' rl_complete"
    else:
        print(f"Using GNU readline.")
        init_file = READLINE_BINDINGS_FILE
        completion_binding = "Control-I: rl_complete"

    if init_file.exists():
        print(f'Loading bindings from "{init_file}"')
        readline.read_init_file(init_file)

    # Ensure that tab = complete
    readline.parse_and_bind(completion_binding)
    # Set up the completer.
    readline.set_completer(command_completer)


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

    columns            - the names of the columns, in order
    data               - list of rows. Each row is a dictionary of
                         (column -> value)
    limit              - the current row limit (for display), or 0
    total              - the total number of rows. If limit is 0, total must
                         match the length of data. Otherwise, total is the
                         number of rows that would have been displayed, if
                         limit were 0.
    no_results_message - the message to display if the results are empty.
    elapsed            - the elapsed time to run the query that produced the
                         results, or None not to display an elapsed time
    """
    if len(data) == 0:
        print(no_results_message or "No data.")
        return

    def get_datum_as_string(row: Dict[str, Any], col: str) -> str:
        if (val := row.get(col)) is None:
            return "NULL"
        else:
            return str(val)

    def make_output_line(
        fields: list[str], delim: str = "|", pad_char: str = " "
    ) -> str:
        return (
            f"{delim}{pad_char}"
            + f"{pad_char}{delim}{pad_char}".join(fields)
            + f"{pad_char}{delim}"
        )

    # For each column, figure out how wide to make it in the display, based on
    # the data.
    widths: dict[str, int] = dict()
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
    """
    from time import perf_counter

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
        print(str(e))

    except Exception as e:
        print(f"{type(e)}: {e}")
        traceback.print_exception(e, file=sys.stdout)


def print_help() -> None:
    """
    Display the help output.
    """
    prefix_width = 0
    for prefix, _ in HELP:
        prefix_width = max(prefix_width, len(prefix))

    # How much room do we have left for text? Allow for separating " - ".
    screen_width = DEFAULT_SCREEN_WIDTH
    s_width = os.environ.get("COLUMNS", str(DEFAULT_SCREEN_WIDTH))
    try:
        screen_width = int(s_width)
    except ValueError as e:
        print(
            "The COLUMNS environment variable has an invalid value of "
            f'"{s_width}". Using screen width of {DEFAULT_SCREEN_WIDTH}.'
        )

    separator = " - "
    text_width = screen_width - len(separator) - prefix_width
    if text_width < 0:
        # Screw it. Just pick some value.
        text_width = DEFAULT_SCREEN_WIDTH // 2

    for prefix, text in HELP:
        padded_prefix = prefix.ljust(prefix_width)
        text_lines = textwrap.wrap(text, width=text_width)
        print(f"{padded_prefix}{separator}{text_lines[0]}")
        for text_line in text_lines[1:]:
            padding = " " * (prefix_width + len(separator))
            print(f"{padding}{text_line}")

    print("")
    for line in HELP_EPILOG:
        wrapped = textwrap.fill(line, width=screen_width)
        print(wrapped)


def show_schema(table_name: str, engine: Engine) -> None:
    """
    Print the schema for a table.
    """

    def show_create_table_ddl(t: sqlalchemy.Table) -> None:
        """
        Get the CREATE TABLE statement from SQLAlchemy, and display that.
        """
        from sqlalchemy.schema import CreateTable

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
            print(f'Table "{table_name}" does not exist.')
            return
        case [t]:
            table = t
        case many:
            raise Exception(f'Too many matches for "{table_name}": {many}')

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
    """
    for t in get_tables(engine):
        print(t.name)


def show_tables_matching(line: str, engine: Engine) -> None:
    """
    Show all table names matching a specified regular expression. This
    function takes the entire input line, including the ".tables" command.
    It re-splits it using shlex, which allows quoting of the regular expression.
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
        print(f"Bad regular expression: {e}")

    except ValueError as e:
        print(str(e))


def show_indexes(table_name: str, engine: Engine) -> None:
    """
    Given a table name, display the indexes (if any), associated with the
    table.
    """
    NO_RESULTS_MESSAGE = "No indexes."

    def show_generic_indexes() -> None:
        inspector = sqlalchemy.inspect(engine)
        indexes = inspector.get_indexes(table_name)
        adjusted_data: list[dict[str, str]] = []
        for idx in indexes:
            adj_dict: dict[str, str] = dict()
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
            print(f'Table "{table_name}" does not exist.')
            return
        case [_]:
            pass
        case many:
            raise Exception(f'Too many matches for "{table_name}": {many}')

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
    NO_RESULTS_MESSAGE = "No foreign keys."

    def show_generic_indexes() -> None:
        inspector = sqlalchemy.inspect(engine)
        indexes = inspector.get_foreign_keys(table_name)
        adjusted_data: list[dict[str, str]] = []
        for idx in indexes:
            adj_dict: dict[str, str] = dict()
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
            print(f'Table "{table_name}" does not exist.')
            return
        case [_]:
            pass
        case many:
            raise Exception(f'Too many matches for "{table_name}": {many}')

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
                "select tc.constraint_name as name, "
                "tc.table_schema as database, "
                "tc.table_name as table, "
                "kcu.column_name as column, "
                "ccu.table_schema as referenced_database, "
                "ccu.table_name as references_table, "
                "ccu.column_name as references_column "
                "from information_schema.table_constraints as "
                "tc join information_schema.key_column_usage "
                "as kcu on tc.constraint_name = kcu.constraint_name and "
                "tc.table_schema = kcu.table_schema join "
                "information_schema.constraint_column_usage as "
                "ccu on ccu.constraint_name = tc.constraint_name and "
                "ccu.table_schema = tc.table_schema "
                "where tc.constraint_type = 'FOREIGN KEY' and "
                f"tc.table_name='{table_name}'"
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
    return f"{index:5d}. {line}"


def show_history(total: int = 0) -> None:
    """
    Display the history. If n is > 0, display only the last n history items.
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
        print(f"Bad regular expression: {e}")

    except ValueError as e:
        print(str(e))


def export_table(table_name: str, where: Path, engine: Engine) -> None:
    """
    Export a table to a text file. If the file ("where") ends in ".csv",
    the table is exported to a CSV file. If the file ends in ".json",
    the table is exported in JSON Lines format.
    """
    from sqlalchemy.engine.result import MappingResult

    def export_csv(mappings: MappingResult) -> None:
        print(f"Exporting {table_name} as CSV to {where} ...")
        with open(where, mode="w", encoding="utf-8") as f:
            columns = list(mappings.keys())
            c_out = csv.DictWriter(f, fieldnames=columns)
            c_out.writeheader()
            while (row := mappings.fetchone()) is not None:
                c_out.writerow(dict(row))

    def export_json(mappings: MappingResult) -> None:
        print(f"Exporting {table_name} as JSON (lines) to {where} ...")
        with open(where, mode="w", encoding="utf-8") as f:
            while (row := mappings.fetchone()) is not None:
                out: dict[str, Any] = dict()
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
            print(
                "Cannot determine export format, because export file "
                "has no extension."
            )
            return
        case ext:
            print(
                "Cannot determine export format, because file extension "
                f'"{ext}" is not ".csv" or ".json".'
            )
            return

    try:
        where = where.expanduser()
        with Session(engine) as session:
            with session.execute(sqlalchemy.text(sql)) as cursor:
                export(cursor.mappings())
    except Exception as e:
        print(f"Export failed: {e}")
        traceback.print_exception(e, file=sys.stdout)


def run_command_loop(db_url: str, history_path: Path) -> None:
    """
    Read and process commands.
    """
    print(f"{NAME}, version {VERSION}\n")

    print(f"Connecting to {db_url}")
    engine = sqlalchemy.create_engine(db_url)

    init_history(history_path)
    init_bindings_and_completion(engine)

    prompt = f"({engine.name}) > "

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

                case [(Command.QUIT1 | Command.QUIT2)]:
                    break

                case [(Command.QUIT1 | Command.QUIT2), *_]:
                    print(f"{Command.QUIT1.value} and {Command.QUIT2.value} "
                          "take nor parameters.")

                case [(Command.HELP1.value | Command.HELP2.value)]:
                    print_help()

                case [(Command.HELP1.value | Command.HELP2.value), *_]:
                    print(f"{Command.HELP1.value} and {Command.HELP2.value} "
                          "take no parameters.")

                case [Command.FKEYS.value, table_name]:
                    show_foreign_keys(table_name, engine)

                case [Command.FKEYS.value, *_]:
                    print(f"Usage: {Command.FKEYS.value} <table_name>")

                case [Command.INDEXES.value, table_name]:
                    show_indexes(table_name, engine)

                case [Command.INDEXES.value, *_]:
                    print(f"Usage: {Command.INDEXES.value} <table_name>")

                case [Command.LIMIT.value]:
                    print(f"Limit is currently {limit:,}.")

                case [Command.LIMIT.value, s] if digits.match(s) is not None:
                    limit = int(s)

                case [Command.LIMIT.value, _]:
                    print(f".limit takes a non-negative integer")

                case [Command.LIMIT.value, *_]:
                    print(f"Usage: .limit <n>")

                case [Command.TABLES.value]:
                    show_tables(engine)

                case [Command.TABLES.value, *_]:
                    show_tables_matching(line, engine)

                case [Command.SCHEMA.value, table_name]:
                    show_schema(table_name, engine)

                case [Command.SCHEMA.value, *_]:
                    print(f"Usage: {Command.SCHEMA.value} <table_name>")

                case [Command.EXPORT.value, table_name, path]:
                    export_table(
                        table_name=table_name, where=Path(path), engine=engine
                    )

                case [Command.EXPORT.value, *_]:
                    print(f"Usage: {Command.EXPORT.value} <table> <path>")

                case [Command.URL.value]:
                    print(engine.url)

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
                    run_sql(sql=line, engine=engine, limit=limit)

        except EOFError:
            # Ctrl-D to input().
            print()
            break


def load_config(config: Path) -> Dict[str, ConnectionConfig]:
    """
    Reads the configuration file, if it exists. Returns a dictionary
    where the keys are names (sections) from the configuration and the
    values are ConnectionConfig objects. The dictionary will be empty,
    if there is no configuration file or if the configuration file is
    empty. Raises ConfigurationError on error.
    """
    import tomllib
    from string import Template

    if not config.exists():
        return {}

    try:
        with open(config, mode='rb') as f:
            data = tomllib.load(f)
    except Exception as e:
        raise ConfigurationError(f'Unable to read "{config}": {e}')

    # For environment substitution, we want a reference to a non-existent
    # variable to substitute "", rather than throw an error (as with
    # Template.substitute()) or leave the reference intact (as with
    # Template.safe_substitute()). To do that, we simply use a custom
    # dictionary class.
    class EnvDict(dict):
        def __init__(self, *args, **kw):
            self.update(*args, **kw)

        def __getitem__(self, key) -> Any:
            return super().get(key, "")

    env = EnvDict(**os.environ)

    result: dict[str, ConnectionConfig] = {}
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

        result[key] = ConnectionConfig(url=url, history_file=history)

    return result


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
    configuration = load_config(Path(config))

    if (conn_cfg := configuration.get(db_spec)) is not None:
        url = conn_cfg.url
        history_file = conn_cfg.history_file or history
    else:
        url = db_spec
        history_file = history

    run_command_loop(url, Path(history_file))


if __name__ == "__main__":
    main()
