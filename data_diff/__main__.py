import sys
import time
import json
import logging
from itertools import islice

from .diff_tables import (
    TableSegment,
    TableDiffer,
    DEFAULT_BISECTION_THRESHOLD,
    DEFAULT_BISECTION_FACTOR,
    parse_table_name,
)
from .database import connect_to_uri, parse_table_name
from .parse_time import parse_time_before_now, UNITS_STR, ParseError

import rich
import click

LOG_FORMAT = "[%(asctime)s] %(levelname)s - %(message)s"
DATE_FORMAT = "%H:%M:%S"

COLOR_SCHEME = {
    "+": "green",
    "-": "red",
}


@click.command()
@click.argument("db1_uri")
@click.argument("table1_name")
@click.argument("db2_uri")
@click.argument("table2_name")
@click.option("-k", "--key-column", default="id", help="Name of primary key column")
@click.option("-t", "--update-column", default=None, help="Name of updated_at/last_updated column")
@click.option("-c", "--columns", default=[], multiple=True, help="Names of extra columns to compare")
@click.option("-l", "--limit", default=None, help="Maximum number of differences to find")
@click.option("--bisection-factor", default=DEFAULT_BISECTION_FACTOR, help="Segments per iteration")
@click.option(
    "--bisection-threshold",
    default=DEFAULT_BISECTION_THRESHOLD,
    help="Minimal bisection threshold. Below it, data-diff will download the data and compare it locally.",
)
@click.option(
    "--min-age",
    default=None,
    help="Considers only rows older than specified. Useful for specifying replication lag."
    "Example: --min-age=5min ignores rows from the last 5 minutes. "
    f"\nValid units: {UNITS_STR}",
)
@click.option("--max-age", default=None, help="Considers only rows younger than specified. See --min-age.")
@click.option("-s", "--stats", is_flag=True, help="Print stats instead of a detailed diff")
@click.option("-d", "--debug", is_flag=True, help="Print debug info")
@click.option("--json", 'json_output', is_flag=True, help="Print JSONL output for machine readability")
@click.option("-v", "--verbose", is_flag=True, help="Print extra info")
@click.option("-i", "--interactive", is_flag=True, help="Confirm queries, implies --debug")
@click.option("--keep-column-case", is_flag=True, help="Don't use the schema to fix the case of given column names.")
@click.option(
    "-j",
    "--threads",
    default="1",
    help="Number of worker threads to use per database. Default=1. "
    "A higher number will increase performance, but take more capacity from your database. "
    "'serial' guarantees a single-threaded execution of the algorithm (useful for debugging).",
)
def main(
    db1_uri,
    table1_name,
    db2_uri,
    table2_name,
    key_column,
    update_column,
    columns,
    limit,
    bisection_factor,
    bisection_threshold,
    min_age,
    max_age,
    stats,
    debug,
    verbose,
    interactive,
    threads,
    keep_column_case,
    json_output,
):
    if limit and stats:
        print("Error: cannot specify a limit when using the -s/--stats switch")
        return
    if interactive:
        debug = True

    if debug:
        logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT, datefmt=DATE_FORMAT)
    elif verbose:
        logging.basicConfig(level=logging.INFO, format=LOG_FORMAT, datefmt=DATE_FORMAT)

    threaded = True
    if threads is not None:
        if threads.lower() == "serial":
            threaded = False
            threads = 1
        else:
            try:
                threads = int(threads)
            except ValueError:
                logger.error("Error: threads must be a number, 'auto', or 'serial'.")
                return
            if threads < 1:
                logging.error("Error: threads must be >= 1")
                return

    db1 = connect_to_uri(db1_uri, threads)
    db2 = connect_to_uri(db2_uri, threads)

    if interactive:
        db1.enable_interactive()
        db2.enable_interactive()

    start = time.time()

    try:
        options = dict(
            min_update=max_age and parse_time_before_now(max_age),
            max_update=min_age and parse_time_before_now(min_age),
            case_sensitive=keep_column_case,
        )
    except ParseError as e:
        logging.error("Error while parsing age expression: %s" % e)
        return

    table1 = TableSegment(db1, parse_table_name(table1_name), key_column, update_column, columns, **options)
    table2 = TableSegment(db2, parse_table_name(table2_name), key_column, update_column, columns, **options)

    differ = TableDiffer(
        bisection_factor=bisection_factor,
        bisection_threshold=bisection_threshold,
        threaded=threaded,
        max_threadpool_size=threads and threads * 2,
        debug=debug,
    )
    diff_iter = differ.diff_tables(table1, table2)

    if limit:
        diff_iter = islice(diff_iter, int(limit))

    if stats:
        diff = list(diff_iter)
        unique_diff_count = len({i[0] for _, i in diff})
        max_table_count = max(differ.stats["table1_count"], differ.stats["table2_count"])
        percent = 100 * unique_diff_count / (max_table_count or 1)
        plus = len([1 for op, _ in diff if op == "+"])
        minus = len([1 for op, _ in diff if op == "-"])

        if json_output:
            json_output = {
                "different_rows": len(diff),
                "different_percent": percent,
                "different_+": plus,
                "different_-": minus,
                "total": max_table_count,
            }
            print(json.dumps(json_output))
        else:
            print(f"Diff-Total: {len(diff)} changed rows out of {max_table_count}")
            print(f"Diff-Percent: {percent:.14f}%")
            print(f"Diff-Split: +{plus}  -{minus}")
    else:
        for op, columns in diff_iter:
            color = COLOR_SCHEME[op]

            if json_output:
                jsonl = json.dumps([op, list(columns)])
                rich.print(f"[{color}]{jsonl}[/{color}]")
            else:
                text = f"{op} {', '.join(columns)}"
                rich.print(f"[{color}]{text}[/{color}]")

            sys.stdout.flush()

    end = time.time()

    logging.info(f"Duration: {end-start:.2f} seconds.")


if __name__ == "__main__":
    main()
