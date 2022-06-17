import unittest
import preql
import time
import datetime
from data_diff import database as db
from data_diff.diff_tables import TableDiffer, TableSegment, split_space
from parameterized import parameterized, parameterized_class
from .common import CONN_STRINGS, DEFAULT_N_SAMPLES, N_SAMPLES, str_to_checksum
import logging

CONNS = {k: db.connect_to_uri(v) for k, v in CONN_STRINGS.items()}
CONNS[db.MySQL].query("SET @@session.time_zone='+00:00'", None)


class Faker:
    pass


class DateTimeFaker:
    def __init__(self, max):
        self.max = max

    def __iter__(self):
        self.prev = datetime.datetime(2000, 1, 1, 0, 0, 0, 0)
        self.i = 0
        return self

    def __len__(self):
        return self.max

    def __next__(self) -> str:
        if self.i < self.max:
            self.prev = self.prev + datetime.timedelta(seconds=3, microseconds=571)
            self.i += 1
            return str(self.prev)
        else:
            raise StopIteration


class IntFaker:
    def __init__(self, max):
        self.max = max

    def __iter__(self):
        self.prev = -128
        self.i = 0
        return self

    def __len__(self):
        return self.max

    def __next__(self) -> str:
        if self.i < self.max:
            self.prev += 1
            self.i += 1
            return str(self.prev)
        else:
            raise StopIteration


class FloatFaker:
    def __init__(self, max):
        self.max = max

    def __iter__(self):
        self.prev = -10.0001
        self.i = 0
        return self

    def __len__(self):
        return self.max

    def __next__(self) -> str:
        if self.i < self.max:
            self.prev += 0.00571
            self.i += 1
            return str(self.prev)
        else:
            raise StopIteration


TYPE_SAMPLES = {
    "int": IntFaker(N_SAMPLES),
    "datetime_no_timezone": DateTimeFaker(N_SAMPLES),
    "float": FloatFaker(N_SAMPLES),
}

DATABASE_TYPES = {
    db.Postgres: {
        # https://www.postgresql.org/docs/current/datatype-numeric.html#DATATYPE-INT
        "int": [
            # "smallint",  # 2 bytes
            # "int", # 4 bytes
            # "bigint", # 8 bytes
        ],
        # https://www.postgresql.org/docs/current/datatype-datetime.html
        "datetime_no_timezone": [
            "timestamp(6) without time zone",
            "timestamp(3) without time zone",
            "timestamp(0) without time zone",
        ],
        # https://www.postgresql.org/docs/current/datatype-numeric.html
        "float": [
            # "real",
            # "double precision",
            # "numeric(6,3)",
        ],
    },
    db.MySQL: {
        # https://dev.mysql.com/doc/refman/8.0/en/integer-types.html
        "int": [
            # "tinyint", # 1 byte
            # "smallint", # 2 bytes
            # "mediumint", # 3 bytes
            # "int", # 4 bytes
            # "bigint", # 8 bytes
        ],
        # https://dev.mysql.com/doc/refman/8.0/en/datetime.html
        "datetime_no_timezone": ["timestamp(6)", "timestamp(3)", "timestamp(0)", "timestamp", "datetime(6)"],
        # https://dev.mysql.com/doc/refman/8.0/en/numeric-types.html
        "float": [
            # "float",
            # "double",
            # "numeric",
        ],
    },
    db.BigQuery: {
        "datetime_no_timezone": [
            "timestamp",
            # "datetime",
        ],
    },
    db.Snowflake: {
        # https://docs.snowflake.com/en/sql-reference/data-types-numeric.html#int-integer-bigint-smallint-tinyint-byteint
        "int": [
            # all 38 digits with 0 precision, don't need to test all
            # "int",
            # "integer",
            # "bigint",
            # "smallint",
            # "tinyint",
            # "byteint"
        ],
        # https://docs.snowflake.com/en/sql-reference/data-types-datetime.html
        "datetime_no_timezone": [
            "timestamp(0)",
            "timestamp(3)",
            "timestamp(6)",
            "timestamp(9)",
        ],
        # https://docs.snowflake.com/en/sql-reference/data-types-numeric.html#decimal-numeric
        "float": [
            # "float"
            # "numeric",
        ],
    },
    db.Redshift: {
        "int": [
            # "int",
        ],
        "datetime_no_timezone": [
            # "TIMESTAMP",
        ],
        # https://docs.aws.amazon.com/redshift/latest/dg/r_Numeric_types201.html#r_Numeric_types201-floating-point-types
        "float": [
            # "float4",
            # "float8",
            # "numeric",
        ],
    },
    db.Oracle: {
        "int": [
            # "int",
        ],
        "datetime_no_timezone": [
            # "timestamp",
            # "timestamp(6)",
            # "timestamp(9)",
        ],
        "float": [
            # "float",
            # "numeric",
        ],
    },
    db.Presto: {
        "int": [
            # "tinyint", # 1 byte
            # "smallint", # 2 bytes
            # "mediumint", # 3 bytes
            # "int", # 4 bytes
            # "bigint", # 8 bytes
        ],
        "datetime_no_timezone": ["timestamp(6)", "timestamp(3)", "timestamp(0)", "timestamp", "datetime(6)"],
        "float": [
            # "float",
            # "double",
            # "numeric",
        ],
    },
}


type_pairs = []
# =>
# { source: (preql, connection)
# target: (preql, connection)
# source_type: (int, tinyint),
# target_type: (int, bigint) }
for source_db, source_type_categories in DATABASE_TYPES.items():
    for target_db, target_type_categories in DATABASE_TYPES.items():
        for (
            type_category,
            source_types,
        ) in source_type_categories.items():  # int, datetime, ..
            for source_type in source_types:
                for target_type in target_type_categories.get(type_category, []):
                    if CONNS.get(source_db, False) and CONNS.get(target_db, False):
                        type_pairs.append(
                            (
                                source_db,
                                target_db,
                                source_type,
                                target_type,
                                type_category,
                            )
                        )

# Pass --verbose to test run to get a nice output.
def expand_params(testcase_func, param_num, param):
    source_db, target_db, source_type, target_type, type_category = param.args
    source_db_type = source_db.__name__
    target_db_type = target_db.__name__
    return "%s_%s_%s_to_%s_%s" % (
        testcase_func.__name__,
        source_db_type,
        parameterized.to_safe_name(source_type),
        target_db_type,
        parameterized.to_safe_name(target_type),
    )


class TestDiffCrossDatabaseTables(unittest.TestCase):
    # https://docs.python.org/2/library/unittest.html#unittest.TestCase.maxDiff
    # For showing assertEqual differences under a certain length.
    maxDiff = 10000

    @parameterized.expand(type_pairs, name_func=expand_params)
    def test_types(self, source_db, target_db, source_type, target_type, type_category):
        self.src_conn = src_conn = CONNS[source_db]
        self.dst_conn = dst_conn = CONNS[target_db]

        self.connections = [self.src_conn, self.dst_conn]
        sample_values = TYPE_SAMPLES[type_category]

        src_table_name = f"{parameterized.to_safe_name(source_type)}_{N_SAMPLES}"
        src_table_path = src_conn.parse_table_name(f"src_{src_table_name}")
        src_table = src_conn.quote(".".join(src_table_path))

        dst_table_name = f"dst_{parameterized.to_safe_name(target_type)}_src_{source_db.__name__.lower()}_{src_table_name}"
        # Since we insert `src` to `dst`, the `dst` can be different for the
        # same type. 😭
        dst_table_path = dst_conn.parse_table_name(dst_table_name)
        dst_table = dst_conn.quote(".".join(dst_table_path))

        # If you want clean house from e.g. changing fakers.
        # src_conn.query(f"DROP TABLE IF EXISTS {src_table}", None)
        # dst_conn.query(f"DROP TABLE IF EXISTS {dst_table}", None)

        self.table = TableSegment(self.src_conn, src_table_path, "id", None, ("col",), case_sensitive=False)
        self.table2 = TableSegment(self.dst_conn, dst_table_path, "id", None, ("col",), case_sensitive=False)

        start = time.time()
        already_seeded = self._create_table(src_conn, src_table, source_type)
        if not already_seeded:
            self._insert_to_table(
                src_conn, src_table, source_type, enumerate(sample_values, 1)
            )
        insertion_source_duration = round(time.time() - start, 2)

        start = time.time()
        already_seeded = self._create_table(dst_conn, dst_table, target_type)
        if not already_seeded:
            values_in_source = src_conn.query(f"SELECT id, col FROM {src_table}", list)
            self._insert_to_table(dst_conn, dst_table, target_type, values_in_source)
        insertion_target_duration = round(time.time() - start, 2)

        start = time.time()
        self.assertEqual(len(sample_values), self.table.count())
        count1_duration = round(time.time() - start, 2)

        start = time.time()
        self.assertEqual(len(sample_values), self.table2.count())
        count2_duration = round(time.time() - start, 2)

        # Ensure we actually checksum even for few samples!
        ch_threshold = 5_000 if N_SAMPLES > DEFAULT_N_SAMPLES else 3
        ch_factor = (
            min(max(int(N_SAMPLES / 250_000), 2), 12)
            if N_SAMPLES > DEFAULT_N_SAMPLES
            else 2
        )
        ch_threads = 12

        start = time.time()
        differ = TableDiffer(
            bisection_threshold=ch_threshold,
            bisection_factor=ch_factor,
            max_threadpool_size=ch_threads,
            debug=True,
        )
        diff = list(differ.diff_tables(self.table, self.table2))
        expected = []
        self.assertEqual(expected, diff)
        self.assertEqual(0, differ.stats.get("rows_downloaded", 0))
        checksum_duration = round(time.time() - start, 2)

        start = time.time()
        # Ensure that Python agrees with the checksum!
        dl_factor = int(N_SAMPLES / 100_000) if N_SAMPLES > DEFAULT_N_SAMPLES else 2
        dl_threshold = int(N_SAMPLES / dl_factor) + 1
        dl_threads = 8

        differ = TableDiffer(
            bisection_threshold=dl_threshold,
            bisection_factor=dl_factor,
            max_threadpool_size=dl_threads,
        )
        diff = list(differ.diff_tables(self.table, self.table2))
        expected = []
        self.assertEqual(expected, diff)
        self.assertEqual(len(sample_values), differ.stats.get("rows_downloaded", 0))
        download_duration = round(time.time() - start, 2)

        print(
            f"\nsource_db={source_db.__name__} target_db={target_db.__name__} rows={N_SAMPLES} src_table={src_table} target_table={dst_table} source_type={repr(source_type)} target_type={repr(target_type)} insertion_source={insertion_source_duration}s insertion_target={insertion_target_duration}s count_source={count1_duration}s count_target={count2_duration}s checksum={checksum_duration}s download={download_duration}s download_threads={dl_threads} download_bisection_factor={dl_factor} download_bisection_threshold={dl_threshold} checksum_threads={ch_threads} checksum_bisection_factor={ch_factor} checksum_bisection_threshold={ch_threshold}"
        )

    def _create_table(self, conn, table, type) -> bool:
        conn.query(f"CREATE TABLE IF NOT EXISTS {table}(id int, col {type})", None)
        # print(conn.query("SHOW TABLES FROM public", list))
        # print(conn.query("SELECT * FROM INFORMATION_SCHEMA.TABLES", list))
        already_seeded = conn.query(f"SELECT COUNT(*) FROM {table}", int) == N_SAMPLES
        return already_seeded

    def _insert_to_table(self, conn, table, type, values):
        conn.query(f"TRUNCATE {table}", None)
        default_insertion_query = f"INSERT INTO {table} (id, col) VALUES "
        insertion_query = default_insertion_query

        for j, sample in values:
            if isinstance(conn, db.Presto):
                if type.startswith("timestamp"):
                    sample = f"timestamp '{sample}'"  #  must be cast...
            else:
                sample = f"'{sample}'"

            insertion_query += f"({j}, {sample}),"

            if j % 10_000 == 0:
                conn.query(insertion_query[0:-1], None)
                insertion_query = default_insertion_query

        if insertion_query != default_insertion_query:
            conn.query(insertion_query[0:-1], None)

        if not isinstance(conn, db.BigQuery):
            conn.query("COMMIT", None)
