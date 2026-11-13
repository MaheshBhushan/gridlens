from pyspark.sql.types import StringType, StructField, StructType

from gridlens.bronze import RAW_SCHEMA
from gridlens.silver import ALL_RULES, apply_quality

GOOD = ("MAC000002", "Std", "2012-10-12 00:30:00.0000000", "0 ", "f.csv")

CASES = [
    (GOOD, None),
    (("MAC000002", "Std", "2012-10-12 01:00:00.0000000", "0.143 ", "f.csv"), None),
    ((None, "Std", "2012-10-12 01:30:00.0000000", "0.1", "f.csv"), "invalid_household_id"),
    (("XYZ12", "Std", "2012-10-12 01:30:00.0000000", "0.1", "f.csv"), "invalid_household_id"),
    (("MAC000002", "Flat", "2012-10-12 02:00:00.0000000", "0.1", "f.csv"), "invalid_tariff"),
    (("MAC000002", "Std", "12/10/2012 02:30", "0.1", "f.csv"), "unparseable_timestamp"),
    (("MAC000002", "Std", None, "0.1", "f.csv"), "unparseable_timestamp"),
    (("MAC000002", "Std", "2012-10-12 02:45:00.0000000", "0.1", "f.csv"), "off_interval_timestamp"),
    (("MAC000002", "Std", "2012-10-12 03:00:00.0000000", "Null", "f.csv"), "missing_kwh"),
    (("MAC000002", "Std", "2012-10-12 03:30:00.0000000", "", "f.csv"), "missing_kwh"),
    (("MAC000002", "Std", "2012-10-12 04:00:00.0000000", "abc", "f.csv"), "non_numeric_kwh"),
    (("MAC000002", "Std", "2012-10-12 04:30:00.0000000", "-0.2", "f.csv"), "kwh_out_of_range"),
    (("MAC000002", "Std", "2012-10-12 05:00:00.0000000", "42", "f.csv"), "kwh_out_of_range"),
    # Exact duplicate of GOOD: the second copy is quarantined.
    (("MAC000002", "Std", "2012-10-12 00:30:00.0000000", "0 ", "g.csv"), "duplicate_reading"),
    # An invalid row sharing a key with a valid row must not turn the valid row into a duplicate.
    (("MAC000002", "Std", "2012-10-12 01:00:00.0000000", "Null", "a.csv"), "missing_kwh"),
]


def test_each_row_gets_expected_rule(spark):
    schema = StructType([*RAW_SCHEMA.fields, StructField("source_file", StringType())])
    bronze = spark.createDataFrame([c[0] for c in CASES], schema)
    clean, rejected = apply_quality(bronze)
    assert clean.count() + rejected.count() == len(CASES)

    got = {(r["datetime_raw"], r["source_file"], r["lcl_id"], r["std_or_tou"]): r["dq_rule"] for r in rejected.collect()}
    for row, expected in CASES:
        key = (row[2], row[4], row[0], row[1])
        assert got.get(key) == expected, (row, got.get(key))

    clean_rows = clean.orderBy("ts").collect()
    assert [(r["household_id"], r["slot"], r["kwh"]) for r in clean_rows] == [
        ("MAC000002", 1, 0.0),
        ("MAC000002", 2, 0.143),
    ]
    assert str(clean_rows[0]["ts"]) == "2012-10-12 00:30:00"


def test_rule_names_unique():
    names = [r.name for r in ALL_RULES]
    assert len(names) == len(set(names))
