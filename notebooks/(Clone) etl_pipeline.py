from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql import types as T
import re, sys, json

def log(msg, layer):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    meta = f"[layer={layer}]"
    print(f"{ts} {meta} {msg}")

def convert_duration(seconds):
    if seconds >= 60:
        m, s = divmod(seconds, 60)
        return f"{int(m)}m {s:.0f}s"
    return f"{seconds:.1f}s"

def read_csv(path):
    try:
        return (
            spark.read
            .option("header", "true")
            .option("sep", ",")
            .option("multiLine", "true")
            .option("escape", "\"")
            .option("quote", "\"")
            .option("inferSchema", "true")
            .csv(path)
        )
    except Exception as e:
        print(f"ERROR: {e}")
        sys.exit(1)

CATALOG = "workspace"
SCHEMA = "us_accidents_project"
DB = f"{CATALOG}.{SCHEMA}"

source_path = "/Workspace/Shared/us-accidents-project/src/"

bronze_dir = f"/Volumes/{CATALOG}/{SCHEMA}/bronze"
silver_dir = f"/Volumes/{CATALOG}/{SCHEMA}/silver"
gold_dir = f"/Volumes/{CATALOG}/{SCHEMA}/gold"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {DB}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {DB}.bronze")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {DB}.silver")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {DB}.gold")

def clean_column_names(df):
    for col in df.columns:
        clean_col = re.sub(r'[ ,;{}()\n\t=]', '_', col).strip('_')
        if col != clean_col:
            df = df.withColumnRenamed(col, clean_col)
    return df

def winsorize_iqr(df, cols):
    for col in cols:
        if col not in df.columns:
            continue

        quantiles = df.approxQuantile(col, [0.25, 0.75], 0.01)
        if quantiles is None or len(quantiles) < 2:
            continue

        q1, q3 = quantiles
        if q1 is None or q3 is None:
            continue

        iqr = q3 - q1
        if iqr is None or iqr == 0:
            continue

        lower = q1 - 1.5 * iqr
        upper = q3 + 1.5 * iqr

        df = df.withColumn(
            col,
            F.when(F.col(col) < lower, lower)
             .when(F.col(col) > upper, upper)
             .otherwise(F.col(col))
        )
    return df

def add_metric_features(df):
    cols_to_drop = []

    if "Distance_mi" in df.columns:
        df = df.withColumn("Distance_km", F.col("Distance_mi") * F.lit(1.60934))
        cols_to_drop.append("Distance_mi")
    if "Visibility_mi" in df.columns:
        df = df.withColumn("Visibility_km", F.col("Visibility_mi") * F.lit(1.60934))
        cols_to_drop.append("Visibility_mi")
    if "Temperature_F" in df.columns:
        df = df.withColumn(
            "Temperature_C",
            (F.col("Temperature_F") - F.lit(32.0)) * F.lit(5.0 / 9.0)
        )
        cols_to_drop.append("Temperature_F")
    if "Wind_Chill_F" in df.columns:
        df = df.withColumn(
            "Wind_Chill_C",
            (F.col("Wind_Chill_F") - F.lit(32.0)) * F.lit(5.0 / 9.0)
        )
        cols_to_drop.append("Wind_Chill_F")
    if "Wind_Speed_mph" in df.columns:
        df = df.withColumn(
            "Wind_Speed_kmh",
            F.col("Wind_Speed_mph") * F.lit(1.60934)
        )
        cols_to_drop.append("Wind_Speed_mph")
    if "Pressure_in" in df.columns:
        df = df.withColumn(
            "Pressure_hPa",
            F.col("Pressure_in") * F.lit(33.8639)
        )
        cols_to_drop.append("Pressure_in")
    if "Precipitation_in" in df.columns:
        df = df.withColumn(
            "Precipitation_mm",
            F.col("Precipitation_in") * F.lit(25.4)
        )
        cols_to_drop.append("Precipitation_in")

    if cols_to_drop:
        df = df.drop(*cols_to_drop)
    return df

def validate_bronze(df, table, path):
    src_rows = df.count()
    src_cols = df.columns
    probs = []

    tdf = spark.table(table)
    if tdf.count() != src_rows:
        probs.append(f"Delta rows {tdf.count()} != {src_rows}")
    if tdf.columns != src_cols:
        probs.append(f"Delta cols {tdf.columns} != {src_cols}")

    pdf = spark.read.parquet(path)
    if pdf.count() != src_rows:
        probs.append(f"Parquet rows {pdf.count()} != {src_rows}")
    if pdf.columns != src_cols:
        probs.append(f"Parquet cols {pdf.columns} != {src_cols}")

    if probs:
        raise ValueError("BRONZE validation failed: " + " | ".join(probs))
    else:
        print("BRONZE validation OK")
        print(f"rows: {src_rows}, cols: {len(src_cols)}")

def validate_silver(df):
    if df.limit(1).count() == 0:
        raise ValueError("SILVER layer is empty")

    if "Severity" in df.columns:
        if df.filter((F.col("Severity") < 1) | (F.col("Severity") > 4)).limit(1).count():
            print("!!! SILVER: out-of-range values detected in Severity")

    if "Distance_mi" in df.columns:
        if df.filter(F.col("Distance_mi") < 0).limit(1).count():
            print("!!! SILVER: negative Distance_mi found")

    if {"Start_Lat", "Start_Lng"}.issubset(df.columns):
        if df.filter(
            (F.col("Start_Lat") < -90) | (F.col("Start_Lat") > 90) |
            (F.col("Start_Lng") < -180) | (F.col("Start_Lng") > 180)
        ).limit(1).count():
            print("!!! SILVER: suspicious lat/lng values detected")

def validate_gold(df, table_name):
    if df.limit(1).count() == 0:
        raise ValueError(f"GOLD table {table_name} is empty")

def to_bronze(raw_filename):
    log("Started BRONZE", "BRONZE")
    start = datetime.now()

    df = read_csv(source_path + raw_filename)
    df = clean_column_names(df)
    df = df.withColumn("ingestion_time", F.current_timestamp())
    df = df.withColumn("event_date", F.to_date("Start_Time"))
    df = df.withColumn("source_file_name", F.lit(raw_filename))

    bronze_table = f"{DB}.bronze_us_accidents"
    bronze_path = f"{bronze_dir}/US_Accidents_March23_bronze"

    if spark.catalog.tableExists(bronze_table) and "ID" in df.columns:
        existing_ids = (spark.table(bronze_table).select("ID").distinct())
        df = df.join(existing_ids, on="ID", how="left_anti")

    df.write.mode("append").format("delta").partitionBy("event_date").saveAsTable(bronze_table)
    df.write.mode("append").partitionBy("event_date").parquet(bronze_path)
    validate_bronze(df, bronze_table, bronze_path)

    duration = (datetime.now() - start).total_seconds()
    log(f"Finished BRONZE, duration {convert_duration(duration)}", "BRONZE")

    rows_count = df.count()
    cols = df.columns
    cols_count = len(cols)

    print(f"BRONZE layer data saved to {bronze_table}")
    print(f"# of rows: {rows_count}, # of cols: {cols_count}")
    print(f"Delta table: {bronze_table}")

def to_silver():
    log("Started SILVER", "SILVER")
    start = datetime.now()

    bronze_table = f"{DB}.bronze_us_accidents"
    df = spark.table(bronze_table)

    dedup_keys = [key for key in ["ID", "Start_Time"] if key in df.columns]
    if dedup_keys:
        df = df.dropDuplicates(dedup_keys)

    #drop column with 100% false
    if "Turning_Loop" in df.columns:
        df = df.drop("Turning_Loop")
    df = add_metric_features(df)

    core_cols = [col for col in ["ID", "Severity", "Start_Time", "End_Time", "Start_Lat", "Start_Lng"] if col in df.columns]
    if core_cols:
        df = df.dropna(subset=core_cols)

    numeric_cols = [f.name for f in df.schema.fields
                    if isinstance(f.dataType, (T.IntegerType, T.LongType,
                                               T.DoubleType, T.FloatType,
                                               T.ShortType, T.DecimalType))]
    
    #Imputation
    impute_values = {}
    for col_name in numeric_cols:
        stats = (
            df.select(
                F.mean(col_name).alias("mean"),
                F.percentile_approx(col_name, 0.5).alias("median"),
                F.skewness(col_name).alias("skew")
            ).first()
        )
        mean_val = stats["mean"]
        median_val = stats["median"]
        skew_val = stats["skew"]

        if skew_val is not None and abs(skew_val) > 1:
            impute_values[col_name] = float(median_val if median_val is not None else mean_val)
        else:
            impute_values[col_name] = float(mean_val if mean_val is not None else median_val)
    df = df.fillna(impute_values)

    #FE
    if {"Start_Time", "End_Time"}.issubset(df.columns):
        df = df.withColumn(
            "accident_duration_hours",
            (F.col("End_Time").cast("long") - F.col("Start_Time").cast("long")) / 3600.0
        )

    if "Start_Time" in df.columns:
        df = (
            df.withColumn("start_hour", F.hour("Start_Time"))
              .withColumn("start_weekday", F.date_format("Start_Time", "E"))
              .withColumn(
                  "is_weekend",
                  F.expr("EXTRACT(DAYOFWEEK FROM Start_Time)").isin([6, 7])
              )
        )

    if "Weather_Condition" in df.columns:
        bad_words = ["Snow", "Ice", "Storm", "Thunder", "Rain", "Squall"]
        df = df.withColumn(
            "is_bad_weather",
            F.lower(F.col("Weather_Condition")).rlike("|".join([w.lower() for w in bad_words]))
        )

    numeric_cols = [f.name for f in df.schema.fields
                    if isinstance(f.dataType, (T.IntegerType, T.LongType,
                                               T.DoubleType, T.FloatType,
                                               T.ShortType, T.DecimalType))]
    
    print(f"Numeric cols:\n{numeric_cols}")

    discrete_to_exclude = {
        "Severity",
        "accident_duration_hours",
        "start_hour",
        "start_weekday",
        "is_weekend",
        "is_bad_weather",
    }

    def is_id_or_zip(col_name):
        name_low = col_name.lower()
        return name_low == "id" or name_low == "zipcode"

    continuous_candidates = [
        col for col in numeric_cols
        if col not in discrete_to_exclude and not is_id_or_zip(col)
    ]

    if continuous_candidates:
        distinct_counts = (
            df.select([F.approx_count_distinct(c).alias(c) for c in continuous_candidates])
            .first()
            .asDict()
        )
    else:
        distinct_counts = {}

    min_unique_for_continuous = 30
    outlier_cols = [
        col for col in continuous_candidates
        if distinct_counts.get(col, 0) > min_unique_for_continuous
    ]

    df = winsorize_iqr(df, outlier_cols)
    validate_silver(df)

    silver_table = f"{DB}.silver_us_accidents"
    silver_path = f"{silver_dir}/US_Accidents_March23_silver"

    partition_cols = ["State", "event_date"]

    delta_writer = (
        df.write
        .format("delta")
        .mode("append")
        .partitionBy(*partition_cols)
    )
    # delta_writer = (
    #     df.write
    #     .mode("overwrite")
    #     .format("delta")
    #     .option("overwriteSchema", "true")
    #     .partitionBy(*partition_cols)
    # )
    
    parquet_writer = df.write.mode("append").partitionBy(*partition_cols)
    # parquet_writer = df.write.mode("overwrite").partitionBy(*partition_cols)

    delta_writer.saveAsTable(silver_table)
    parquet_writer.parquet(silver_path)

    duration = (datetime.now() - start).total_seconds()
    log(f"Finished SILVER, duration {convert_duration(duration)}", "SILVER")

    rows_count = df.count()
    cols = df.columns
    cols_count = len(cols)

    print(f"SILVER layer data saved to {silver_path}")
    print(f"# of rows: {rows_count}, # of cols: {cols_count}")
    print(f"Delta table: {silver_table}")

def to_gold():
    log("Started GOLD", "GOLD")
    start = datetime.now()

    silver_table = f"{DB}.silver_us_accidents"
    df = spark.table(silver_table)

    state_kpi = (
        df.groupBy("State")
            .agg(
                F.count("*").alias("accident_count"),
                F.avg("Severity").alias("avg_severity"),
                F.avg("accident_duration_hours").alias("avg_duration_hours"),
                F.avg("Distance_km").alias("avg_distance_km"),
            )
            .withColumnRenamed("State", "state")
    )

    gold_state_table = f"{DB}.gold_state_kpi"
    (
        state_kpi.write
        .mode("overwrite")
        .format("delta")
        .option("overwriteSchema", "true")
        .saveAsTable(gold_state_table)
    )
    validate_gold(state_kpi, gold_state_table)
    print(f"GOLD state KPI table: {gold_state_table}")

    hour_kpi = (
        df.groupBy("start_hour")
            .agg(
                F.count("*").alias("accident_count"),
                F.avg("Severity").alias("avg_severity"),
                F.avg("accident_duration_hours").alias("avg_duration_hours"),
            )
    )

    gold_hour_table = f"{DB}.gold_hour_kpi"
    (
        hour_kpi.write
        .mode("overwrite")
        .format("delta")
        .option("overwriteSchema", "true")
        .saveAsTable(gold_hour_table)
    )
    validate_gold(hour_kpi, gold_hour_table)
    print(f"GOLD hour KPI table: {gold_hour_table}")

    duration = (datetime.now() - start).total_seconds()
    log(f"Finished GOLD, duration {convert_duration(duration)}", "GOLD")

def run_etl(raw_filename):
    total_start = datetime.now()
    log("ETL pipeline started", "pipeline")

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {DB}")

    to_bronze(raw_filename)
    to_silver()
    to_gold()

    total_duration = (datetime.now() - total_start).total_seconds()
    log(f"ETL pipeline completed in {convert_duration(total_duration)}", "pipeline")

if __name__ == "__main__":
    try:
        arg = sys.argv[1]
        params = json.loads(arg) if arg.strip() else {}
    except (IndexError, json.JSONDecodeError):
        params = {}

    raw_filename = params.get("raw_filename", "US_Accidents_March23_trimmed.csv")
    run_etl(raw_filename=raw_filename)