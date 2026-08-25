from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, split, trim
from pyspark.storagelevel import StorageLevel
import time


# Paths inside the Docker containers
INPUT_PATH = "/opt/spark/data/web-BerkStan.txt"
OUTPUT_PATH = "/opt/spark/output/top_50_nodes"


def create_spark_session():
    """Create and configure the Spark session."""

    spark = (
        SparkSession.builder
        .appName("BerkStan Network In-Degree Analysis")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark


def load_raw_data(spark):
    """Load the SNAP network dataset as raw text."""

    return spark.read.text(INPUT_PATH)


def parse_network_data(raw_df):
    """
    Remove metadata headers and convert the raw text into
    a structured DataFrame containing directed graph edges.
    """

    # Remove blank lines and SNAP metadata lines beginning with #
    data_lines_df = raw_df.filter(
        (trim(col("value")) != "")
        & (~trim(col("value")).startswith("#"))
    )

    # Split each line using tabs or other whitespace
    split_df = data_lines_df.withColumn(
        "parts",
        split(trim(col("value")), r"\s+")
    )

    # Create typed source and destination columns
    edges_df = (
        split_df
        .filter(col("parts").getItem(1).isNotNull())
        .select(
            col("parts").getItem(0).cast("long").alias("source"),
            col("parts").getItem(1).cast("long").alias("destination")
        )
        .filter(
            col("source").isNotNull()
            & col("destination").isNotNull()
        )
    )

    return edges_df


def calculate_in_degree(edges_df):
    """
    Count incoming links for every destination node.
    """

    return (
        edges_df
        .groupBy("destination")
        .agg(count("*").alias("in_degree"))
    )


def calculate_in_degree_distribution(in_degree_df):
    """
    Calculate how many nodes have each observed in-degree.
    """

    return (
        in_degree_df
        .groupBy("in_degree")
        .agg(count("*").alias("node_count"))
        .orderBy(col("in_degree").desc())
    )


def identify_top_nodes(in_degree_df, limit=50):
    """
    Identify destination nodes with the highest in-degree.
    """

    return (
        in_degree_df
        .orderBy(
            col("in_degree").desc(),
            col("destination").asc()
        )
        .limit(limit)
    )


def save_results(top_nodes_df):
    """Save the Top 50 destination nodes as CSV output."""

    (
        top_nodes_df
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(OUTPUT_PATH)
    )


def main():
    spark = create_spark_session()

    try:
        print("\n==========================================")
        print("BERKSTAN NETWORK IN-DEGREE ANALYSIS")
        print("==========================================")
        print(f"Spark version: {spark.version}")
        print(f"Spark master: {spark.sparkContext.master}")
        print(f"Input path: {INPUT_PATH}")
        print(f"Shuffle partitions: {spark.conf.get('spark.sql.shuffle.partitions')}")

        # Step 1: Load raw text
        raw_df = load_raw_data(spark)

        # Step 2: Define parsing and cleaning transformations
        edges_df = parse_network_data(raw_df)

        # Cache because the edges are reused by multiple actions
        edges_df.persist(StorageLevel.MEMORY_AND_DISK)

        # First action: executes the lazy loading and parsing pipeline
        edge_count = edges_df.count()

        print(f"\nValid directed edges processed: {edge_count:,}")
        print(f"Input partitions: {edges_df.rdd.getNumPartitions()}")

        print("\nParsed DataFrame schema:")
        edges_df.printSchema()

        print("Sample parsed edges:")
        edges_df.show(10, truncate=False)

        # Step 3: Calculate the in-degree of each destination node
        in_degree_df = calculate_in_degree(edges_df)

        # Cache because it is reused for distribution and Top 50 analysis
        in_degree_df.persist(StorageLevel.MEMORY_AND_DISK)

        # Materialise the cached in-degree DataFrame
        destination_count = in_degree_df.count()

        print(f"Distinct destination nodes: {destination_count:,}")

        # Calculate the structural in-degree distribution
        distribution_df = calculate_in_degree_distribution(in_degree_df)

        print("\nHighest observed in-degree values and node frequencies:")
        distribution_df.show(20, truncate=False)

        # Step 4: Identify the Top 50 dominant destination nodes
        top_50_df = identify_top_nodes(in_degree_df, 50)

        print("\nTop 50 destination nodes by in-degree:")
        top_50_df.show(50, truncate=False)

        # Step 5: Save the result
        save_results(top_50_df)

        print(f"\nResults saved to: {OUTPUT_PATH}")
        print("Analysis completed successfully.")

        print("\nKeeping the Spark application active for 10 minutes.")
        print("Open the Spark UI at: http://localhost:4040")

        time.sleep(600)

        # Release cached DataFrames
        in_degree_df.unpersist()
        edges_df.unpersist()

    finally:
        spark.stop()


if __name__ == "__main__":
    main()