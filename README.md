# Spark Network Analysis

A distributed Apache Spark ETL and network-analysis pipeline developed to analyse the Stanford SNAP Berkeley–Stanford web graph.

The project uses PySpark to clean and transform a large directed-edge dataset, calculate the structural in-degree of each destination node, identify the Top 50 dominant nodes, and capture Spark execution metrics through the Web UI.

## Project Objectives

This implementation demonstrates:

- A standalone Spark cluster using Docker Compose
- One Spark Master and two Spark Workers
- Distributed processing across four CPU cores
- PySpark DataFrame-based ETL
- Lazy evaluation and DAG execution
- In-degree aggregation of a directed graph
- DataFrame persistence using `MEMORY_AND_DISK`
- Shuffle-partition configuration
- Top 50 destination-node identification
- Execution analysis using jobs, stages and executor metrics
- Data-skew assessment across worker nodes

## Dataset

The project uses the Stanford SNAP Berkeley–Stanford web graph:

- Dataset page: https://snap.stanford.edu/data/web-BerkStan.html
- Download: https://snap.stanford.edu/data/web-BerkStan.txt.gz
- Nodes: 685,230
- Directed edges: 7,600,595
- Collected: 2002

Each valid record represents a directed hyperlink:

```text
source_node    destination_node
```

Lines beginning with `#` contain metadata and are removed by the PySpark ETL pipeline.

### Dataset preparation

1. Download `web-BerkStan.txt.gz`.
2. Extract the compressed file.
3. Place the uncompressed file here:

```text
data/web-BerkStan.txt
```

The dataset is excluded from Git because of its size.

## Project Structure

```text
spark-network-analysis/
├── data/
│   └── web-BerkStan.txt
├── jobs/
│   └── network_analysis.py
├── output/
│   └── top_50_nodes/
├── docker-compose.yml
├── requirements.txt
├── .gitignore
└── README.md
```

## Architecture

```text
PySpark application
        |
        v
Spark Driver
        |
        v
Spark Standalone Master
        |
        +-------------------+
        |                   |
        v                   v
Spark Worker 1        Spark Worker 2
2 cores, 2 GB         2 cores, 2 GB
        |                   |
        v                   v
    Executor 0          Executor 1
```

The Driver builds and submits the execution plan. The Spark Master manages cluster resources, while executors on the two Worker nodes process data partitions in parallel.

## Cluster Configuration

| Service | Purpose | Resources | Browser address |
|---|---|---:|---|
| Spark Master | Coordinates applications and workers | Cluster manager | `http://localhost:8080` |
| Spark Worker 1 | Executes distributed tasks | 2 cores, 2 GB | `http://localhost:8091` |
| Spark Worker 2 | Executes distributed tasks | 2 cores, 2 GB | `http://localhost:8092` |
| Spark Application UI | Displays live job metrics | Available during execution | `http://localhost:4040` |

The Worker UIs use host ports `8091` and `8092`. Both workers use port `8081` internally, but different host ports avoid conflicts with other services.

## Shared Storage

The following bind mounts make the local project folders available inside all Spark containers:

```yaml
volumes:
  - ./data:/opt/spark/data:ro
  - ./jobs:/opt/spark/jobs
  - ./output:/opt/spark/output
```

| Local folder | Container path | Purpose |
|---|---|---|
| `./data` | `/opt/spark/data` | Input dataset, mounted as read-only |
| `./jobs` | `/opt/spark/jobs` | PySpark application scripts |
| `./output` | `/opt/spark/output` | Generated results |

Files written to `/opt/spark/output` inside a container appear in the local `output` folder.

## Prerequisites

Install the following software:

- Docker Desktop
- Docker Compose
- Git
- A web browser
- VS Code or another code editor

A local Spark or Java installation is not required because the Spark runtime is supplied by Docker.

## Python Dependency

The documented PySpark version is:

```text
pyspark==3.5.8
```

The Spark Docker image already contains the required runtime. Therefore, installing `requirements.txt` on the host machine is not required for the Docker-based workflow.

## Starting the Cluster

Open PowerShell or a terminal in the project directory and run:

```powershell
docker compose up -d
```

Check the containers:

```powershell
docker compose ps
```

The expected containers are:

```text
spark-master
spark-worker-1
spark-worker-2
```

Open the Spark Master dashboard:

```text
http://localhost:8080
```

Confirm that:

- The Master is alive.
- Two Workers are registered.
- Each Worker provides two cores.
- Each Worker provides 2 GB of memory.
- The cluster has four cores and 4 GB of worker memory in total.

## Running the PySpark Application

Submit the application through the running Master container:

```powershell
docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client --executor-cores 2 --executor-memory 1g --conf spark.cores.max=4 --conf spark.ui.port=4040 /opt/spark/jobs/network_analysis.py
```

### Submission options

| Option | Purpose |
|---|---|
| `docker exec spark-master` | Runs the command inside the Master container |
| `/opt/spark/bin/spark-submit` | Submits the PySpark application |
| `--master spark://spark-master:7077` | Connects the Driver to the standalone Spark Master |
| `--deploy-mode client` | Runs the Driver inside the submitting container |
| `--executor-cores 2` | Allocates up to two cores to each executor |
| `--executor-memory 1g` | Allocates 1 GB of memory to each executor |
| `spark.cores.max=4` | Allows the application to use all four cluster cores |
| `spark.ui.port=4040` | Exposes the live Spark Application UI |
| `network_analysis.py` | Specifies the PySpark application |

## ETL and Analysis Pipeline

The application performs the following processing flow:

```text
Read uncompressed text file
        |
        v
Remove metadata and blank lines
        |
        v
Split source and destination IDs
        |
        v
Cast node IDs to numeric values
        |
        v
Remove malformed records
        |
        v
Cache parsed edge DataFrame
        |
        v
Group by destination node
        |
        v
Calculate structural in-degree
        |
        v
Cache aggregated DataFrame
        |
        v
Sort by descending in-degree
        |
        v
Select Top 50 destination nodes
        |
        v
Write results to CSV
```

## Lazy Evaluation

Transformations such as `filter()`, `select()`, `groupBy()` and `orderBy()` are evaluated lazily.

Spark first records these operations as a Directed Acyclic Graph. Execution begins only when an action requests a result, including:

```python
count()
show()
write()
```

This allows Spark to optimize the complete transformation plan before distributing its tasks across the cluster.

## Memory Optimization

The application persists reusable DataFrames with:

```python
StorageLevel.MEMORY_AND_DISK
```

Spark stores available partitions in executor memory and uses disk as a fallback when necessary. This prevents repeated text loading and transformation when the same DataFrame is used by multiple actions.

A broadcast join is not applied because the pipeline does not join the main edge DataFrame with a second small reference DataFrame. Caching is the more appropriate optimization for this workflow.

## Shuffle Operations

The following operations require data redistribution:

- Grouping edges by destination node
- Aggregating in-degree counts
- Sorting nodes by in-degree

Records sharing the same destination must be transferred to suitable partitions before aggregation. This transfer appears as shuffle read and shuffle write in the Spark Web UI.

The application configures:

```python
spark.sql.shuffle.partitions = 8
```

This creates eight partitions for DataFrame shuffle operations.

## Application Output

Successful execution creates:

```text
output/top_50_nodes/
├── _SUCCESS
└── part-00000-....csv
```

The part file contains columns similar to:

```text
destination,in_degree
```

The `_SUCCESS` file is an empty marker confirming that Spark completed the write operation successfully.

The script uses `coalesce(1)` before writing so that the Top 50 result is stored in one CSV part file.

## Spark Web Interfaces

### Master UI

```text
http://localhost:8080
```

Shows:

- Registered Workers
- Available cores and memory
- Running and completed applications
- Worker status

### Worker UIs

```text
http://localhost:8091
http://localhost:8092
```

Show:

- Worker resources
- Running executors
- Application information
- Worker logs

### Application UI

```text
http://localhost:4040
```

Available while the PySpark application is active. It contains:

- Jobs
- Stages
- Storage
- Environment
- Executors
- SQL/DataFrame queries
- DAG visualizations
- Shuffle read/write metrics

The application may include a temporary waiting period to keep this interface available for metric inspection and screenshots.

## Observed Execution Metrics

The recorded execution included:

- 12 completed Spark jobs
- 13 completed stages
- 10 skipped stages through result reuse
- Initial ETL duration: approximately 18 seconds
- Initial stage input: 105.4 MiB
- In-degree shuffle write: 4.3 MiB
- Corresponding shuffle read: 4.3 MiB
- No failed tasks
- No significant executor-level data skew

### Executor comparison

| Metric | Executor 0 | Executor 1 |
|---|---:|---:|
| Completed tasks | 30 | 27 |
| Task time | 29 seconds | 27 seconds |
| Input | 67.9 MiB | 62.6 MiB |
| Shuffle read | 2.2 MiB | 2.1 MiB |
| Shuffle write | 2.0 MiB | 2.3 MiB |
| Storage memory | 5.1 MiB | 5.4 MiB |
| RDD blocks | 6 | 6 |

The close task counts, durations, input volumes and shuffle metrics indicate that the workload was distributed relatively evenly between both workers.

Execution times may differ between computers depending on available CPU, memory, Docker configuration and background workload.

## Stopping the Cluster

Stop the containers while preserving the project files:

```powershell
docker compose down
```

Start them again later with:

```powershell
docker compose up -d
```

The bind-mounted dataset, scripts and outputs remain in the local project folders.

## Troubleshooting

### Container-name conflict

If Docker reports that `spark-master` is already in use:

```powershell
docker ps -a --filter "name=spark-master"
```

Remove the old container only if it is no longer needed:

```powershell
docker stop spark-master
docker rm spark-master
```

Then restart the cluster:

```powershell
docker compose up -d
```

### Port already in use

Check whether another application is using ports:

```text
4040
7077
8080
8091
8092
```

Change only the host side of a port mapping if necessary:

```yaml
ports:
  - "NEW_HOST_PORT:CONTAINER_PORT"
```

### `/opt/spark/...` is not recognized

Paths beginning with `/opt/spark` exist inside the Linux container, not directly in Windows PowerShell.

Use:

```powershell
docker exec spark-master ...
```

before the Spark command.

### Application UI is unavailable

Port `4040` is normally available only while a Spark application is running. Submit the job and open:

```text
http://localhost:4040
```

while the application remains active.

### Dataset not found

Confirm that the uncompressed file exists at:

```text
data/web-BerkStan.txt
```

Inside the containers, it should appear as:

```text
/opt/spark/data/web-BerkStan.txt
```

### Output already exists

The PySpark application uses overwrite mode, so rerunning it replaces the previous `top_50_nodes` output.

## Reproducibility Notes

To reproduce the project:

1. Clone the repository.
2. Download and extract the SNAP dataset.
3. Place `web-BerkStan.txt` in `data/`.
4. Start the Docker Compose cluster.
5. Verify both workers in the Master UI.
6. Submit the application using `spark-submit`.
7. Review the generated CSV and Spark Web UI metrics.

## Technologies

- Apache Spark 3.5.8
- PySpark
- Python
- Docker
- Docker Compose
- Spark Standalone Cluster
- Stanford SNAP Dataset

## Dataset Citation

J. Leskovec, K. Lang, A. Dasgupta and M. Mahoney, “Community Structure in Large Networks: Natural Cluster Sizes and the Absence of Large Well-Defined Clusters,” *Internet Mathematics*, vol. 6, no. 1, pp. 29–123, 2009.

Dataset source:

https://snap.stanford.edu/data/web-BerkStan.html
