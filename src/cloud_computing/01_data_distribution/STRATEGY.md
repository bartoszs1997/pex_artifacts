# Data Distribution Strategy — Apache Cassandra (Airline On-Time Performance)

Design of how airline on-time performance data is distributed across the nodes
of an Apache Cassandra cluster so the system stays balanced, fault tolerant, and
fast for its main queries. The two decisions that define distribution in
Cassandra are the **partition key** (which node a row lands on) and the
**replication factor** (how many nodes keep a copy). This document justifies
both. The cluster architecture is shown in `diagram.svg`; runnable CQL is in
`schema.cql`.

## 1. Workloads (the design is driven by these)

Cassandra is query-first: pick the partition key that answers the main reads
with a single-partition lookup and, at the same time, spreads data evenly.

| # | Query the system must serve | Access pattern |
|---|---|---|
| W1 | On-time performance for an airport in a given month | filter by `origin`, `year`, `month` |
| W2 | Flight history within that airport-month | range inside one partition |
| W3 | Fleet-wide aggregation (e.g. avg delay per airport) | scan spread over all nodes |

The data is high-cardinality (hundreds of airports x many years x 12 months x
millions of flights), so there is a lot of parallelism to exploit if we choose
the key well.

## 2. Partition key: `(origin, year, month)`

```
PRIMARY KEY ((origin, year, month), day, carrier, flight_num)
            └─────── partition key ───────┘ └── clustering columns ──┘
```

- **Balance (no hot partitions).** A key of only `origin` would create massive,
  skewed partitions for hubs like ATL/ORD and tiny ones for small airports.
  Adding `year` and `month` splits every airport into one partition per month, so
  each partition is bounded (one airport-month, tens of thousands of rows) and
  tokens spread evenly across the ring.
- **Read/write performance.** W1 and W2 hit a **single partition** — the
  coordinator contacts only that token's replicas (the fastest read). Clustering
  by `(day, carrier, flight_num)` keeps rows sorted on disk, so W2's range scan is
  sequential.
- **Parallel calculations.** Because partitions are spread evenly by token, W3 is
  executed by every node in parallel on the data it owns (token-range scans),
  instead of overloading one node.

Rejected keys: `origin` only (skew / hot partitions); `flight_num` only (spreads
well but W1/W2 do not know it up front); `(year, month)` only (about 12
partitions/year — terrible balance and no parallelism).

## 3. Replication factor: `NetworkTopologyStrategy`, `dc1 = 3` (RF 3)

- **Why RF 3.** Standard balance of durability and cost: data survives the loss
  of any single node and enables **`QUORUM`** (2 of 3) reads/writes that are
  strongly consistent while still tolerating one node down. RF 1 loses data on any
  failure; RF 2 cannot form a fault-tolerant majority.
- **Why NetworkTopologyStrategy.** Rack/DC aware — the only strategy to use beyond
  a toy. It places the 3 replicas on distinct nodes (and, in a real multi-rack/DC
  setup, on distinct failure domains). Here one DC (`dc1`) with RF 3 puts one
  replica on each of the three nodes.
- **Placement.** The partitioner hashes the key to a token; the node owning that
  token range holds the primary replica, and the next two nodes clockwise hold the
  other two. With 3 nodes and RF 3, every node holds a copy of every partition.

## 4. How it maps back to the workflows

| Workflow | Served by | Why it is fast / parallel |
|---|---|---|
| W1 | single-partition read on `(origin, year, month)` | one token, replicas only, no scatter-gather |
| W2 | clustering-range read inside the partition | rows pre-sorted by `(day, carrier, flight_num)` |
| W3 | token-range scan across all nodes | even token spread -> every node works in parallel |
| writes | hash of key picks owner + 2 replicas | writes spread evenly, no single write hot spot |

## 5. Trade-offs

- The key optimizes W1/W2. A query by a different dimension (e.g. all flights of
  one carrier across airports) would need a separate query-table
  (Cassandra denormalization) — not modeled, because no workflow needs it.
- RF 3 triples storage and write amplification vs RF 1; that is the accepted price
  of fault tolerance.
- Very small airports make small partitions (harmless); the month split keeps
  even the busiest airport-months within Cassandra's partition-size guidance.

## Dataset

Airline On-Time Performance Data (Kaggle `bulter22/airline-data`). Columns used
for the model: `Origin`, `Year`, `Month`, `DayofMonth`, `UniqueCarrier`,
`FlightNum`, `DepDelay`, `ArrDelay`, `Dest`, `Distance`.

## Acceptance criteria mapping

| Criterion | Where |
|---|---|
| Strategy with partition keys + RF (balance, fault tolerance, performance) | sections 2, 3 |
| Alignment with workflows and parallel processing justified | sections 1, 2, 4 |
| Diagram of token assignment, distribution, replication | `diagram.svg` |
| Thorough documentation of reasoning | this document + `schema.cql` |
| Optional code snippets for partition keys in modeling/querying | `schema.cql` |
