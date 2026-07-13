# Task 01 — Design a Data Distribution Strategy (Apache Cassandra)

Design a data distribution strategy for a big data system that stores airline
on-time performance data in Apache Cassandra: choose the **partition key** and
**replication factor** that keep the cluster balanced, fault tolerant, and fast.

This is a design task — all four acceptance criteria are documentation/diagram
deliverables and the running cluster is explicitly optional — so the solution is
just three artifacts, no infrastructure to stand up.

## Files (this is the whole task)

| File | Task outcome it delivers |
|---|---|
| `STRATEGY.md` | **Documentation** — the strategy: workloads, partition key, replication factor, justification, trade-offs, acceptance-criteria mapping. |
| `diagram.svg` | **Diagram** — real image of the Cassandra architecture: token ring, key-to-token-to-node placement, RF=3 replication. Open it in any browser. |
| `schema.cql` | **Code snippets** — CQL showing the RF (keyspace), the partition key (table), and the queries that use it. |

## How to view

- `diagram.svg` — open in a web browser (or GitHub renders it inline). It is a
  standalone SVG image, not text.
- `STRATEGY.md` — read top to bottom.
- `schema.cql` — demonstrative CQL; optional to run (`cqlsh -f schema.cql`).

## Strategy in one line

Partition key `(origin, year, month)` spreads a skewed dataset evenly (one
partition per airport-month) and makes fleet-wide aggregations run in parallel;
replication `NetworkTopologyStrategy dc1:3` (RF 3) gives fault tolerance and
strongly-consistent `QUORUM`.

## Dataset

Airline On-Time Performance Data — Kaggle `bulter22/airline-data`.
