# Benchmark Strategies Module
#
# 5 strategies for distributed BLAST on AKS:
#   1. query_split    - Split queries, full DB on each node (current ElasticBLAST)
#   2. db_shard       - Shard DB across nodes, broadcast all queries
#   3. hybrid         - Query split × DB shard (maximum parallelism)
#   4. taxonomy       - Taxonomy-based DB subset (reduce search space)
#   5. preloaded      - Pre-loaded persistent nodes with job queue
