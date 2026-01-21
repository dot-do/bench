/**
 * Common Crawl Host Graph Dataset
 *
 * The Common Crawl web graph represents the link structure of the web,
 * extracted from Common Crawl's petabyte-scale web archives.
 *
 * Excellent for:
 * - PageRank and centrality algorithms
 * - Graph analytics benchmarks
 * - Web structure analysis
 * - Link-based ranking experiments
 *
 * Best suited for:
 * - DuckDB (for analytical graph queries)
 * - ClickHouse (for large-scale aggregations)
 * - Specialized graph databases (Neo4j, DGraph)
 */

import type { DatasetConfig, BenchmarkQuery } from './index'

/**
 * Common Crawl Host Graph specific configuration
 */
export interface CommonCrawlHostGraphConfig extends DatasetConfig {
  /** Available crawl snapshots */
  crawlSnapshots: CrawlSnapshot[]
  /** Graph statistics */
  graphStats: GraphStatistics
  /** Alternative formats available */
  formats: string[]
}

/**
 * Crawl snapshot information
 */
interface CrawlSnapshot {
  id: string
  date: string
  hosts: string
  edges: string
  size: string
  url: string
}

/**
 * Graph statistics
 */
interface GraphStatistics {
  totalHosts: string
  totalEdges: string
  avgOutDegree: string
  avgInDegree: string
  maxOutDegree: string
  maxInDegree: string
  stronglyConnectedComponents: string
}

/**
 * Common Crawl Host Graph benchmark queries
 */
const hostGraphQueries: BenchmarkQuery[] = [
  // Simple graph queries
  {
    id: 'graph-simple-1',
    name: 'Node degree distribution',
    description: 'Calculate in-degree and out-degree for hosts',
    complexity: 'simple',
    sql: `SELECT
  h.host,
  COUNT(DISTINCT e_out.target_host_id) as out_degree,
  COUNT(DISTINCT e_in.source_host_id) as in_degree
FROM hosts h
LEFT JOIN edges e_out ON e_out.source_host_id = h.id
LEFT JOIN edges e_in ON e_in.target_host_id = h.id
GROUP BY h.host
ORDER BY in_degree DESC
LIMIT 100`,
    benchmarks: ['degree-calculation', 'aggregation'],
    expectedResults: { rowCount: 100 },
  },
  {
    id: 'graph-simple-2',
    name: 'Top-level domain distribution',
    description: 'Count hosts by TLD',
    complexity: 'simple',
    sql: `SELECT
  SPLIT_PART(REVERSE(SPLIT_PART(REVERSE(host), '.', 1)), '.', 1) as tld,
  COUNT(*) as host_count
FROM hosts
GROUP BY tld
ORDER BY host_count DESC
LIMIT 50`,
    benchmarks: ['string-manipulation', 'groupby'],
  },
  {
    id: 'graph-simple-3',
    name: 'Edge weight distribution',
    description: 'Distribution of link counts between hosts',
    complexity: 'simple',
    sql: `SELECT
  CASE
    WHEN link_count = 1 THEN '1'
    WHEN link_count BETWEEN 2 AND 5 THEN '2-5'
    WHEN link_count BETWEEN 6 AND 10 THEN '6-10'
    WHEN link_count BETWEEN 11 AND 50 THEN '11-50'
    WHEN link_count BETWEEN 51 AND 100 THEN '51-100'
    ELSE '100+'
  END as link_bucket,
  COUNT(*) as edge_count
FROM edges
GROUP BY link_bucket
ORDER BY MIN(link_count)`,
    benchmarks: ['bucketing', 'case-aggregation'],
  },

  // Moderate complexity queries
  {
    id: 'graph-moderate-1',
    name: 'Top linked-to hosts',
    description: 'Hosts with most incoming links (simple PageRank proxy)',
    complexity: 'moderate',
    sql: `SELECT
  h.host,
  SUM(e.link_count) as total_incoming_links,
  COUNT(DISTINCT e.source_host_id) as unique_referrers
FROM hosts h
JOIN edges e ON e.target_host_id = h.id
GROUP BY h.host
ORDER BY total_incoming_links DESC
LIMIT 100`,
    benchmarks: ['join-aggregation', 'top-k'],
  },
  {
    id: 'graph-moderate-2',
    name: 'Domain authority estimation',
    description: 'Estimate domain authority based on incoming links from unique domains',
    complexity: 'moderate',
    sql: `WITH host_domains AS (
  SELECT
    id,
    host,
    -- Extract domain (last two parts of hostname)
    REVERSE(SPLIT_PART(REVERSE(host), '.', 1)) || '.' ||
    REVERSE(SPLIT_PART(REVERSE(host), '.', 2)) as domain
  FROM hosts
)
SELECT
  target.domain,
  COUNT(DISTINCT source.domain) as linking_domains,
  SUM(e.link_count) as total_links,
  LOG10(COUNT(DISTINCT source.domain) + 1) * 10 as estimated_authority
FROM edges e
JOIN host_domains source ON source.id = e.source_host_id
JOIN host_domains target ON target.id = e.target_host_id
WHERE source.domain != target.domain  -- External links only
GROUP BY target.domain
HAVING COUNT(DISTINCT source.domain) > 100
ORDER BY linking_domains DESC
LIMIT 100`,
    benchmarks: ['cte', 'domain-extraction', 'multi-aggregate'],
  },
  {
    id: 'graph-moderate-3',
    name: 'Reciprocal links',
    description: 'Find hosts that link to each other',
    complexity: 'moderate',
    sql: `SELECT
  h1.host as host_a,
  h2.host as host_b,
  e1.link_count as a_to_b_links,
  e2.link_count as b_to_a_links
FROM edges e1
JOIN edges e2 ON e1.source_host_id = e2.target_host_id
  AND e1.target_host_id = e2.source_host_id
JOIN hosts h1 ON h1.id = e1.source_host_id
JOIN hosts h2 ON h2.id = e1.target_host_id
WHERE e1.source_host_id < e1.target_host_id  -- Avoid duplicates
ORDER BY (e1.link_count + e2.link_count) DESC
LIMIT 100`,
    benchmarks: ['self-join', 'reciprocal-detection'],
  },
  {
    id: 'graph-moderate-4',
    name: 'Link hub analysis',
    description: 'Find hosts that link to many unique destinations',
    complexity: 'moderate',
    sql: `SELECT
  h.host,
  COUNT(DISTINCT e.target_host_id) as unique_outlinks,
  SUM(e.link_count) as total_outlinks,
  AVG(e.link_count) as avg_links_per_target
FROM hosts h
JOIN edges e ON e.source_host_id = h.id
GROUP BY h.host
HAVING unique_outlinks > 1000
ORDER BY unique_outlinks DESC
LIMIT 100`,
    benchmarks: ['hub-detection', 'having-clause'],
  },

  // Complex graph analytics
  {
    id: 'graph-complex-1',
    name: 'Two-hop neighborhood',
    description: 'Find all hosts within 2 hops of a seed host',
    complexity: 'complex',
    sql: `WITH seed AS (
  SELECT id FROM hosts WHERE host = 'wikipedia.org'
),
one_hop AS (
  SELECT DISTINCT
    e.target_host_id as host_id,
    1 as distance
  FROM seed s
  JOIN edges e ON e.source_host_id = s.id

  UNION

  SELECT DISTINCT
    e.source_host_id as host_id,
    1 as distance
  FROM seed s
  JOIN edges e ON e.target_host_id = s.id
),
two_hop AS (
  SELECT DISTINCT
    e.target_host_id as host_id,
    2 as distance
  FROM one_hop oh
  JOIN edges e ON e.source_host_id = oh.host_id
  WHERE e.target_host_id NOT IN (SELECT host_id FROM one_hop)
    AND e.target_host_id NOT IN (SELECT id FROM seed)

  UNION

  SELECT DISTINCT
    e.source_host_id as host_id,
    2 as distance
  FROM one_hop oh
  JOIN edges e ON e.target_host_id = oh.host_id
  WHERE e.source_host_id NOT IN (SELECT host_id FROM one_hop)
    AND e.source_host_id NOT IN (SELECT id FROM seed)
)
SELECT
  h.host,
  COALESCE(oh.distance, th.distance) as distance
FROM hosts h
LEFT JOIN one_hop oh ON oh.host_id = h.id
LEFT JOIN two_hop th ON th.host_id = h.id
WHERE oh.host_id IS NOT NULL OR th.host_id IS NOT NULL
ORDER BY distance, h.host
LIMIT 1000`,
    benchmarks: ['multi-hop-traversal', 'union-cte', 'neighborhood'],
  },
  {
    id: 'graph-complex-2',
    name: 'PageRank iteration (single step)',
    description: 'One iteration of simplified PageRank algorithm',
    complexity: 'complex',
    sql: `WITH damping AS (SELECT 0.85 as d),
host_out_degrees AS (
  SELECT
    source_host_id,
    SUM(link_count) as out_degree
  FROM edges
  GROUP BY source_host_id
),
initial_rank AS (
  SELECT
    id as host_id,
    1.0 / (SELECT COUNT(*) FROM hosts) as rank
  FROM hosts
),
rank_contribution AS (
  SELECT
    e.target_host_id,
    SUM(ir.rank / hod.out_degree * e.link_count) as contributed_rank
  FROM edges e
  JOIN initial_rank ir ON ir.host_id = e.source_host_id
  JOIN host_out_degrees hod ON hod.source_host_id = e.source_host_id
  GROUP BY e.target_host_id
)
SELECT
  h.host,
  (1 - d.d) / (SELECT COUNT(*) FROM hosts) +
  d.d * COALESCE(rc.contributed_rank, 0) as new_rank
FROM hosts h
CROSS JOIN damping d
LEFT JOIN rank_contribution rc ON rc.target_host_id = h.id
ORDER BY new_rank DESC
LIMIT 100`,
    benchmarks: ['pagerank-single-iteration', 'complex-aggregation'],
  },
  {
    id: 'graph-complex-3',
    name: 'Community detection (connected components proxy)',
    description: 'Find tightly connected host clusters',
    complexity: 'complex',
    sql: `WITH bidirectional_edges AS (
  -- Only consider strongly connected pairs
  SELECT
    LEAST(e1.source_host_id, e1.target_host_id) as node_a,
    GREATEST(e1.source_host_id, e1.target_host_id) as node_b,
    e1.link_count + e2.link_count as total_links
  FROM edges e1
  JOIN edges e2 ON e1.source_host_id = e2.target_host_id
    AND e1.target_host_id = e2.source_host_id
  WHERE e1.source_host_id < e1.target_host_id
),
high_connectivity AS (
  SELECT *
  FROM bidirectional_edges
  WHERE total_links > 100
),
node_clusters AS (
  SELECT
    node_a as node,
    ARRAY_AGG(DISTINCT node_b) as connected_to
  FROM high_connectivity
  GROUP BY node_a
)
SELECT
  h.host,
  ARRAY_LENGTH(nc.connected_to) as cluster_size,
  nc.connected_to[1:5] as sample_connections
FROM node_clusters nc
JOIN hosts h ON h.id = nc.node
WHERE ARRAY_LENGTH(nc.connected_to) >= 5
ORDER BY cluster_size DESC
LIMIT 50`,
    benchmarks: ['community-detection', 'array-aggregation'],
  },
  {
    id: 'graph-complex-4',
    name: 'Cross-TLD link analysis',
    description: 'Analyze linking patterns between top-level domains',
    complexity: 'complex',
    sql: `WITH host_tlds AS (
  SELECT
    id,
    SPLIT_PART(REVERSE(SPLIT_PART(REVERSE(host), '.', 1)), '.', 1) as tld
  FROM hosts
),
tld_links AS (
  SELECT
    src.tld as source_tld,
    tgt.tld as target_tld,
    SUM(e.link_count) as total_links,
    COUNT(*) as edge_count
  FROM edges e
  JOIN host_tlds src ON src.id = e.source_host_id
  JOIN host_tlds tgt ON tgt.id = e.target_host_id
  GROUP BY src.tld, tgt.tld
)
SELECT
  source_tld,
  target_tld,
  total_links,
  edge_count,
  total_links * 100.0 / SUM(total_links) OVER (PARTITION BY source_tld) as pct_of_source_outlinks,
  total_links * 100.0 / SUM(total_links) OVER (PARTITION BY target_tld) as pct_of_target_inlinks
FROM tld_links
WHERE source_tld IN ('com', 'org', 'net', 'edu', 'gov', 'io', 'co')
  AND target_tld IN ('com', 'org', 'net', 'edu', 'gov', 'io', 'co')
  AND total_links > 10000
ORDER BY total_links DESC
LIMIT 100`,
    benchmarks: ['cross-partition-analysis', 'window-functions'],
  },

  // Expert-level graph algorithms
  {
    id: 'graph-expert-1',
    name: 'HITS algorithm (single iteration)',
    description: 'Hyperlink-Induced Topic Search - compute hub and authority scores',
    complexity: 'expert',
    sql: `WITH initial_scores AS (
  SELECT
    id as host_id,
    1.0 as hub_score,
    1.0 as authority_score
  FROM hosts
),
-- Authority update: sum of hub scores of pages that link to it
authority_update AS (
  SELECT
    e.target_host_id as host_id,
    SUM(i.hub_score) as new_authority
  FROM edges e
  JOIN initial_scores i ON i.host_id = e.source_host_id
  GROUP BY e.target_host_id
),
-- Hub update: sum of authority scores of pages it links to
hub_update AS (
  SELECT
    e.source_host_id as host_id,
    SUM(COALESCE(au.new_authority, 0)) as new_hub
  FROM edges e
  LEFT JOIN authority_update au ON au.host_id = e.target_host_id
  GROUP BY e.source_host_id
),
-- Normalize
normalized AS (
  SELECT
    h.id as host_id,
    COALESCE(au.new_authority, 0) /
      SQRT(SUM(POWER(COALESCE(au.new_authority, 0), 2)) OVER ()) as authority,
    COALESCE(hu.new_hub, 0) /
      SQRT(SUM(POWER(COALESCE(hu.new_hub, 0), 2)) OVER ()) as hub
  FROM hosts h
  LEFT JOIN authority_update au ON au.host_id = h.id
  LEFT JOIN hub_update hu ON hu.host_id = h.id
)
SELECT
  h.host,
  n.authority,
  n.hub,
  n.authority + n.hub as combined_score
FROM normalized n
JOIN hosts h ON h.id = n.host_id
WHERE n.authority > 0 OR n.hub > 0
ORDER BY combined_score DESC
LIMIT 100`,
    benchmarks: ['hits-algorithm', 'iterative-graph', 'normalization'],
  },
  {
    id: 'graph-expert-2',
    name: 'Betweenness centrality approximation',
    description: 'Approximate betweenness centrality using sampled shortest paths',
    complexity: 'expert',
    sql: `-- Approximate betweenness via sampling (not full algorithm)
WITH sample_sources AS (
  -- Sample 100 random source nodes
  SELECT id FROM hosts ORDER BY RANDOM() LIMIT 100
),
direct_paths AS (
  -- Direct connections from sample sources
  SELECT
    s.id as source_id,
    e.target_host_id as target_id,
    NULL::BIGINT as intermediate_id,
    1 as path_length
  FROM sample_sources s
  JOIN edges e ON e.source_host_id = s.id
),
two_hop_paths AS (
  -- Two-hop paths through intermediate nodes
  SELECT
    dp.source_id,
    e2.target_host_id as target_id,
    e2.source_host_id as intermediate_id,
    2 as path_length
  FROM direct_paths dp
  JOIN edges e2 ON e2.source_host_id = dp.target_id
  WHERE e2.target_host_id != dp.source_id
    AND NOT EXISTS (
      SELECT 1 FROM direct_paths dp2
      WHERE dp2.source_id = dp.source_id
        AND dp2.target_id = e2.target_host_id
    )
),
intermediate_counts AS (
  SELECT
    intermediate_id,
    COUNT(*) as times_on_shortest_path
  FROM two_hop_paths
  WHERE intermediate_id IS NOT NULL
  GROUP BY intermediate_id
)
SELECT
  h.host,
  ic.times_on_shortest_path,
  ic.times_on_shortest_path * 1.0 / (SELECT COUNT(*) FROM two_hop_paths) as approx_betweenness
FROM intermediate_counts ic
JOIN hosts h ON h.id = ic.intermediate_id
ORDER BY times_on_shortest_path DESC
LIMIT 50`,
    benchmarks: ['betweenness-centrality', 'path-sampling', 'complex-cte'],
  },
  {
    id: 'graph-expert-3',
    name: 'Link prediction features',
    description: 'Compute features for link prediction ML models',
    complexity: 'expert',
    sql: `WITH node_features AS (
  SELECT
    h.id,
    h.host,
    COUNT(DISTINCT e_out.target_host_id) as out_degree,
    COUNT(DISTINCT e_in.source_host_id) as in_degree,
    COALESCE(SUM(e_out.link_count), 0) as total_out_links,
    COALESCE(SUM(e_in.link_count), 0) as total_in_links
  FROM hosts h
  LEFT JOIN edges e_out ON e_out.source_host_id = h.id
  LEFT JOIN edges e_in ON e_in.target_host_id = h.id
  GROUP BY h.id, h.host
),
-- Sample non-edges (potential links to predict)
sample_pairs AS (
  SELECT
    nf1.id as source_id,
    nf2.id as target_id
  FROM node_features nf1
  CROSS JOIN node_features nf2
  WHERE nf1.id != nf2.id
    AND nf1.out_degree > 10
    AND nf2.in_degree > 10
    AND NOT EXISTS (
      SELECT 1 FROM edges e
      WHERE e.source_host_id = nf1.id AND e.target_host_id = nf2.id
    )
  ORDER BY RANDOM()
  LIMIT 10000
),
-- Common neighbors (Jaccard similarity)
common_neighbors AS (
  SELECT
    sp.source_id,
    sp.target_id,
    COUNT(DISTINCT cn.id) as common_neighbor_count
  FROM sample_pairs sp
  JOIN edges e1 ON e1.source_host_id = sp.source_id
  JOIN edges e2 ON e2.source_host_id = sp.target_id
  JOIN hosts cn ON cn.id = e1.target_host_id AND cn.id = e2.target_host_id
  GROUP BY sp.source_id, sp.target_id
)
SELECT
  h1.host as source_host,
  h2.host as target_host,
  nf1.out_degree as source_out_degree,
  nf2.in_degree as target_in_degree,
  COALESCE(cn.common_neighbor_count, 0) as common_neighbors,
  -- Jaccard coefficient
  COALESCE(cn.common_neighbor_count, 0) * 1.0 /
    (nf1.out_degree + nf2.in_degree - COALESCE(cn.common_neighbor_count, 0)) as jaccard_coef,
  -- Adamic-Adar approximation (simplified)
  LOG10(nf1.out_degree + 1) + LOG10(nf2.in_degree + 1) as adamic_adar_approx
FROM sample_pairs sp
JOIN hosts h1 ON h1.id = sp.source_id
JOIN hosts h2 ON h2.id = sp.target_id
JOIN node_features nf1 ON nf1.id = sp.source_id
JOIN node_features nf2 ON nf2.id = sp.target_id
LEFT JOIN common_neighbors cn ON cn.source_id = sp.source_id AND cn.target_id = sp.target_id
ORDER BY jaccard_coef DESC
LIMIT 100`,
    benchmarks: ['link-prediction', 'ml-features', 'jaccard-similarity'],
  },
  {
    id: 'graph-expert-4',
    name: 'Temporal link evolution',
    description: 'Compare link patterns across crawl snapshots',
    complexity: 'expert',
    sql: `-- Requires multiple crawl snapshots loaded
WITH crawl_comparison AS (
  SELECT
    h.host,
    e1.target_host_id,
    e1.link_count as links_jan,
    e2.link_count as links_jul,
    COALESCE(e2.link_count, 0) - COALESCE(e1.link_count, 0) as link_change
  FROM hosts h
  LEFT JOIN edges_2024_01 e1 ON e1.source_host_id = h.id
  LEFT JOIN edges_2024_07 e2 ON e2.source_host_id = h.id
    AND e2.target_host_id = COALESCE(e1.target_host_id, e2.target_host_id)
  WHERE e1.target_host_id IS NOT NULL OR e2.target_host_id IS NOT NULL
),
host_changes AS (
  SELECT
    host,
    SUM(CASE WHEN links_jan IS NULL THEN 1 ELSE 0 END) as new_links,
    SUM(CASE WHEN links_jul IS NULL THEN 1 ELSE 0 END) as lost_links,
    SUM(link_change) as net_link_change,
    AVG(ABS(link_change)) as avg_volatility
  FROM crawl_comparison
  GROUP BY host
)
SELECT
  host,
  new_links,
  lost_links,
  net_link_change,
  avg_volatility,
  new_links * 100.0 / NULLIF(new_links + lost_links, 0) as link_acquisition_rate
FROM host_changes
WHERE new_links + lost_links > 100
ORDER BY ABS(net_link_change) DESC
LIMIT 100`,
    benchmarks: ['temporal-analysis', 'snapshot-comparison', 'link-evolution'],
  },
]

/**
 * Common Crawl Host Graph dataset configuration
 */
export const commonCrawlHostGraph: CommonCrawlHostGraphConfig = {
  id: 'commonCrawlHostGraph',
  name: 'Common Crawl Host Graph',
  description: `Web host-level link graph extracted from Common Crawl archives.
Contains billions of links between web hosts, ideal for PageRank,
link analysis, and web graph research.`,
  category: 'graph-analytics',
  size: 'xlarge',
  rowCount: '~100M hosts, ~3B edges',
  compressedSize: '~50GB (per snapshot)',
  uncompressedSize: '~200GB (per snapshot)',
  sourceUrl: 'https://commoncrawl.org/2023/11/host-and-domain-level-web-graphs-nov-dec-2023/',
  license: 'Common Crawl Terms of Use',
  suitedFor: ['duckdb', 'clickhouse'],

  crawlSnapshots: [
    {
      id: 'cc-main-2024-18',
      date: 'April 2024',
      hosts: '~100M',
      edges: '~3B',
      size: '~50GB',
      url: 'https://data.commoncrawl.org/projects/hyperlinkgraph/cc-main-2024-18/host/',
    },
    {
      id: 'cc-main-2023-50',
      date: 'December 2023',
      hosts: '~95M',
      edges: '~2.8B',
      size: '~45GB',
      url: 'https://data.commoncrawl.org/projects/hyperlinkgraph/cc-main-2023-50/host/',
    },
    {
      id: 'cc-main-2023-06',
      date: 'February 2023',
      hosts: '~90M',
      edges: '~2.5B',
      size: '~40GB',
      url: 'https://data.commoncrawl.org/projects/hyperlinkgraph/cc-main-2023-06/host/',
    },
  ],

  graphStats: {
    totalHosts: '~100M',
    totalEdges: '~3B',
    avgOutDegree: '~30',
    avgInDegree: '~30',
    maxOutDegree: '~50M (aggregator sites)',
    maxInDegree: '~100M (google.com, facebook.com)',
    stronglyConnectedComponents: '~50M in largest SCC',
  },

  formats: ['txt.gz (edge list)', 'parquet', 'graphml', 'adjacency list'],

  downloadConfigs: {
    local: {
      urls: [
        'https://data.commoncrawl.org/projects/hyperlinkgraph/cc-main-2024-18/host/cc-main-2024-18-host-ranks.txt.gz',
        'https://data.commoncrawl.org/projects/hyperlinkgraph/cc-main-2024-18/host/cc-main-2024-18-host-vertices.txt.gz',
      ],
      size: '~1GB (1% sample)',
      rowCount: '~1M hosts, ~30M edges',
      instructions: [
        '# Download host vertices (node list with ranks)',
        'curl -O https://data.commoncrawl.org/projects/hyperlinkgraph/cc-main-2024-18/host/cc-main-2024-18-host-vertices.txt.gz',
        '',
        '# Download host ranks',
        'curl -O https://data.commoncrawl.org/projects/hyperlinkgraph/cc-main-2024-18/host/cc-main-2024-18-host-ranks.txt.gz',
        '',
        '# Download subset of edges (first file)',
        'curl -O https://data.commoncrawl.org/projects/hyperlinkgraph/cc-main-2024-18/host/edges/part-00000.txt.gz',
        '',
        '# For local testing, sample the data',
        'zcat cc-main-2024-18-host-vertices.txt.gz | head -n 1000000 > hosts_sample.txt',
        'zcat part-00000.txt.gz | head -n 10000000 > edges_sample.txt',
      ],
      setupCommands: [
        '# Load into DuckDB',
        'duckdb hostgraph.db << EOF',
        "CREATE TABLE hosts AS SELECT",
        "  CAST(SPLIT_PART(line, '\t', 1) AS BIGINT) as id,",
        "  SPLIT_PART(line, '\t', 2) as host",
        "FROM read_csv('hosts_sample.txt', columns={'line': 'VARCHAR'}, header=false);",
        '',
        "CREATE TABLE edges AS SELECT",
        "  CAST(SPLIT_PART(line, '\t', 1) AS BIGINT) as source_host_id,",
        "  CAST(SPLIT_PART(line, '\t', 2) AS BIGINT) as target_host_id,",
        "  CAST(SPLIT_PART(line, '\t', 3) AS INTEGER) as link_count",
        "FROM read_csv('edges_sample.txt', columns={'line': 'VARCHAR'}, header=false);",
        '',
        'CREATE INDEX idx_edges_source ON edges(source_host_id);',
        'CREATE INDEX idx_edges_target ON edges(target_host_id);',
        'EOF',
      ],
    },
    development: {
      urls: [
        'https://data.commoncrawl.org/projects/hyperlinkgraph/cc-main-2024-18/host/cc-main-2024-18-host-vertices.txt.gz',
        'https://data.commoncrawl.org/projects/hyperlinkgraph/cc-main-2024-18/host/edges/',
      ],
      size: '~10GB',
      rowCount: '~10M hosts, ~300M edges',
      instructions: [
        '# Download vertices',
        'curl -O https://data.commoncrawl.org/projects/hyperlinkgraph/cc-main-2024-18/host/cc-main-2024-18-host-vertices.txt.gz',
        '',
        '# Download first 10 edge files',
        'for i in $(seq -w 0 9); do',
        '  curl -O https://data.commoncrawl.org/projects/hyperlinkgraph/cc-main-2024-18/host/edges/part-0000${i}.txt.gz',
        'done',
      ],
      setupCommands: [
        '# Combine and load edge files',
        'zcat part-*.txt.gz > all_edges.txt',
        'duckdb hostgraph_dev.db < scripts/load_hostgraph.sql',
      ],
    },
    production: {
      urls: ['https://data.commoncrawl.org/projects/hyperlinkgraph/cc-main-2024-18/host/'],
      size: '~50GB compressed',
      rowCount: '~100M hosts, ~3B edges',
      instructions: [
        '# Download complete dataset using aws cli (faster)',
        'aws s3 sync --no-sign-request \\',
        '  s3://commoncrawl/projects/hyperlinkgraph/cc-main-2024-18/host/ \\',
        '  ./hostgraph/',
        '',
        '# Or use Common Crawl index for selective download',
        '# https://commoncrawl.org/get-started',
      ],
      setupCommands: [
        '# For production, use distributed processing',
        '# See scripts/spark_load_hostgraph.py',
        '',
        '# Or load incrementally with streaming',
        'python3 scripts/stream_load_hostgraph.py \\',
        '  --input ./hostgraph/ \\',
        '  --database hostgraph_full.db \\',
        '  --batch-size 10000000',
      ],
    },
  },

  schema: {
    tableName: 'hosts',
    columns: [
      { name: 'id', type: 'BIGINT', nullable: false, description: 'Host numeric identifier' },
      { name: 'host', type: 'TEXT', nullable: false, description: 'Hostname (e.g., example.com)' },
      { name: 'harmonic_rank', type: 'DOUBLE', nullable: true, description: 'Pre-computed harmonic centrality rank' },
      { name: 'pagerank', type: 'DOUBLE', nullable: true, description: 'Pre-computed PageRank score' },
    ],
    primaryKey: ['id'],
    indexes: [
      { name: 'idx_host_name', columns: ['host'], type: 'btree', description: 'Host lookup' },
      { name: 'idx_edges_source', columns: ['source_host_id'], type: 'btree', description: 'Outgoing edge lookup' },
      { name: 'idx_edges_target', columns: ['target_host_id'], type: 'btree', description: 'Incoming edge lookup' },
    ],
    createTableSQL: {
      duckdb: `-- Hosts table
CREATE TABLE hosts (
  id BIGINT PRIMARY KEY,
  host TEXT NOT NULL,
  harmonic_rank DOUBLE,
  pagerank DOUBLE
);

-- Edges table
CREATE TABLE edges (
  source_host_id BIGINT NOT NULL,
  target_host_id BIGINT NOT NULL,
  link_count INTEGER NOT NULL DEFAULT 1,
  PRIMARY KEY (source_host_id, target_host_id)
);

-- Indexes
CREATE INDEX idx_host_name ON hosts(host);
CREATE INDEX idx_edges_target ON edges(target_host_id);`,

      clickhouse: `-- Optimized for graph analytics
CREATE TABLE hosts (
  id UInt64,
  host String,
  harmonic_rank Nullable(Float64),
  pagerank Nullable(Float64)
) ENGINE = MergeTree()
ORDER BY id;

CREATE TABLE edges (
  source_host_id UInt64,
  target_host_id UInt64,
  link_count UInt32
) ENGINE = MergeTree()
ORDER BY (source_host_id, target_host_id);

-- Materialized view for degree distribution
CREATE MATERIALIZED VIEW host_degrees
ENGINE = SummingMergeTree()
ORDER BY host_id
AS SELECT
  source_host_id as host_id,
  count() as out_degree,
  0 as in_degree
FROM edges
GROUP BY source_host_id
UNION ALL
SELECT
  target_host_id as host_id,
  0 as out_degree,
  count() as in_degree
FROM edges
GROUP BY target_host_id;`,

      postgres: `-- Standard relational schema
CREATE TABLE hosts (
  id BIGINT PRIMARY KEY,
  host TEXT NOT NULL,
  harmonic_rank DOUBLE PRECISION,
  pagerank DOUBLE PRECISION
);

CREATE TABLE edges (
  source_host_id BIGINT NOT NULL REFERENCES hosts(id),
  target_host_id BIGINT NOT NULL REFERENCES hosts(id),
  link_count INTEGER NOT NULL DEFAULT 1,
  PRIMARY KEY (source_host_id, target_host_id)
);

CREATE INDEX idx_host_name ON hosts(host);
CREATE INDEX idx_edges_target ON edges(target_host_id);
CREATE INDEX idx_host_name_trgm ON hosts USING GIN(host gin_trgm_ops);`,

      db4: `-- Simplified for edge deployment
CREATE TABLE hosts (
  id INTEGER PRIMARY KEY,
  host TEXT NOT NULL,
  harmonic_rank REAL,
  pagerank REAL
);

CREATE TABLE edges (
  source_host_id INTEGER NOT NULL,
  target_host_id INTEGER NOT NULL,
  link_count INTEGER DEFAULT 1,
  PRIMARY KEY (source_host_id, target_host_id)
);

CREATE INDEX idx_host_name ON hosts(host);
CREATE INDEX idx_edges_target ON edges(target_host_id);`,

      evodb: `-- Same as db4`,

      sqlite: `-- Same as db4`,
    },
  },

  queries: hostGraphQueries,

  performanceExpectations: {
    duckdb: {
      loadTime: '~30 minutes for 3B edges',
      simpleQueryLatency: '100ms-1s',
      complexQueryLatency: '5-60s',
      storageEfficiency: 'Excellent (columnar)',
      concurrency: 'Moderate',
      notes: [
        'Best for analytical graph queries',
        'Recursive CTEs for traversal',
        'Excellent join performance',
        'Consider partitioning by source_host_id',
      ],
    },
    clickhouse: {
      loadTime: '~15 minutes for 3B edges',
      simpleQueryLatency: '50ms-500ms',
      complexQueryLatency: '1-30s',
      storageEfficiency: 'Excellent',
      concurrency: 'Excellent',
      notes: [
        'Best for aggregation queries',
        'Materialized views for degrees',
        'No recursive CTEs',
        'Use JOINs for multi-hop',
      ],
    },
    postgres: {
      loadTime: '~2 hours for 3B edges',
      simpleQueryLatency: '200ms-2s',
      complexQueryLatency: '10-120s',
      storageEfficiency: 'Moderate',
      concurrency: 'Good',
      notes: [
        'Recursive CTEs available',
        'Consider partitioning',
        'pg_trgm for fuzzy host search',
        'May need connection pooling',
      ],
    },
    db4: {
      loadTime: 'Not recommended for full dataset',
      simpleQueryLatency: 'N/A',
      complexQueryLatency: 'N/A',
      storageEfficiency: 'N/A',
      concurrency: 'N/A',
      notes: [
        'Use subset only (< 10M edges)',
        'Edge deployment use cases',
        'Memory constraints significant',
      ],
    },
    evodb: {
      loadTime: 'Not recommended',
      simpleQueryLatency: 'N/A',
      complexQueryLatency: 'N/A',
      storageEfficiency: 'N/A',
      concurrency: 'N/A',
      notes: ['Use subset only', 'Same limitations as db4'],
    },
    sqlite: {
      loadTime: 'Not recommended for full dataset',
      simpleQueryLatency: 'N/A',
      complexQueryLatency: 'N/A',
      storageEfficiency: 'Poor',
      concurrency: 'Very limited',
      notes: ['Use subset only', 'Consider DuckDB instead', 'May run out of memory'],
    },
  },

  r2Config: {
    bucketName: 'bench-datasets',
    pathPrefix: 'common-crawl-hostgraph/',
    format: 'parquet',
    compression: 'zstd',
    partitioning: {
      columns: ['crawl_id', 'source_host_bucket'],
      format: 'crawl={crawl_id}/bucket={source_host_bucket}/edges.parquet',
    },
    uploadInstructions: [
      '# Partition edges by crawl snapshot and source host bucket',
      '# (bucket = source_host_id % 1000 for manageable partition sizes)',
      'duckdb -c "',
      "  COPY (",
      "    SELECT",
      "      'cc-main-2024-18' as crawl_id,",
      "      source_host_id % 1000 as source_host_bucket,",
      "      source_host_id,",
      "      target_host_id,",
      "      link_count",
      "    FROM edges",
      "  ) TO 'edges_partitioned'",
      "  (FORMAT PARQUET, PARTITION_BY (crawl_id, source_host_bucket), COMPRESSION 'zstd');",
      '"',
      '',
      '# Upload hosts (single file, no partitioning needed)',
      "duckdb -c \"COPY hosts TO 'hosts.parquet' (FORMAT PARQUET, COMPRESSION 'zstd')\"",
      '',
      '# Upload to R2',
      'wrangler r2 object put bench-datasets/common-crawl-hostgraph/hosts.parquet --file=hosts.parquet',
      'wrangler r2 object put bench-datasets/common-crawl-hostgraph/edges/ --file=edges_partitioned/ --recursive',
    ],
    duckdbInstructions: [
      '-- Query hosts from R2',
      "SELECT * FROM read_parquet('s3://bench-datasets/common-crawl-hostgraph/hosts.parquet')",
      "WHERE host LIKE '%.edu'",
      'LIMIT 100;',
      '',
      '-- Query edges with partition pruning',
      "SELECT * FROM read_parquet('s3://bench-datasets/common-crawl-hostgraph/edges/crawl=cc-main-2024-18/bucket=42/*.parquet')",
      'LIMIT 1000;',
      '',
      '-- Aggregate across all partitions (may be slow)',
      'SELECT',
      '  target_host_id,',
      '  SUM(link_count) as total_incoming',
      "FROM read_parquet('s3://bench-datasets/common-crawl-hostgraph/edges/**/*.parquet')",
      'GROUP BY target_host_id',
      'ORDER BY total_incoming DESC',
      'LIMIT 100;',
    ],
  },
}

export default commonCrawlHostGraph
