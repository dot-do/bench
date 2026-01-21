/**
 * Wikidata Knowledge Graph Dataset
 *
 * Wikidata is a free, collaborative, multilingual knowledge base
 * that contains structured data about entities, properties, and their relationships.
 *
 * Excellent for:
 * - Graph traversal queries
 * - SPARQL-style queries translated to SQL
 * - Entity relationship analysis
 * - Knowledge graph applications
 *
 * Best suited for:
 * - DuckDB (with graph extensions or recursive CTEs)
 * - PostgreSQL (with graph extensions)
 * - ClickHouse (for entity analytics)
 */

import type { DatasetConfig, BenchmarkQuery, DatabaseType } from './index'

/**
 * Wikidata-specific configuration
 */
export interface WikidataConfig extends DatasetConfig {
  /** Available subset configurations */
  subsets: WikidataSubset[]
  /** SPARQL endpoint for reference */
  sparqlEndpoint: string
  /** Entity type statistics */
  entityStats: Record<string, string>
}

/**
 * Wikidata subset configuration
 */
interface WikidataSubset {
  name: string
  description: string
  entities: string
  triples: string
  size: string
  focusAreas: string[]
}

/**
 * Wikidata benchmark queries - SPARQL-style queries translated to SQL
 */
const wikidataQueries: BenchmarkQuery[] = [
  // Simple entity lookups
  {
    id: 'wd-simple-1',
    name: 'Entity lookup by ID',
    description: 'Direct entity lookup (equivalent to SPARQL DESCRIBE)',
    complexity: 'simple',
    sql: `SELECT e.*, p.label as property_label, c.value, c.datatype
FROM entities e
LEFT JOIN claims c ON c.entity_id = e.id
LEFT JOIN properties p ON p.id = c.property_id
WHERE e.id = 'Q42'  -- Douglas Adams`,
    benchmarks: ['point-lookup', 'join-performance'],
    expectedResults: { columns: ['id', 'label', 'description', 'property_label', 'value'] },
  },
  {
    id: 'wd-simple-2',
    name: 'Label search',
    description: 'Find entities by label text',
    complexity: 'simple',
    sql: `SELECT id, label, description, aliases
FROM entities
WHERE label LIKE '%Albert Einstein%'
   OR aliases LIKE '%Albert Einstein%'
LIMIT 20`,
    benchmarks: ['text-search', 'like-performance'],
  },
  {
    id: 'wd-simple-3',
    name: 'Entity type count',
    description: 'Count entities by instance-of property',
    complexity: 'simple',
    sql: `SELECT
  e2.label as type_label,
  COUNT(*) as count
FROM claims c
JOIN entities e2 ON c.value_entity_id = e2.id
WHERE c.property_id = 'P31'  -- instance of
GROUP BY e2.label
ORDER BY count DESC
LIMIT 50`,
    benchmarks: ['aggregation', 'join-groupby'],
  },

  // Moderate graph queries
  {
    id: 'wd-moderate-1',
    name: 'Find all humans',
    description: 'All entities that are instance of human (Q5)',
    complexity: 'moderate',
    sql: `SELECT e.id, e.label, e.description
FROM entities e
JOIN claims c ON c.entity_id = e.id
WHERE c.property_id = 'P31'  -- instance of
  AND c.value_entity_id = 'Q5'  -- human
LIMIT 10000`,
    benchmarks: ['entity-type-filter', 'large-result-set'],
  },
  {
    id: 'wd-moderate-2',
    name: 'Entity properties',
    description: 'Get all properties and values for an entity',
    complexity: 'moderate',
    sql: `SELECT
  p.label as property,
  COALESCE(e2.label, c.value) as value,
  c.datatype
FROM claims c
JOIN properties p ON p.id = c.property_id
LEFT JOIN entities e2 ON c.value_entity_id = e2.id
WHERE c.entity_id = 'Q42'
ORDER BY p.label`,
    benchmarks: ['entity-expansion', 'left-join'],
  },
  {
    id: 'wd-moderate-3',
    name: 'Birth/death date range',
    description: 'Find people born in a specific decade',
    complexity: 'moderate',
    sql: `SELECT
  e.id,
  e.label,
  birth.value as birth_date,
  death.value as death_date
FROM entities e
JOIN claims birth ON birth.entity_id = e.id AND birth.property_id = 'P569'  -- date of birth
LEFT JOIN claims death ON death.entity_id = e.id AND death.property_id = 'P570'  -- date of death
JOIN claims instance ON instance.entity_id = e.id
  AND instance.property_id = 'P31'
  AND instance.value_entity_id = 'Q5'  -- human
WHERE birth.value >= '1900-01-01'
  AND birth.value < '1910-01-01'
ORDER BY birth.value
LIMIT 1000`,
    benchmarks: ['date-range', 'multi-join'],
  },
  {
    id: 'wd-moderate-4',
    name: 'Country statistics',
    description: 'Aggregate statistics about countries',
    complexity: 'moderate',
    sql: `SELECT
  e.label as country,
  pop.value as population,
  area.value as area_km2,
  cap.label as capital
FROM entities e
JOIN claims c_type ON c_type.entity_id = e.id
  AND c_type.property_id = 'P31'
  AND c_type.value_entity_id = 'Q6256'  -- country
LEFT JOIN claims pop ON pop.entity_id = e.id AND pop.property_id = 'P1082'  -- population
LEFT JOIN claims area ON area.entity_id = e.id AND area.property_id = 'P2046'  -- area
LEFT JOIN claims cap_claim ON cap_claim.entity_id = e.id AND cap_claim.property_id = 'P36'  -- capital
LEFT JOIN entities cap ON cap.id = cap_claim.value_entity_id
ORDER BY CAST(pop.value AS BIGINT) DESC NULLS LAST
LIMIT 200`,
    benchmarks: ['multi-property-join', 'nullable-ordering'],
  },

  // Complex graph traversal
  {
    id: 'wd-complex-1',
    name: 'Subclass hierarchy',
    description: 'Find all subclasses of a class (recursive)',
    complexity: 'complex',
    sql: `WITH RECURSIVE subclass_tree AS (
  -- Base case: direct subclasses of "mammal" (Q7377)
  SELECT
    c.entity_id as id,
    e.label,
    1 as depth,
    e.label as path
  FROM claims c
  JOIN entities e ON e.id = c.entity_id
  WHERE c.property_id = 'P279'  -- subclass of
    AND c.value_entity_id = 'Q7377'  -- mammal

  UNION ALL

  -- Recursive case: subclasses of subclasses
  SELECT
    c.entity_id,
    e.label,
    st.depth + 1,
    st.path || ' > ' || e.label
  FROM claims c
  JOIN entities e ON e.id = c.entity_id
  JOIN subclass_tree st ON c.value_entity_id = st.id
  WHERE c.property_id = 'P279'
    AND st.depth < 5  -- limit depth
)
SELECT id, label, depth, path
FROM subclass_tree
ORDER BY depth, label
LIMIT 500`,
    benchmarks: ['recursive-cte', 'graph-traversal', 'hierarchy'],
  },
  {
    id: 'wd-complex-2',
    name: 'Shortest path between entities',
    description: 'Find connection path between two entities',
    complexity: 'complex',
    sql: `WITH RECURSIVE path_search AS (
  -- Start from source entity
  SELECT
    c.entity_id as current_id,
    c.value_entity_id as next_id,
    p.label as relation,
    1 as depth,
    ARRAY[c.entity_id] as visited,
    c.entity_id || ' -[' || p.label || ']-> ' || c.value_entity_id as path
  FROM claims c
  JOIN properties p ON p.id = c.property_id
  WHERE c.entity_id = 'Q42'  -- Douglas Adams
    AND c.value_entity_id IS NOT NULL

  UNION ALL

  SELECT
    ps.next_id,
    c.value_entity_id,
    p.label,
    ps.depth + 1,
    ps.visited || c.entity_id,
    ps.path || ' -[' || p.label || ']-> ' || c.value_entity_id
  FROM path_search ps
  JOIN claims c ON c.entity_id = ps.next_id
  JOIN properties p ON p.id = c.property_id
  WHERE ps.depth < 4
    AND c.value_entity_id IS NOT NULL
    AND c.value_entity_id != ALL(ps.visited)
)
SELECT path, depth
FROM path_search
WHERE next_id = 'Q1339'  -- Bach
ORDER BY depth
LIMIT 10`,
    benchmarks: ['path-finding', 'recursive-cte', 'array-operations'],
  },
  {
    id: 'wd-complex-3',
    name: 'Occupations of Nobel laureates',
    description: 'Analyze occupations of Nobel Prize winners',
    complexity: 'complex',
    sql: `WITH nobel_laureates AS (
  SELECT DISTINCT e.id, e.label
  FROM entities e
  JOIN claims award ON award.entity_id = e.id
    AND award.property_id = 'P166'  -- award received
  JOIN entities award_entity ON award_entity.id = award.value_entity_id
  WHERE award_entity.label LIKE '%Nobel%'
)
SELECT
  occ.label as occupation,
  COUNT(DISTINCT nl.id) as laureate_count,
  STRING_AGG(nl.label, ', ' ORDER BY nl.label) as sample_laureates
FROM nobel_laureates nl
JOIN claims c ON c.entity_id = nl.id
  AND c.property_id = 'P106'  -- occupation
JOIN entities occ ON occ.id = c.value_entity_id
GROUP BY occ.label
HAVING COUNT(*) >= 5
ORDER BY laureate_count DESC
LIMIT 30`,
    benchmarks: ['cte', 'aggregation-with-strings', 'like-filter'],
  },
  {
    id: 'wd-complex-4',
    name: 'Geographic entity clustering',
    description: 'Cluster entities by geographic location',
    complexity: 'complex',
    sql: `SELECT
  FLOOR(CAST(lat.value AS DOUBLE) / 10) * 10 as lat_bucket,
  FLOOR(CAST(lon.value AS DOUBLE) / 10) * 10 as lon_bucket,
  COUNT(*) as entity_count,
  COUNT(DISTINCT type_claim.value_entity_id) as type_diversity,
  STRING_AGG(DISTINCT e.label, ', ' ORDER BY e.label LIMIT 5) as sample_entities
FROM entities e
JOIN claims coord ON coord.entity_id = e.id
  AND coord.property_id = 'P625'  -- coordinate location
JOIN claims lat ON lat.entity_id = e.id
  AND lat.property_id = 'P625_lat'  -- extracted latitude
JOIN claims lon ON lon.entity_id = e.id
  AND lon.property_id = 'P625_lon'  -- extracted longitude
LEFT JOIN claims type_claim ON type_claim.entity_id = e.id
  AND type_claim.property_id = 'P31'  -- instance of
WHERE lat.value IS NOT NULL
  AND lon.value IS NOT NULL
GROUP BY lat_bucket, lon_bucket
HAVING entity_count > 100
ORDER BY entity_count DESC
LIMIT 50`,
    benchmarks: ['geographic', 'bucketing', 'multi-aggregate'],
  },

  // Expert-level graph analytics
  {
    id: 'wd-expert-1',
    name: 'PageRank-style importance',
    description: 'Calculate entity importance via incoming references',
    complexity: 'expert',
    sql: `WITH entity_references AS (
  SELECT
    c.value_entity_id as entity_id,
    COUNT(DISTINCT c.entity_id) as incoming_refs
  FROM claims c
  WHERE c.value_entity_id IS NOT NULL
  GROUP BY c.value_entity_id
),
outgoing_refs AS (
  SELECT
    c.entity_id,
    COUNT(*) as outgoing_count
  FROM claims c
  WHERE c.value_entity_id IS NOT NULL
  GROUP BY c.entity_id
)
SELECT
  e.id,
  e.label,
  e.description,
  COALESCE(er.incoming_refs, 0) as incoming_references,
  COALESCE(o.outgoing_count, 0) as outgoing_references,
  COALESCE(er.incoming_refs, 0) * 1.0 /
    NULLIF(COALESCE(o.outgoing_count, 0), 0) as importance_ratio
FROM entities e
LEFT JOIN entity_references er ON er.entity_id = e.id
LEFT JOIN outgoing_refs o ON o.entity_id = e.id
WHERE COALESCE(er.incoming_refs, 0) > 1000
ORDER BY incoming_references DESC
LIMIT 100`,
    benchmarks: ['graph-centrality', 'pagerank-approx', 'ratio-calculation'],
  },
  {
    id: 'wd-expert-2',
    name: 'Temporal entity evolution',
    description: 'Track how entity properties change over time',
    complexity: 'expert',
    sql: `WITH qualified_claims AS (
  SELECT
    c.entity_id,
    c.property_id,
    c.value,
    q.qualifier_property_id,
    q.qualifier_value,
    CASE
      WHEN q.qualifier_property_id = 'P580' THEN 'start_time'
      WHEN q.qualifier_property_id = 'P582' THEN 'end_time'
      WHEN q.qualifier_property_id = 'P585' THEN 'point_in_time'
    END as time_type
  FROM claims c
  JOIN qualifiers q ON q.claim_id = c.id
  WHERE q.qualifier_property_id IN ('P580', 'P582', 'P585')
)
SELECT
  e.label as entity,
  p.label as property,
  qc.value,
  MIN(CASE WHEN qc.time_type = 'start_time' THEN qc.qualifier_value END) as valid_from,
  MAX(CASE WHEN qc.time_type = 'end_time' THEN qc.qualifier_value END) as valid_until
FROM qualified_claims qc
JOIN entities e ON e.id = qc.entity_id
JOIN properties p ON p.id = qc.property_id
WHERE e.id = 'Q30'  -- United States
GROUP BY e.label, p.label, qc.value
HAVING valid_from IS NOT NULL OR valid_until IS NOT NULL
ORDER BY valid_from`,
    benchmarks: ['temporal-queries', 'qualifier-joins', 'pivot-aggregation'],
  },
  {
    id: 'wd-expert-3',
    name: 'Property co-occurrence analysis',
    description: 'Find properties that commonly appear together',
    complexity: 'expert',
    sql: `WITH entity_properties AS (
  SELECT
    entity_id,
    property_id
  FROM claims
  GROUP BY entity_id, property_id
),
property_pairs AS (
  SELECT
    ep1.property_id as prop1,
    ep2.property_id as prop2,
    COUNT(DISTINCT ep1.entity_id) as co_occurrence_count
  FROM entity_properties ep1
  JOIN entity_properties ep2 ON ep1.entity_id = ep2.entity_id
    AND ep1.property_id < ep2.property_id  -- avoid duplicates and self-pairs
  GROUP BY ep1.property_id, ep2.property_id
  HAVING COUNT(*) > 10000
)
SELECT
  p1.label as property_1,
  p2.label as property_2,
  pp.co_occurrence_count,
  pp.co_occurrence_count * 100.0 / (
    SELECT COUNT(DISTINCT entity_id)
    FROM entity_properties
    WHERE property_id = pp.prop1
  ) as pct_of_prop1
FROM property_pairs pp
JOIN properties p1 ON p1.id = pp.prop1
JOIN properties p2 ON p2.id = pp.prop2
ORDER BY co_occurrence_count DESC
LIMIT 50`,
    benchmarks: ['co-occurrence', 'self-join', 'subquery-in-select'],
  },
  {
    id: 'wd-expert-4',
    name: 'Multi-hop relationship discovery',
    description: 'Find entities connected through specific relationship chains',
    complexity: 'expert',
    sql: `-- Find musicians who studied under teachers who also taught famous composers
WITH teacher_student AS (
  SELECT
    student.entity_id as student_id,
    teacher.value_entity_id as teacher_id
  FROM claims student
  JOIN claims teacher ON teacher.entity_id = student.entity_id
  WHERE student.property_id = 'P31' AND student.value_entity_id = 'Q5'  -- human
    AND teacher.property_id = 'P1066'  -- student of
),
famous_composers AS (
  SELECT DISTINCT e.id
  FROM entities e
  JOIN claims occ ON occ.entity_id = e.id
    AND occ.property_id = 'P106'
    AND occ.value_entity_id = 'Q36834'  -- composer
  JOIN claims ref ON ref.value_entity_id = e.id
  GROUP BY e.id
  HAVING COUNT(DISTINCT ref.entity_id) > 100  -- highly referenced
)
SELECT
  student.label as musician,
  teacher.label as teacher,
  STRING_AGG(DISTINCT fc_entity.label, ', ') as famous_co_students
FROM teacher_student ts
JOIN entities student ON student.id = ts.student_id
JOIN entities teacher ON teacher.id = ts.teacher_id
JOIN teacher_student ts2 ON ts2.teacher_id = ts.teacher_id
  AND ts2.student_id != ts.student_id
JOIN famous_composers fc ON fc.id = ts2.student_id
JOIN entities fc_entity ON fc_entity.id = fc.id
GROUP BY student.label, teacher.label
HAVING COUNT(DISTINCT fc.id) >= 2
ORDER BY COUNT(DISTINCT fc.id) DESC
LIMIT 30`,
    benchmarks: ['multi-hop', 'complex-joins', 'having-with-count'],
  },
]

/**
 * Wikidata dataset configuration
 */
export const wikidata: WikidataConfig = {
  id: 'wikidata',
  name: 'Wikidata Knowledge Graph',
  description: `Wikidata is a free, collaborative knowledge base with structured data about
100M+ entities, their properties, and relationships. Excellent for graph queries,
entity analytics, and knowledge graph applications.`,
  category: 'knowledge-graph',
  size: 'xlarge',
  rowCount: '~100M entities, ~1.5B claims',
  compressedSize: '~100GB (JSON dump)',
  uncompressedSize: '~1TB (relational format)',
  sourceUrl: 'https://dumps.wikimedia.org/wikidatawiki/',
  license: 'CC0 1.0 (Public Domain)',
  sparqlEndpoint: 'https://query.wikidata.org/sparql',
  suitedFor: ['duckdb', 'postgres', 'clickhouse'],

  subsets: [
    {
      name: 'truthy',
      description: 'Only truthy statements (no deprecated/normal rank filtering)',
      entities: '~100M',
      triples: '~700M',
      size: '~30GB compressed',
      focusAreas: ['simplified queries', 'most common use cases'],
    },
    {
      name: 'humans',
      description: 'All human entities with their properties',
      entities: '~10M',
      triples: '~200M',
      size: '~5GB compressed',
      focusAreas: ['biographical data', 'person analytics'],
    },
    {
      name: 'places',
      description: 'Geographic entities (countries, cities, landmarks)',
      entities: '~15M',
      triples: '~150M',
      size: '~4GB compressed',
      focusAreas: ['geographic queries', 'location analytics'],
    },
    {
      name: 'works',
      description: 'Creative works (books, films, music, art)',
      entities: '~20M',
      triples: '~300M',
      size: '~8GB compressed',
      focusAreas: ['media analytics', 'cultural data'],
    },
    {
      name: 'sample',
      description: 'Random 1% sample of all entities',
      entities: '~1M',
      triples: '~15M',
      size: '~500MB compressed',
      focusAreas: ['local testing', 'quick benchmarks'],
    },
  ],

  entityStats: {
    Q5: '~10M humans',
    Q6256: '~200 countries',
    Q515: '~100K cities',
    Q7725634: '~500K literary works',
    Q11424: '~300K films',
    Q7366: '~1M songs',
    Q16521: '~2M taxa (species)',
  },

  downloadConfigs: {
    local: {
      urls: ['https://dumps.wikimedia.org/wikidatawiki/entities/latest-all.json.bz2'],
      size: '~500MB (1% sample)',
      rowCount: '~1M entities',
      instructions: [
        '# Option 1: Download pre-built sample from Academic Torrents',
        '# https://academictorrents.com/details/...',
        '',
        '# Option 2: Stream and sample from full dump',
        'curl https://dumps.wikimedia.org/wikidatawiki/entities/latest-all.json.bz2 | \\',
        '  bzcat | \\',
        '  python3 scripts/sample_wikidata.py --rate 0.01 --out wikidata_sample.json',
        '',
        '# Option 3: Use wikidata-subset-graph tool',
        '# https://github.com/usc-isi-i2/wikidata-subset-graph',
        'pip install wikidata-subset-graph',
        'wdsubset --input latest-all.json.bz2 --output sample.json --sample 0.01',
      ],
      setupCommands: [
        '# Parse JSON to relational tables',
        'python3 scripts/wikidata_to_relational.py \\',
        '  --input wikidata_sample.json \\',
        '  --output-dir wikidata_tables/',
        '',
        '# Load into DuckDB',
        'duckdb wikidata.db < scripts/load_wikidata.sql',
      ],
    },
    development: {
      urls: ['https://dumps.wikimedia.org/wikidatawiki/entities/latest-all.json.bz2'],
      size: '~5GB (humans subset)',
      rowCount: '~10M entities',
      instructions: [
        '# Extract humans subset using SPARQL-like filtering',
        '# This extracts all entities that are instance of human (Q5)',
        'python3 scripts/extract_wikidata_subset.py \\',
        '  --input latest-all.json.bz2 \\',
        '  --filter "P31=Q5" \\',
        '  --output wikidata_humans.json',
      ],
      setupCommands: [
        '# Load humans subset',
        'duckdb wikidata_humans.db < scripts/load_wikidata_humans.sql',
      ],
    },
    production: {
      urls: [
        'https://dumps.wikimedia.org/wikidatawiki/entities/latest-all.json.bz2',
        'https://dumps.wikimedia.org/wikidatawiki/entities/latest-lexemes.json.bz2',
      ],
      size: '~100GB compressed',
      rowCount: '~100M entities, ~1.5B claims',
      instructions: [
        '# Download full Wikidata dump (warning: 100GB+)',
        'curl -O https://dumps.wikimedia.org/wikidatawiki/entities/latest-all.json.bz2',
        '',
        '# For distributed processing, use Spark',
        '# See: https://github.com/wmde/spark-wikidata-toolkit',
        '',
        '# Or use Academic Torrents for faster download',
        '# https://academictorrents.com/collection/wikidata',
      ],
      setupCommands: [
        '# Full load requires distributed processing',
        '# See scripts/spark_wikidata_load.py for PySpark job',
        '',
        '# Or partition and load incrementally',
        'python3 scripts/wikidata_partitioned_load.py \\',
        '  --input latest-all.json.bz2 \\',
        '  --partitions 100 \\',
        '  --database wikidata_full.db',
      ],
    },
  },

  schema: {
    tableName: 'entities',
    columns: [
      { name: 'id', type: 'VARCHAR(20)', nullable: false, description: 'Wikidata entity ID (Q-number)' },
      { name: 'type', type: 'VARCHAR(20)', nullable: false, description: 'Entity type (item, property, lexeme)' },
      { name: 'label', type: 'TEXT', nullable: true, description: 'Primary label (English)' },
      { name: 'description', type: 'TEXT', nullable: true, description: 'Entity description (English)' },
      { name: 'aliases', type: 'TEXT', nullable: true, description: 'Alternative names (JSON array)' },
      { name: 'labels_all', type: 'JSON', nullable: true, description: 'Labels in all languages' },
      { name: 'sitelinks', type: 'JSON', nullable: true, description: 'Wikipedia and other site links' },
      { name: 'modified', type: 'TIMESTAMP', nullable: true, description: 'Last modification time' },
    ],
    primaryKey: ['id'],
    indexes: [
      { name: 'idx_entity_type', columns: ['type'], type: 'btree', description: 'Filter by entity type' },
      { name: 'idx_entity_label', columns: ['label'], type: 'btree', description: 'Label lookup' },
    ],
    createTableSQL: {
      duckdb: `-- Entities table
CREATE TABLE entities (
  id VARCHAR(20) PRIMARY KEY,
  type VARCHAR(20) NOT NULL,
  label TEXT,
  description TEXT,
  aliases TEXT,
  labels_all JSON,
  sitelinks JSON,
  modified TIMESTAMP
);

-- Properties table (subset of entities)
CREATE TABLE properties (
  id VARCHAR(20) PRIMARY KEY,
  label TEXT,
  description TEXT,
  datatype VARCHAR(50)
);

-- Claims table (entity-property-value triples)
CREATE TABLE claims (
  id VARCHAR(50) PRIMARY KEY,
  entity_id VARCHAR(20) NOT NULL,
  property_id VARCHAR(20) NOT NULL,
  value TEXT,
  value_entity_id VARCHAR(20),
  datatype VARCHAR(50),
  rank VARCHAR(20)
);

-- Qualifiers table
CREATE TABLE qualifiers (
  id VARCHAR(50) PRIMARY KEY,
  claim_id VARCHAR(50) NOT NULL,
  qualifier_property_id VARCHAR(20) NOT NULL,
  qualifier_value TEXT
);

-- Indexes
CREATE INDEX idx_claims_entity ON claims(entity_id);
CREATE INDEX idx_claims_property ON claims(property_id);
CREATE INDEX idx_claims_value_entity ON claims(value_entity_id);
CREATE INDEX idx_qualifiers_claim ON qualifiers(claim_id);`,

      postgres: `-- Same structure as DuckDB with additional GIN indexes
CREATE TABLE entities (
  id VARCHAR(20) PRIMARY KEY,
  type VARCHAR(20) NOT NULL,
  label TEXT,
  description TEXT,
  aliases TEXT,
  labels_all JSONB,
  sitelinks JSONB,
  modified TIMESTAMP
);

CREATE TABLE properties (
  id VARCHAR(20) PRIMARY KEY,
  label TEXT,
  description TEXT,
  datatype VARCHAR(50)
);

CREATE TABLE claims (
  id VARCHAR(50) PRIMARY KEY,
  entity_id VARCHAR(20) NOT NULL REFERENCES entities(id),
  property_id VARCHAR(20) NOT NULL REFERENCES properties(id),
  value TEXT,
  value_entity_id VARCHAR(20) REFERENCES entities(id),
  datatype VARCHAR(50),
  rank VARCHAR(20)
);

CREATE TABLE qualifiers (
  id VARCHAR(50) PRIMARY KEY,
  claim_id VARCHAR(50) NOT NULL REFERENCES claims(id),
  qualifier_property_id VARCHAR(20) NOT NULL,
  qualifier_value TEXT
);

-- Indexes
CREATE INDEX idx_claims_entity ON claims(entity_id);
CREATE INDEX idx_claims_property ON claims(property_id);
CREATE INDEX idx_claims_value_entity ON claims(value_entity_id);
CREATE INDEX idx_qualifiers_claim ON qualifiers(claim_id);
CREATE INDEX idx_entities_label_trgm ON entities USING GIN(label gin_trgm_ops);`,

      clickhouse: `-- Optimized for analytics
CREATE TABLE entities (
  id String,
  type LowCardinality(String),
  label Nullable(String),
  description Nullable(String),
  aliases Nullable(String),
  modified Nullable(DateTime)
) ENGINE = MergeTree()
ORDER BY id;

CREATE TABLE claims (
  entity_id String,
  property_id LowCardinality(String),
  value Nullable(String),
  value_entity_id Nullable(String),
  datatype LowCardinality(Nullable(String)),
  rank LowCardinality(String)
) ENGINE = MergeTree()
ORDER BY (property_id, entity_id);

-- Materialized view for common patterns
CREATE MATERIALIZED VIEW entity_type_counts
ENGINE = SummingMergeTree()
ORDER BY type_id
AS SELECT
  value_entity_id as type_id,
  count() as count
FROM claims
WHERE property_id = 'P31'
GROUP BY type_id;`,

      db4: `-- Simplified schema for embedded use
CREATE TABLE entities (
  id TEXT PRIMARY KEY,
  type TEXT NOT NULL,
  label TEXT,
  description TEXT,
  aliases TEXT,
  modified TEXT
);

CREATE TABLE claims (
  id TEXT PRIMARY KEY,
  entity_id TEXT NOT NULL,
  property_id TEXT NOT NULL,
  value TEXT,
  value_entity_id TEXT,
  datatype TEXT,
  rank TEXT
);

CREATE INDEX idx_claims_entity ON claims(entity_id);
CREATE INDEX idx_claims_property ON claims(property_id);
CREATE INDEX idx_claims_value_entity ON claims(value_entity_id);`,

      evodb: `-- Same as db4`,

      sqlite: `-- Same as db4`,
    },
  },

  queries: wikidataQueries,

  performanceExpectations: {
    duckdb: {
      loadTime: '~1 hour for 10M entities',
      simpleQueryLatency: '10-100ms',
      complexQueryLatency: '1-30s (recursive CTEs)',
      storageEfficiency: 'Good (columnar)',
      concurrency: 'Moderate',
      notes: [
        'Excellent for analytical queries',
        'Recursive CTEs for graph traversal',
        'Consider graph extension for complex paths',
        'Parquet files enable partition pruning',
      ],
    },
    postgres: {
      loadTime: '~2 hours for 10M entities',
      simpleQueryLatency: '5-50ms',
      complexQueryLatency: '1-60s',
      storageEfficiency: 'Moderate',
      concurrency: 'Excellent',
      notes: [
        'Best for production OLTP+OLAP mix',
        'pg_trgm for fuzzy label search',
        'Consider Apache AGE for graph queries',
        'Partitioning recommended for full dataset',
      ],
    },
    clickhouse: {
      loadTime: '~30 minutes for 10M entities',
      simpleQueryLatency: '5-50ms',
      complexQueryLatency: '100ms-10s',
      storageEfficiency: 'Excellent',
      concurrency: 'Excellent',
      notes: [
        'Best for aggregation queries',
        'No recursive CTEs (use JOINs)',
        'Materialized views for common patterns',
        'Distributed queries for full dataset',
      ],
    },
    db4: {
      loadTime: '~3 hours for 10M entities',
      simpleQueryLatency: '10-100ms',
      complexQueryLatency: '5-120s',
      storageEfficiency: 'Moderate',
      concurrency: 'Limited',
      notes: [
        'Use subset for edge deployment',
        'Recursive CTEs supported',
        'Memory-constrained environments',
      ],
    },
    evodb: {
      loadTime: '~3 hours',
      simpleQueryLatency: '10-100ms',
      complexQueryLatency: '5-120s',
      storageEfficiency: 'Moderate',
      concurrency: 'Limited',
      notes: ['Same as db4', 'Event sourcing for change tracking'],
    },
    sqlite: {
      loadTime: '~4 hours for 10M entities',
      simpleQueryLatency: '10-200ms',
      complexQueryLatency: '10-300s',
      storageEfficiency: 'Poor (large indexes)',
      concurrency: 'Very limited',
      notes: [
        'Use subset only',
        'Recursive CTEs available',
        'Not recommended for full dataset',
      ],
    },
  },

  r2Config: {
    bucketName: 'bench-datasets',
    pathPrefix: 'wikidata/',
    format: 'parquet',
    compression: 'zstd',
    partitioning: {
      columns: ['entity_type', 'property_id'],
      format: 'type={entity_type}/entities.parquet and claims/property={property_id}/*.parquet',
    },
    uploadInstructions: [
      '# Partition entities by type',
      'duckdb -c "',
      "  COPY (SELECT * FROM entities)",
      "  TO 'entities_parquet' (FORMAT PARQUET, PARTITION_BY (type), COMPRESSION 'zstd');",
      '"',
      '',
      '# Partition claims by property (enables efficient predicate pushdown)',
      'duckdb -c "',
      "  COPY (SELECT * FROM claims)",
      "  TO 'claims_parquet' (FORMAT PARQUET, PARTITION_BY (property_id), COMPRESSION 'zstd');",
      '"',
      '',
      '# Upload to R2',
      'wrangler r2 object put bench-datasets/wikidata/entities/ --file=entities_parquet/ --recursive',
      'wrangler r2 object put bench-datasets/wikidata/claims/ --file=claims_parquet/ --recursive',
    ],
    duckdbInstructions: [
      '-- Query entities from R2',
      "SELECT * FROM read_parquet('s3://bench-datasets/wikidata/entities/type=item/*.parquet')",
      "WHERE label LIKE 'Albert%';",
      '',
      '-- Query claims for specific property (partition pruning)',
      "SELECT * FROM read_parquet('s3://bench-datasets/wikidata/claims/property_id=P31/*.parquet')",
      "WHERE value_entity_id = 'Q5'  -- humans",
      'LIMIT 1000;',
      '',
      '-- Join entities and claims across partitions',
      'SELECT e.label, c.value',
      "FROM read_parquet('s3://bench-datasets/wikidata/entities/**/*.parquet') e",
      "JOIN read_parquet('s3://bench-datasets/wikidata/claims/property_id=P569/*.parquet') c",
      '  ON c.entity_id = e.id',
      'LIMIT 100;',
    ],
  },
}

export default wikidata
