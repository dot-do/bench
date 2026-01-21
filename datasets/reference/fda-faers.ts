/**
 * FDA FAERS Dataset
 *
 * The FDA Adverse Event Reporting System (FAERS) is a database containing
 * adverse event reports, medication error reports, and product quality complaints.
 * It's used for post-market drug safety surveillance.
 *
 * Characteristics:
 * - ~25M adverse event reports (1968-present)
 * - ~100M drug-event relationships
 * - ~50M reaction records
 * - Quarterly data releases
 *
 * Excellent for:
 * - Healthcare analytics and drug safety research
 * - Time-series analysis of adverse events
 * - Text search on drug names and reactions
 * - Statistical signal detection
 * - Temporal pattern analysis
 *
 * Best suited for:
 * - DuckDB (analytical queries, time-series)
 * - PostgreSQL (complex joins, full-text)
 * - ClickHouse (large-scale aggregations)
 */

import type { DatasetConfig, BenchmarkQuery, DatabaseType } from '../analytics'

/**
 * FAERS-specific configuration
 */
export interface FAERSConfig extends DatasetConfig {
  /** Quarterly data files */
  quarterlyFiles: FAERSQuarterlyRelease[]
  /** Data dictionary information */
  dataDictionary: string
  /** Entity statistics */
  entityStats: Record<string, string>
  /** Update frequency */
  updateFrequency: string
}

/**
 * FAERS quarterly release
 */
interface FAERSQuarterlyRelease {
  quarter: string
  releaseDate: string
  url: string
  reportCount: string
}

/**
 * FAERS benchmark queries
 */
const faersQueries: BenchmarkQuery[] = [
  // Point lookups
  {
    id: 'faers-lookup-1',
    name: 'Case lookup by ID',
    description: 'Direct lookup of an adverse event case by primary ID',
    complexity: 'simple',
    sql: `SELECT *
FROM demographics
WHERE primaryid = 123456789`,
    benchmarks: ['point-lookup', 'primary-key'],
    expectedResults: { rowCount: 1, columns: ['primaryid', 'caseid', 'age', 'sex', 'wt'] },
  },
  {
    id: 'faers-lookup-2',
    name: 'Drug lookup by name',
    description: 'Find all cases involving a specific drug',
    complexity: 'simple',
    sql: `SELECT d.primaryid, d.drugname, d.prod_ai, d.role_cod
FROM drugs d
WHERE d.drugname ILIKE '%aspirin%'
LIMIT 100`,
    benchmarks: ['text-search', 'like-filter'],
  },
  {
    id: 'faers-lookup-3',
    name: 'Reaction lookup by term',
    description: 'Find all cases with a specific reaction',
    complexity: 'simple',
    sql: `SELECT r.primaryid, r.pt AS preferred_term
FROM reactions r
WHERE r.pt = 'Nausea'
LIMIT 100`,
    benchmarks: ['point-lookup', 'indexed-column'],
  },

  // Range scans
  {
    id: 'faers-range-1',
    name: 'Cases by date range',
    description: 'Find cases reported in a specific quarter',
    complexity: 'moderate',
    sql: `SELECT d.primaryid, d.event_dt, d.age, d.sex, d.reporter_country
FROM demographics d
WHERE d.event_dt >= '20230101'
  AND d.event_dt < '20230401'
ORDER BY d.event_dt
LIMIT 1000`,
    benchmarks: ['range-scan', 'date-filter'],
  },
  {
    id: 'faers-range-2',
    name: 'Cases by age group',
    description: 'Find cases for a specific age range',
    complexity: 'moderate',
    sql: `SELECT d.primaryid, d.age, d.age_cod, d.sex, d.wt
FROM demographics d
WHERE d.age >= 65
  AND d.age_cod = 'YR'
  AND d.sex = 'F'
ORDER BY d.age DESC
LIMIT 1000`,
    benchmarks: ['range-scan', 'multi-condition'],
  },
  {
    id: 'faers-range-3',
    name: 'Drug therapy duration',
    description: 'Find therapies within a duration range',
    complexity: 'moderate',
    sql: `SELECT t.primaryid, t.drugname, t.start_dt, t.end_dt, t.dur, t.dur_cod
FROM therapy t
WHERE t.dur >= 30
  AND t.dur_cod = 'DAY'
ORDER BY t.dur DESC
LIMIT 500`,
    benchmarks: ['range-scan', 'therapy-analysis'],
  },

  // Aggregations
  {
    id: 'faers-agg-1',
    name: 'Top reported drugs',
    description: 'Most frequently reported drugs in adverse events',
    complexity: 'moderate',
    sql: `SELECT
  d.drugname,
  COUNT(DISTINCT d.primaryid) as case_count,
  COUNT(*) as mention_count
FROM drugs d
WHERE d.drugname IS NOT NULL
GROUP BY d.drugname
ORDER BY case_count DESC
LIMIT 50`,
    benchmarks: ['aggregation', 'distinct-count'],
  },
  {
    id: 'faers-agg-2',
    name: 'Top adverse reactions',
    description: 'Most commonly reported adverse reactions',
    complexity: 'moderate',
    sql: `SELECT
  r.pt AS preferred_term,
  COUNT(DISTINCT r.primaryid) as case_count
FROM reactions r
WHERE r.pt IS NOT NULL
GROUP BY r.pt
ORDER BY case_count DESC
LIMIT 50`,
    benchmarks: ['aggregation', 'reaction-analysis'],
  },
  {
    id: 'faers-agg-3',
    name: 'Cases by country',
    description: 'Geographic distribution of adverse event reports',
    complexity: 'moderate',
    sql: `SELECT
  d.reporter_country,
  COUNT(*) as case_count,
  COUNT(CASE WHEN d.sex = 'M' THEN 1 END) as male_count,
  COUNT(CASE WHEN d.sex = 'F' THEN 1 END) as female_count
FROM demographics d
WHERE d.reporter_country IS NOT NULL
GROUP BY d.reporter_country
ORDER BY case_count DESC
LIMIT 50`,
    benchmarks: ['aggregation', 'geographic-analysis'],
  },
  {
    id: 'faers-agg-4',
    name: 'Quarterly trend',
    description: 'Number of reports per quarter over time',
    complexity: 'moderate',
    sql: `SELECT
  SUBSTR(CAST(d.event_dt AS VARCHAR), 1, 4) as year,
  CASE
    WHEN SUBSTR(CAST(d.event_dt AS VARCHAR), 5, 2) IN ('01', '02', '03') THEN 'Q1'
    WHEN SUBSTR(CAST(d.event_dt AS VARCHAR), 5, 2) IN ('04', '05', '06') THEN 'Q2'
    WHEN SUBSTR(CAST(d.event_dt AS VARCHAR), 5, 2) IN ('07', '08', '09') THEN 'Q3'
    ELSE 'Q4'
  END as quarter,
  COUNT(*) as case_count
FROM demographics d
WHERE d.event_dt IS NOT NULL
  AND LENGTH(CAST(d.event_dt AS VARCHAR)) >= 6
GROUP BY year, quarter
ORDER BY year, quarter`,
    benchmarks: ['aggregation', 'time-series'],
  },
  {
    id: 'faers-agg-5',
    name: 'Outcome distribution',
    description: 'Distribution of case outcomes',
    complexity: 'simple',
    sql: `SELECT
  o.outc_cod,
  CASE o.outc_cod
    WHEN 'DE' THEN 'Death'
    WHEN 'LT' THEN 'Life-Threatening'
    WHEN 'HO' THEN 'Hospitalization'
    WHEN 'DS' THEN 'Disability'
    WHEN 'CA' THEN 'Congenital Anomaly'
    WHEN 'RI' THEN 'Required Intervention'
    WHEN 'OT' THEN 'Other'
    ELSE 'Unknown'
  END as outcome_description,
  COUNT(DISTINCT o.primaryid) as case_count
FROM outcomes o
GROUP BY o.outc_cod
ORDER BY case_count DESC`,
    benchmarks: ['aggregation', 'outcome-analysis'],
  },

  // Complex joins - drug-reaction analysis
  {
    id: 'faers-join-1',
    name: 'Drug-reaction pairs',
    description: 'Find all reactions for a specific drug',
    complexity: 'complex',
    sql: `SELECT
  d.drugname,
  r.pt AS reaction,
  COUNT(DISTINCT d.primaryid) as case_count
FROM drugs d
JOIN reactions r ON r.primaryid = d.primaryid
WHERE d.drugname ILIKE '%metformin%'
  AND d.role_cod = 'PS'  -- Primary suspect
GROUP BY d.drugname, r.pt
ORDER BY case_count DESC
LIMIT 30`,
    benchmarks: ['join', 'drug-reaction-analysis'],
  },
  {
    id: 'faers-join-2',
    name: 'Full case details',
    description: 'Get complete case information with drugs and reactions',
    complexity: 'complex',
    sql: `SELECT
  demo.primaryid,
  demo.age,
  demo.sex,
  demo.event_dt,
  demo.reporter_country,
  d.drugname,
  d.role_cod,
  r.pt AS reaction,
  o.outc_cod AS outcome
FROM demographics demo
JOIN drugs d ON d.primaryid = demo.primaryid
JOIN reactions r ON r.primaryid = demo.primaryid
LEFT JOIN outcomes o ON o.primaryid = demo.primaryid
WHERE demo.primaryid = 123456789
ORDER BY d.role_cod, d.drugname`,
    benchmarks: ['multi-join', 'case-detail'],
  },
  {
    id: 'faers-join-3',
    name: 'Drug combination analysis',
    description: 'Find cases with specific drug combinations',
    complexity: 'complex',
    sql: `WITH drug_cases AS (
  SELECT primaryid
  FROM drugs
  WHERE drugname ILIKE '%warfarin%'
  INTERSECT
  SELECT primaryid
  FROM drugs
  WHERE drugname ILIKE '%aspirin%'
)
SELECT
  r.pt AS reaction,
  COUNT(*) as case_count
FROM drug_cases dc
JOIN reactions r ON r.primaryid = dc.primaryid
GROUP BY r.pt
ORDER BY case_count DESC
LIMIT 20`,
    benchmarks: ['cte', 'set-operations', 'combination-analysis'],
  },

  // Signal detection / disproportionality analysis
  {
    id: 'faers-signal-1',
    name: 'Proportional Reporting Ratio (PRR)',
    description: 'Calculate PRR for drug-reaction pairs',
    complexity: 'expert',
    sql: `WITH drug_reaction_counts AS (
  SELECT
    d.drugname,
    r.pt AS reaction,
    COUNT(DISTINCT d.primaryid) as a  -- cases with drug and reaction
  FROM drugs d
  JOIN reactions r ON r.primaryid = d.primaryid
  WHERE d.role_cod = 'PS'
  GROUP BY d.drugname, r.pt
  HAVING COUNT(DISTINCT d.primaryid) >= 10
),
drug_totals AS (
  SELECT
    d.drugname,
    COUNT(DISTINCT d.primaryid) as drug_total
  FROM drugs d
  WHERE d.role_cod = 'PS'
  GROUP BY d.drugname
),
reaction_totals AS (
  SELECT
    r.pt AS reaction,
    COUNT(DISTINCT r.primaryid) as reaction_total
  FROM reactions r
  GROUP BY r.pt
),
total_cases AS (
  SELECT COUNT(DISTINCT primaryid) as total
  FROM demographics
)
SELECT
  drc.drugname,
  drc.reaction,
  drc.a as case_count,
  dt.drug_total,
  rt.reaction_total,
  tc.total,
  ROUND(
    (drc.a * 1.0 / dt.drug_total) /
    ((rt.reaction_total - drc.a) * 1.0 / (tc.total - dt.drug_total)),
    2
  ) as PRR,
  ROUND(
    (drc.a * 1.0 / (drc.a + (dt.drug_total - drc.a))) /
    ((rt.reaction_total - drc.a) * 1.0 / ((tc.total - drc.a) - (dt.drug_total - drc.a))),
    2
  ) as ROR
FROM drug_reaction_counts drc
JOIN drug_totals dt ON dt.drugname = drc.drugname
JOIN reaction_totals rt ON rt.reaction = drc.reaction
CROSS JOIN total_cases tc
WHERE drc.a >= 25
  AND (drc.a * 1.0 / dt.drug_total) / ((rt.reaction_total - drc.a) * 1.0 / (tc.total - dt.drug_total)) >= 2
ORDER BY PRR DESC
LIMIT 100`,
    benchmarks: ['signal-detection', 'disproportionality', 'complex-calculation'],
  },
  {
    id: 'faers-signal-2',
    name: 'Time-to-onset analysis',
    description: 'Analyze time from drug start to adverse event',
    complexity: 'expert',
    sql: `WITH onset_data AS (
  SELECT
    d.drugname,
    d.primaryid,
    CASE t.dur_cod
      WHEN 'DAY' THEN t.dur
      WHEN 'WK' THEN t.dur * 7
      WHEN 'MON' THEN t.dur * 30
      WHEN 'YR' THEN t.dur * 365
      ELSE NULL
    END as onset_days
  FROM drugs d
  JOIN therapy t ON t.primaryid = d.primaryid AND t.drugname = d.drugname
  WHERE d.role_cod = 'PS'
    AND t.dur IS NOT NULL
    AND t.dur > 0
)
SELECT
  drugname,
  COUNT(*) as case_count,
  ROUND(AVG(onset_days), 1) as avg_onset_days,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY onset_days) as median_onset_days,
  MIN(onset_days) as min_onset,
  MAX(onset_days) as max_onset
FROM onset_data
WHERE onset_days IS NOT NULL
  AND onset_days <= 365  -- Within 1 year
GROUP BY drugname
HAVING COUNT(*) >= 50
ORDER BY avg_onset_days
LIMIT 50`,
    benchmarks: ['temporal-analysis', 'percentile', 'pharmacovigilance'],
  },
  {
    id: 'faers-signal-3',
    name: 'Age-stratified analysis',
    description: 'Analyze reactions by age group',
    complexity: 'expert',
    sql: `WITH age_groups AS (
  SELECT
    demo.primaryid,
    CASE
      WHEN demo.age_cod != 'YR' THEN 'Unknown'
      WHEN demo.age < 18 THEN 'Pediatric (<18)'
      WHEN demo.age < 65 THEN 'Adult (18-64)'
      WHEN demo.age >= 65 THEN 'Elderly (65+)'
      ELSE 'Unknown'
    END as age_group
  FROM demographics demo
  WHERE demo.age IS NOT NULL
)
SELECT
  d.drugname,
  ag.age_group,
  r.pt AS reaction,
  COUNT(*) as case_count,
  COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY d.drugname, r.pt) as pct_of_drug_reaction
FROM drugs d
JOIN age_groups ag ON ag.primaryid = d.primaryid
JOIN reactions r ON r.primaryid = d.primaryid
WHERE d.drugname ILIKE '%acetaminophen%'
  AND d.role_cod = 'PS'
GROUP BY d.drugname, ag.age_group, r.pt
HAVING COUNT(*) >= 10
ORDER BY r.pt, ag.age_group`,
    benchmarks: ['stratified-analysis', 'window-functions', 'demographic-analysis'],
  },

  // Temporal analysis
  {
    id: 'faers-temporal-1',
    name: 'Drug report trends',
    description: 'Track reporting trends for a drug over time',
    complexity: 'complex',
    sql: `WITH monthly_counts AS (
  SELECT
    SUBSTR(CAST(demo.event_dt AS VARCHAR), 1, 6) as year_month,
    COUNT(DISTINCT d.primaryid) as case_count
  FROM demographics demo
  JOIN drugs d ON d.primaryid = demo.primaryid
  WHERE d.drugname ILIKE '%ozempic%'
    AND d.role_cod = 'PS'
    AND demo.event_dt >= 20180101
  GROUP BY year_month
)
SELECT
  year_month,
  case_count,
  SUM(case_count) OVER (ORDER BY year_month) as cumulative_count,
  ROUND(AVG(case_count) OVER (ORDER BY year_month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 1) as moving_avg_3mo
FROM monthly_counts
WHERE year_month IS NOT NULL
ORDER BY year_month`,
    benchmarks: ['time-series', 'window-functions', 'cumulative'],
  },

  // Text search and pattern matching
  {
    id: 'faers-text-1',
    name: 'Drug class search',
    description: 'Find all drugs in a therapeutic class',
    complexity: 'moderate',
    sql: {
      postgres: `SELECT DISTINCT drugname, prod_ai
FROM drugs
WHERE to_tsvector('english', drugname || ' ' || COALESCE(prod_ai, ''))
  @@ to_tsquery('english', 'statin | atorvastatin | simvastatin | rosuvastatin')
LIMIT 100`,
      duckdb: `SELECT DISTINCT drugname, prod_ai
FROM drugs
WHERE drugname ILIKE '%statin%'
   OR drugname ILIKE '%atorvastatin%'
   OR drugname ILIKE '%simvastatin%'
   OR drugname ILIKE '%rosuvastatin%'
LIMIT 100`,
      sqlite: `SELECT DISTINCT drugname, prod_ai
FROM drugs
WHERE drugname LIKE '%statin%'
   OR drugname LIKE '%atorvastatin%'
   OR drugname LIKE '%simvastatin%'
   OR drugname LIKE '%rosuvastatin%'
LIMIT 100`,
    },
    benchmarks: ['full-text-search', 'drug-class'],
  },
]

/**
 * FAERS dataset configuration
 */
export const fdaFaers: FAERSConfig = {
  id: 'fda-faers',
  name: 'FDA FAERS (Adverse Event Reporting System)',
  description: `The FDA Adverse Event Reporting System contains ~25M adverse event reports
from 1968 to present. Excellent for healthcare analytics, drug safety surveillance,
signal detection, and temporal pattern analysis.`,
  category: 'knowledge-graph',
  size: 'xlarge',
  rowCount: '~25M reports, ~100M drug records, ~50M reactions',
  compressedSize: '~5GB (quarterly ASCII files)',
  uncompressedSize: '~15-20GB',
  sourceUrl: 'https://fis.fda.gov/extensions/FPD-QDE-FAERS/FPD-QDE-FAERS.html',
  license: 'Public Domain (US Government)',
  suitedFor: ['duckdb', 'postgres', 'clickhouse'],
  updateFrequency: 'Quarterly (Jan, Apr, Jul, Oct)',

  dataDictionary: 'https://fis.fda.gov/content/Exports/FAERS_QDE_Documentation.pdf',

  quarterlyFiles: [
    {
      quarter: '2024Q3',
      releaseDate: '2024-10-01',
      url: 'https://fis.fda.gov/content/Exports/faers_ascii_2024Q3.zip',
      reportCount: '~500K reports',
    },
    {
      quarter: '2024Q2',
      releaseDate: '2024-07-01',
      url: 'https://fis.fda.gov/content/Exports/faers_ascii_2024Q2.zip',
      reportCount: '~500K reports',
    },
    {
      quarter: '2024Q1',
      releaseDate: '2024-04-01',
      url: 'https://fis.fda.gov/content/Exports/faers_ascii_2024Q1.zip',
      reportCount: '~500K reports',
    },
  ],

  entityStats: {
    total_reports: '~25M (1968-present)',
    annual_reports: '~2M per year (recent)',
    drug_records: '~100M',
    reaction_records: '~50M',
    outcome_records: '~30M',
    unique_drugs: '~500K distinct drug names',
    unique_reactions: '~25K distinct MedDRA preferred terms',
  },

  downloadConfigs: {
    local: {
      urls: [
        'https://fis.fda.gov/content/Exports/faers_ascii_2024Q3.zip',
      ],
      size: '~200MB (single quarter)',
      rowCount: '~500K reports',
      instructions: [
        '# Download a single quarter for local testing',
        'mkdir -p faers_data && cd faers_data',
        'curl -O https://fis.fda.gov/content/Exports/faers_ascii_2024Q3.zip',
        'unzip faers_ascii_2024Q3.zip',
        '',
        '# Files included:',
        '# - DEMO24Q3.txt (Demographics)',
        '# - DRUG24Q3.txt (Drugs)',
        '# - REAC24Q3.txt (Reactions)',
        '# - OUTC24Q3.txt (Outcomes)',
        '# - RPSR24Q3.txt (Report Sources)',
        '# - THER24Q3.txt (Therapy)',
        '# - INDI24Q3.txt (Indications)',
      ],
      setupCommands: [
        '# Load into DuckDB',
        "duckdb faers.db -c \"",
        "CREATE TABLE demographics AS SELECT * FROM read_csv('DEMO*.txt', delim='$', header=true);",
        "CREATE TABLE drugs AS SELECT * FROM read_csv('DRUG*.txt', delim='$', header=true);",
        "CREATE TABLE reactions AS SELECT * FROM read_csv('REAC*.txt', delim='$', header=true);",
        "CREATE TABLE outcomes AS SELECT * FROM read_csv('OUTC*.txt', delim='$', header=true);",
        "CREATE TABLE therapy AS SELECT * FROM read_csv('THER*.txt', delim='$', header=true);",
        "\"",
      ],
    },
    development: {
      urls: [
        'https://fis.fda.gov/content/Exports/faers_ascii_2024Q3.zip',
        'https://fis.fda.gov/content/Exports/faers_ascii_2024Q2.zip',
        'https://fis.fda.gov/content/Exports/faers_ascii_2024Q1.zip',
        'https://fis.fda.gov/content/Exports/faers_ascii_2023Q4.zip',
      ],
      size: '~800MB (4 quarters)',
      rowCount: '~2M reports',
      instructions: [
        '# Download 4 quarters for development',
        'for q in 2024Q3 2024Q2 2024Q1 2023Q4; do',
        '  curl -O "https://fis.fda.gov/content/Exports/faers_ascii_${q}.zip"',
        '  unzip -o "faers_ascii_${q}.zip" -d faers_data/',
        'done',
      ],
      setupCommands: [
        '# Load multiple quarters into DuckDB',
        'duckdb faers_dev.db < scripts/load_faers_multi_quarter.sql',
      ],
    },
    production: {
      urls: ['https://fis.fda.gov/extensions/FPD-QDE-FAERS/FPD-QDE-FAERS.html'],
      size: '~5GB compressed, ~15-20GB uncompressed',
      rowCount: '~25M reports (all history)',
      instructions: [
        '# Download all quarterly files from 2004 to present',
        '# Note: Pre-2004 data uses different format (AERS)',
        '',
        '# Script to download all FAERS quarters',
        'for year in $(seq 2004 2024); do',
        '  for q in Q1 Q2 Q3 Q4; do',
        '    url="https://fis.fda.gov/content/Exports/faers_ascii_${year}${q}.zip"',
        '    curl -O "$url" 2>/dev/null || echo "Skipping ${year}${q}"',
        '  done',
        'done',
        '',
        '# Extract all files',
        'for f in faers_ascii_*.zip; do',
        '  unzip -o "$f" -d faers_all/',
        'done',
      ],
      setupCommands: [
        '# Create partitioned tables for full dataset',
        'duckdb faers_full.db < scripts/load_faers_full.sql',
        '',
        '# Create materialized views for common queries',
        'duckdb faers_full.db < scripts/create_faers_views.sql',
      ],
    },
  },

  schema: {
    tableName: 'demographics',
    columns: [
      { name: 'primaryid', type: 'BIGINT', nullable: false, description: 'Unique case identifier' },
      { name: 'caseid', type: 'BIGINT', nullable: true, description: 'Case ID (may have duplicates for follow-ups)' },
      { name: 'caseversion', type: 'INTEGER', nullable: true, description: 'Version number of the case' },
      { name: 'i_f_code', type: 'CHAR(1)', nullable: true, description: 'Initial (I) or Followup (F)' },
      { name: 'event_dt', type: 'INTEGER', nullable: true, description: 'Event date (YYYYMMDD)' },
      { name: 'mfr_dt', type: 'INTEGER', nullable: true, description: 'Manufacturer receive date' },
      { name: 'init_fda_dt', type: 'INTEGER', nullable: true, description: 'Initial FDA receive date' },
      { name: 'fda_dt', type: 'INTEGER', nullable: true, description: 'FDA receive date' },
      { name: 'rept_cod', type: 'VARCHAR(10)', nullable: true, description: 'Report type code' },
      { name: 'auth_num', type: 'VARCHAR(100)', nullable: true, description: 'Manufacturer auth number' },
      { name: 'mfr_num', type: 'VARCHAR(100)', nullable: true, description: 'Manufacturer control number' },
      { name: 'mfr_sndr', type: 'VARCHAR(100)', nullable: true, description: 'Manufacturer name' },
      { name: 'lit_ref', type: 'TEXT', nullable: true, description: 'Literature reference' },
      { name: 'age', type: 'DECIMAL(12,2)', nullable: true, description: 'Patient age' },
      { name: 'age_cod', type: 'VARCHAR(10)', nullable: true, description: 'Age unit (YR, MON, WK, DY, HR)' },
      { name: 'age_grp', type: 'VARCHAR(10)', nullable: true, description: 'Age group code' },
      { name: 'sex', type: 'CHAR(1)', nullable: true, description: 'Patient sex (M, F, UNK)' },
      { name: 'wt', type: 'DECIMAL(12,2)', nullable: true, description: 'Patient weight' },
      { name: 'wt_cod', type: 'VARCHAR(10)', nullable: true, description: 'Weight unit (KG, LBS)' },
      { name: 'rept_dt', type: 'INTEGER', nullable: true, description: 'Report date' },
      { name: 'to_mfr', type: 'CHAR(1)', nullable: true, description: 'Sent to manufacturer flag' },
      { name: 'occp_cod', type: 'VARCHAR(10)', nullable: true, description: 'Reporter occupation code' },
      { name: 'reporter_country', type: 'VARCHAR(50)', nullable: true, description: 'Reporter country' },
      { name: 'occr_country', type: 'VARCHAR(50)', nullable: true, description: 'Occurrence country' },
    ],
    primaryKey: ['primaryid'],
    indexes: [
      { name: 'idx_demo_caseid', columns: ['caseid'], type: 'btree', description: 'Case lookup' },
      { name: 'idx_demo_event_dt', columns: ['event_dt'], type: 'btree', description: 'Date range queries' },
      { name: 'idx_demo_country', columns: ['reporter_country'], type: 'btree', description: 'Geographic analysis' },
    ],
    createTableSQL: {
      duckdb: `-- Demographics (core case information)
CREATE TABLE demographics (
  primaryid BIGINT PRIMARY KEY,
  caseid BIGINT,
  caseversion INTEGER,
  i_f_code CHAR(1),
  event_dt INTEGER,
  mfr_dt INTEGER,
  init_fda_dt INTEGER,
  fda_dt INTEGER,
  rept_cod VARCHAR(10),
  auth_num VARCHAR(100),
  mfr_num VARCHAR(100),
  mfr_sndr VARCHAR(100),
  lit_ref TEXT,
  age DECIMAL(12,2),
  age_cod VARCHAR(10),
  age_grp VARCHAR(10),
  sex CHAR(1),
  wt DECIMAL(12,2),
  wt_cod VARCHAR(10),
  rept_dt INTEGER,
  to_mfr CHAR(1),
  occp_cod VARCHAR(10),
  reporter_country VARCHAR(50),
  occr_country VARCHAR(50)
);

-- Drugs involved in cases
CREATE TABLE drugs (
  primaryid BIGINT NOT NULL,
  caseid BIGINT,
  drug_seq INTEGER,
  role_cod VARCHAR(5),      -- PS=Primary Suspect, SS=Secondary Suspect, C=Concomitant, I=Interacting
  drugname TEXT,
  prod_ai TEXT,             -- Active ingredient
  val_vbm INTEGER,
  route VARCHAR(50),
  dose_vbm TEXT,
  cum_dose_chr DECIMAL(15,5),
  cum_dose_unit VARCHAR(20),
  dechal CHAR(1),
  rechal CHAR(1),
  lot_num VARCHAR(100),
  exp_dt INTEGER,
  nda_num VARCHAR(20),
  dose_amt TEXT,
  dose_unit VARCHAR(20),
  dose_form VARCHAR(50),
  dose_freq VARCHAR(50)
);

-- Adverse reactions
CREATE TABLE reactions (
  primaryid BIGINT NOT NULL,
  caseid BIGINT,
  pt TEXT,                  -- Preferred Term (MedDRA)
  drug_rec_act TEXT
);

-- Outcomes
CREATE TABLE outcomes (
  primaryid BIGINT NOT NULL,
  caseid BIGINT,
  outc_cod VARCHAR(5)       -- DE=Death, LT=Life-Threatening, HO=Hospitalization, etc.
);

-- Therapy information
CREATE TABLE therapy (
  primaryid BIGINT NOT NULL,
  caseid BIGINT,
  dsg_drug_seq INTEGER,
  drugname TEXT,
  start_dt INTEGER,
  end_dt INTEGER,
  dur DECIMAL(12,2),
  dur_cod VARCHAR(10)
);

-- Indications
CREATE TABLE indications (
  primaryid BIGINT NOT NULL,
  caseid BIGINT,
  indi_drug_seq INTEGER,
  indi_pt TEXT              -- Indication Preferred Term
);

-- Report sources
CREATE TABLE report_sources (
  primaryid BIGINT NOT NULL,
  caseid BIGINT,
  rpsr_cod VARCHAR(10)      -- Report source code
);

-- Indexes
CREATE INDEX idx_drugs_primaryid ON drugs(primaryid);
CREATE INDEX idx_drugs_name ON drugs(drugname);
CREATE INDEX idx_drugs_role ON drugs(role_cod);
CREATE INDEX idx_reactions_primaryid ON reactions(primaryid);
CREATE INDEX idx_reactions_pt ON reactions(pt);
CREATE INDEX idx_outcomes_primaryid ON outcomes(primaryid);
CREATE INDEX idx_therapy_primaryid ON therapy(primaryid);
CREATE INDEX idx_demo_event_dt ON demographics(event_dt);`,

      postgres: `-- Demographics with full-text search
CREATE TABLE demographics (
  primaryid BIGINT PRIMARY KEY,
  caseid BIGINT,
  caseversion INTEGER,
  i_f_code CHAR(1),
  event_dt INTEGER,
  mfr_dt INTEGER,
  init_fda_dt INTEGER,
  fda_dt INTEGER,
  rept_cod VARCHAR(10),
  auth_num VARCHAR(100),
  mfr_num VARCHAR(100),
  mfr_sndr VARCHAR(100),
  lit_ref TEXT,
  age DECIMAL(12,2),
  age_cod VARCHAR(10),
  age_grp VARCHAR(10),
  sex CHAR(1),
  wt DECIMAL(12,2),
  wt_cod VARCHAR(10),
  rept_dt INTEGER,
  to_mfr CHAR(1),
  occp_cod VARCHAR(10),
  reporter_country VARCHAR(50),
  occr_country VARCHAR(50)
);

CREATE TABLE drugs (
  primaryid BIGINT NOT NULL REFERENCES demographics(primaryid),
  caseid BIGINT,
  drug_seq INTEGER,
  role_cod VARCHAR(5),
  drugname TEXT,
  prod_ai TEXT,
  val_vbm INTEGER,
  route VARCHAR(50),
  dose_vbm TEXT,
  cum_dose_chr DECIMAL(15,5),
  cum_dose_unit VARCHAR(20),
  dechal CHAR(1),
  rechal CHAR(1),
  lot_num VARCHAR(100),
  exp_dt INTEGER,
  nda_num VARCHAR(20),
  dose_amt TEXT,
  dose_unit VARCHAR(20),
  dose_form VARCHAR(50),
  dose_freq VARCHAR(50),
  drugname_tsv TSVECTOR GENERATED ALWAYS AS (to_tsvector('english', COALESCE(drugname, ''))) STORED
);

CREATE TABLE reactions (
  primaryid BIGINT NOT NULL REFERENCES demographics(primaryid),
  caseid BIGINT,
  pt TEXT,
  drug_rec_act TEXT
);

CREATE TABLE outcomes (
  primaryid BIGINT NOT NULL REFERENCES demographics(primaryid),
  caseid BIGINT,
  outc_cod VARCHAR(5)
);

CREATE TABLE therapy (
  primaryid BIGINT NOT NULL REFERENCES demographics(primaryid),
  caseid BIGINT,
  dsg_drug_seq INTEGER,
  drugname TEXT,
  start_dt INTEGER,
  end_dt INTEGER,
  dur DECIMAL(12,2),
  dur_cod VARCHAR(10)
);

-- Indexes
CREATE INDEX idx_drugs_primaryid ON drugs(primaryid);
CREATE INDEX idx_drugs_name_tsv ON drugs USING GIN(drugname_tsv);
CREATE INDEX idx_drugs_role ON drugs(role_cod);
CREATE INDEX idx_reactions_primaryid ON reactions(primaryid);
CREATE INDEX idx_reactions_pt ON reactions(pt);
CREATE INDEX idx_outcomes_primaryid ON outcomes(primaryid);
CREATE INDEX idx_demo_event_dt ON demographics(event_dt);
CREATE INDEX idx_demo_country ON demographics(reporter_country);`,

      clickhouse: `-- Optimized for large-scale analytics
CREATE TABLE demographics (
  primaryid Int64,
  caseid Nullable(Int64),
  event_dt Nullable(Int32),
  age Nullable(Float32),
  age_cod LowCardinality(Nullable(String)),
  sex LowCardinality(Nullable(String)),
  wt Nullable(Float32),
  reporter_country LowCardinality(Nullable(String))
) ENGINE = MergeTree()
ORDER BY (event_dt, primaryid)
PARTITION BY toYYYYMM(toDate(toString(event_dt)));

CREATE TABLE drugs (
  primaryid Int64,
  drug_seq Nullable(Int8),
  role_cod LowCardinality(Nullable(String)),
  drugname Nullable(String),
  prod_ai Nullable(String),
  route LowCardinality(Nullable(String))
) ENGINE = MergeTree()
ORDER BY (primaryid, drug_seq);

CREATE TABLE reactions (
  primaryid Int64,
  pt String
) ENGINE = MergeTree()
ORDER BY (pt, primaryid);

CREATE TABLE outcomes (
  primaryid Int64,
  outc_cod LowCardinality(String)
) ENGINE = MergeTree()
ORDER BY (outc_cod, primaryid);

-- Materialized view for drug-reaction counts
CREATE MATERIALIZED VIEW drug_reaction_counts
ENGINE = SummingMergeTree()
ORDER BY (drugname, pt)
AS SELECT
  d.drugname,
  r.pt,
  count() as count
FROM drugs d
JOIN reactions r ON r.primaryid = d.primaryid
WHERE d.role_cod = 'PS'
GROUP BY d.drugname, r.pt;`,

      sqlite: `-- SQLite schema
CREATE TABLE demographics (
  primaryid INTEGER PRIMARY KEY,
  caseid INTEGER,
  caseversion INTEGER,
  i_f_code TEXT,
  event_dt INTEGER,
  age REAL,
  age_cod TEXT,
  sex TEXT,
  wt REAL,
  wt_cod TEXT,
  reporter_country TEXT,
  occr_country TEXT
);

CREATE TABLE drugs (
  primaryid INTEGER NOT NULL,
  drug_seq INTEGER,
  role_cod TEXT,
  drugname TEXT,
  prod_ai TEXT,
  route TEXT
);

CREATE TABLE reactions (
  primaryid INTEGER NOT NULL,
  pt TEXT
);

CREATE TABLE outcomes (
  primaryid INTEGER NOT NULL,
  outc_cod TEXT
);

CREATE TABLE therapy (
  primaryid INTEGER NOT NULL,
  drugname TEXT,
  start_dt INTEGER,
  end_dt INTEGER,
  dur REAL,
  dur_cod TEXT
);

-- Indexes
CREATE INDEX idx_drugs_primaryid ON drugs(primaryid);
CREATE INDEX idx_drugs_name ON drugs(drugname);
CREATE INDEX idx_reactions_primaryid ON reactions(primaryid);
CREATE INDEX idx_reactions_pt ON reactions(pt);
CREATE INDEX idx_demo_event_dt ON demographics(event_dt);

-- FTS for drug name search
CREATE VIRTUAL TABLE drugs_fts USING fts5(drugname, prod_ai, content=drugs, content_rowid=rowid);`,

      db4: `-- Same as SQLite`,
      evodb: `-- Same as SQLite`,
    },
  },

  queries: faersQueries,

  performanceExpectations: {
    duckdb: {
      loadTime: '~5 minutes for full dataset',
      simpleQueryLatency: '<100ms',
      complexQueryLatency: '500ms-5s',
      storageEfficiency: 'Excellent',
      concurrency: 'Good',
      notes: [
        'Best for analytical queries',
        'Fast signal detection calculations',
        'Efficient time-series aggregations',
        'Good JOIN performance for drug-reaction analysis',
      ],
    },
    postgres: {
      loadTime: '~20 minutes',
      simpleQueryLatency: '<50ms',
      complexQueryLatency: '1-10s',
      storageEfficiency: 'Good',
      concurrency: 'Excellent',
      notes: [
        'Best for full-text drug search',
        'Excellent for complex JOINs',
        'Consider partitioning by year',
        'pg_trgm for fuzzy drug name matching',
      ],
    },
    clickhouse: {
      loadTime: '~3 minutes',
      simpleQueryLatency: '<50ms',
      complexQueryLatency: '100ms-2s',
      storageEfficiency: 'Excellent',
      concurrency: 'Excellent',
      notes: [
        'Best for large-scale aggregations',
        'Materialized views for common patterns',
        'LowCardinality for categorical columns',
        'Excellent compression ratio',
      ],
    },
    sqlite: {
      loadTime: '~30 minutes',
      simpleQueryLatency: '50-200ms',
      complexQueryLatency: '2-30s',
      storageEfficiency: 'Moderate',
      concurrency: 'Very limited',
      notes: [
        'Use subset for local development',
        'FTS5 for drug name search',
        'Single-writer limitation',
        'Consider quarterly subsets',
      ],
    },
    db4: {
      loadTime: '~30 minutes',
      simpleQueryLatency: '50-200ms',
      complexQueryLatency: '2-30s',
      storageEfficiency: 'Moderate',
      concurrency: 'Limited',
      notes: ['Same as SQLite', 'Good for edge pharmacovigilance'],
    },
    evodb: {
      loadTime: '~30 minutes',
      simpleQueryLatency: '50-200ms',
      complexQueryLatency: '2-30s',
      storageEfficiency: 'Moderate',
      concurrency: 'Limited',
      notes: ['Same as SQLite'],
    },
  },

  r2Config: {
    bucketName: 'bench-datasets',
    pathPrefix: 'fda-faers/',
    format: 'parquet',
    compression: 'zstd',
    partitioning: {
      columns: ['year', 'quarter'],
      format: 'year={year}/quarter={quarter}/*.parquet',
    },
    uploadInstructions: [
      '# Convert FAERS files to partitioned Parquet',
      'duckdb -c "',
      "  COPY (",
      "    SELECT *,",
      "    SUBSTR(CAST(event_dt AS VARCHAR), 1, 4) as year,",
      "    CASE",
      "      WHEN SUBSTR(CAST(event_dt AS VARCHAR), 5, 2) IN ('01','02','03') THEN 'Q1'",
      "      WHEN SUBSTR(CAST(event_dt AS VARCHAR), 5, 2) IN ('04','05','06') THEN 'Q2'",
      "      WHEN SUBSTR(CAST(event_dt AS VARCHAR), 5, 2) IN ('07','08','09') THEN 'Q3'",
      "      ELSE 'Q4'",
      "    END as quarter",
      "    FROM demographics",
      "  ) TO 'faers_parquet/demographics'",
      "  (FORMAT PARQUET, PARTITION_BY (year, quarter), COMPRESSION 'zstd');",
      '"',
      '',
      '# Upload to R2',
      'wrangler r2 object put bench-datasets/fda-faers/ --file=faers_parquet/ --recursive',
    ],
    duckdbInstructions: [
      '-- Query specific quarter from R2',
      "SELECT * FROM read_parquet('s3://bench-datasets/fda-faers/demographics/year=2024/quarter=Q3/*.parquet')",
      'LIMIT 1000;',
      '',
      '-- Cross-quarter analysis with partition pruning',
      "SELECT year, quarter, COUNT(*) FROM read_parquet('s3://bench-datasets/fda-faers/demographics/**/*.parquet')",
      'GROUP BY year, quarter',
      'ORDER BY year, quarter;',
    ],
  },
}

export default fdaFaers
