/**
 * Wiktionary Dataset
 *
 * Wiktionary dump files containing word definitions, translations,
 * etymologies, and linguistic data from multiple languages.
 *
 * Excellent for:
 * - Full-text search benchmarks
 * - Multi-language queries
 * - Linguistic analysis
 * - Dictionary/reference applications
 *
 * Best suited for:
 * - DuckDB (full-text search extension)
 * - SQLite with FTS5
 * - PostgreSQL with pg_trgm/full-text search
 */

import type { DatasetConfig, BenchmarkQuery, DatabaseType } from './index'

/**
 * Wiktionary-specific configuration
 */
export interface WiktionaryConfig extends DatasetConfig {
  /** Available language editions */
  languages: LanguageEdition[]
  /** Dump format versions */
  dumpFormats: string[]
}

/**
 * Language edition details
 */
interface LanguageEdition {
  code: string
  name: string
  entries: string
  size: string
  url: string
}

/**
 * Wiktionary benchmark queries
 */
const wiktionaryQueries: BenchmarkQuery[] = [
  // Simple full-text searches
  {
    id: 'fts-simple-1',
    name: 'Single word search',
    description: 'Search for a single common word',
    complexity: 'simple',
    sql: {
      duckdb: `SELECT title, snippet(definitions, 0, '<b>', '</b>', '...', 32) as snippet
FROM wiktionary
WHERE fts_main_wiktionary.match_bm25(id, 'etymology')
ORDER BY fts_main_wiktionary.match_bm25(id, 'etymology') DESC
LIMIT 20`,
      sqlite: `SELECT title, snippet(wiktionary_fts, 0, '<b>', '</b>', '...', 32)
FROM wiktionary_fts
WHERE wiktionary_fts MATCH 'etymology'
ORDER BY rank
LIMIT 20`,
      postgres: `SELECT title, ts_headline('english', definitions, q) as snippet
FROM wiktionary, to_tsquery('english', 'etymology') q
WHERE tsv @@ q
ORDER BY ts_rank(tsv, q) DESC
LIMIT 20`,
    },
    benchmarks: ['fts-single-term', 'ranking'],
    expectedResults: { rowCount: 20 },
  },
  {
    id: 'fts-simple-2',
    name: 'Prefix search',
    description: 'Search for words starting with prefix',
    complexity: 'simple',
    sql: {
      duckdb: `SELECT title, word_type, language
FROM wiktionary
WHERE title LIKE 'anti%'
ORDER BY title
LIMIT 100`,
      sqlite: `SELECT title, word_type, language
FROM wiktionary
WHERE title LIKE 'anti%'
ORDER BY title
LIMIT 100`,
      postgres: `SELECT title, word_type, language
FROM wiktionary
WHERE title LIKE 'anti%'
ORDER BY title
LIMIT 100`,
    },
    benchmarks: ['prefix-search', 'index-utilization'],
  },
  {
    id: 'fts-simple-3',
    name: 'Exact phrase search',
    description: 'Search for exact phrase in definitions',
    complexity: 'simple',
    sql: {
      duckdb: `SELECT title, definitions
FROM wiktionary
WHERE definitions LIKE '%derived from Latin%'
LIMIT 50`,
      sqlite: `SELECT title, definitions
FROM wiktionary_fts
WHERE wiktionary_fts MATCH '"derived from Latin"'
LIMIT 50`,
      postgres: `SELECT title, definitions
FROM wiktionary
WHERE to_tsvector('english', definitions) @@ phraseto_tsquery('english', 'derived from Latin')
LIMIT 50`,
    },
    benchmarks: ['phrase-search', 'exact-match'],
  },

  // Moderate complexity searches
  {
    id: 'fts-moderate-1',
    name: 'Multi-term boolean search',
    description: 'Boolean AND/OR search across fields',
    complexity: 'moderate',
    sql: {
      duckdb: `SELECT title, word_type, etymology
FROM wiktionary
WHERE fts_main_wiktionary.match_bm25(id, 'Greek AND Latin')
   OR fts_main_wiktionary.match_bm25(id, 'ancient AND root')
ORDER BY fts_main_wiktionary.match_bm25(id, 'Greek Latin ancient root') DESC
LIMIT 50`,
      sqlite: `SELECT title, word_type, etymology
FROM wiktionary_fts
WHERE wiktionary_fts MATCH '(Greek AND Latin) OR (ancient AND root)'
ORDER BY rank
LIMIT 50`,
      postgres: `SELECT title, word_type, etymology
FROM wiktionary
WHERE tsv @@ to_tsquery('english', '(Greek & Latin) | (ancient & root)')
ORDER BY ts_rank(tsv, to_tsquery('english', '(Greek & Latin) | (ancient & root)')) DESC
LIMIT 50`,
    },
    benchmarks: ['boolean-search', 'multi-term'],
  },
  {
    id: 'fts-moderate-2',
    name: 'Fuzzy search',
    description: 'Search with spelling variations',
    complexity: 'moderate',
    sql: {
      duckdb: `SELECT title, definitions,
       levenshtein(title, 'colour') as distance
FROM wiktionary
WHERE levenshtein(title, 'colour') <= 2
ORDER BY distance
LIMIT 20`,
      sqlite: `-- SQLite requires custom function for fuzzy
SELECT title, definitions
FROM wiktionary
WHERE title LIKE '%colo_r%' OR title LIKE '%colou%'
LIMIT 20`,
      postgres: `SELECT title, definitions,
       title <-> 'colour' as distance
FROM wiktionary
WHERE title % 'colour'
ORDER BY distance
LIMIT 20`,
    },
    benchmarks: ['fuzzy-search', 'edit-distance'],
  },
  {
    id: 'fts-moderate-3',
    name: 'Cross-language search',
    description: 'Find translations across languages',
    complexity: 'moderate',
    sql: `SELECT
  e.title as english_word,
  f.title as french_word,
  s.title as spanish_word
FROM wiktionary e
LEFT JOIN wiktionary f ON f.language = 'fr' AND f.translations LIKE '%' || e.title || '%'
LEFT JOIN wiktionary s ON s.language = 'es' AND s.translations LIKE '%' || e.title || '%'
WHERE e.language = 'en'
  AND e.title = 'love'
LIMIT 10`,
    benchmarks: ['join-performance', 'multi-language'],
  },
  {
    id: 'fts-moderate-4',
    name: 'Word type aggregation',
    description: 'Count entries by word type and language',
    complexity: 'moderate',
    sql: `SELECT
  language,
  word_type,
  COUNT(*) as count
FROM wiktionary
WHERE language IN ('en', 'fr', 'de', 'es', 'it')
GROUP BY language, word_type
ORDER BY language, count DESC`,
    benchmarks: ['aggregation', 'groupby-performance'],
  },

  // Complex linguistic queries
  {
    id: 'fts-complex-1',
    name: 'Etymology chain analysis',
    description: 'Find words with common etymological roots',
    complexity: 'complex',
    sql: `WITH RECURSIVE etymology_chain AS (
  SELECT title, etymology, 1 as depth
  FROM wiktionary
  WHERE etymology LIKE '%Proto-Indo-European%'
    AND language = 'en'

  UNION ALL

  SELECT w.title, w.etymology, ec.depth + 1
  FROM wiktionary w
  JOIN etymology_chain ec ON w.etymology LIKE '%' || ec.title || '%'
  WHERE ec.depth < 3
    AND w.language = 'en'
)
SELECT DISTINCT title, etymology, depth
FROM etymology_chain
ORDER BY depth, title
LIMIT 100`,
    benchmarks: ['recursive-cte', 'etymology-analysis'],
  },
  {
    id: 'fts-complex-2',
    name: 'Semantic similarity search',
    description: 'Find words with similar definitions',
    complexity: 'complex',
    sql: {
      duckdb: `WITH word_vectors AS (
  SELECT title,
         fts_main_wiktionary.match_bm25(id, 'happy joyful pleased content') as similarity
  FROM wiktionary
  WHERE language = 'en'
    AND word_type = 'adjective'
)
SELECT title, similarity
FROM word_vectors
WHERE similarity > 0
ORDER BY similarity DESC
LIMIT 20`,
      postgres: `SELECT title,
       ts_rank(tsv, to_tsquery('english', 'happy | joyful | pleased | content')) as similarity
FROM wiktionary
WHERE language = 'en'
  AND word_type = 'adjective'
  AND tsv @@ to_tsquery('english', 'happy | joyful | pleased | content')
ORDER BY similarity DESC
LIMIT 20`,
    },
    benchmarks: ['semantic-search', 'similarity-ranking'],
  },
  {
    id: 'fts-complex-3',
    name: 'Rhyme finder',
    description: 'Find rhyming words using phonetic patterns',
    complexity: 'complex',
    sql: `SELECT title, pronunciation
FROM wiktionary
WHERE language = 'en'
  AND pronunciation IS NOT NULL
  AND (
    pronunciation LIKE '%eɪʃən%'  -- -ation words
    OR pronunciation LIKE '%eɪʒən%'
  )
ORDER BY LENGTH(title)
LIMIT 50`,
    benchmarks: ['phonetic-search', 'pattern-matching'],
  },
  {
    id: 'fts-complex-4',
    name: 'Definition complexity analysis',
    description: 'Analyze definition length and complexity by word type',
    complexity: 'complex',
    sql: `SELECT
  word_type,
  AVG(LENGTH(definitions)) as avg_def_length,
  AVG(LENGTH(definitions) - LENGTH(REPLACE(definitions, ' ', '')) + 1) as avg_word_count,
  COUNT(*) as entry_count,
  COUNT(DISTINCT title) as unique_words
FROM wiktionary
WHERE language = 'en'
  AND definitions IS NOT NULL
  AND definitions != ''
GROUP BY word_type
HAVING entry_count > 1000
ORDER BY avg_word_count DESC`,
    benchmarks: ['string-functions', 'analytical-aggregation'],
  },

  // Expert linguistic analysis
  {
    id: 'fts-expert-1',
    name: 'Cognate detection',
    description: 'Find potential cognates across languages',
    complexity: 'expert',
    sql: `WITH english_words AS (
  SELECT title, etymology, definitions
  FROM wiktionary
  WHERE language = 'en'
    AND etymology LIKE '%Latin%'
    AND word_type = 'noun'
),
romance_words AS (
  SELECT title, language, etymology
  FROM wiktionary
  WHERE language IN ('fr', 'es', 'it', 'pt')
    AND etymology LIKE '%Latin%'
)
SELECT
  e.title as english,
  r.title as romance,
  r.language,
  e.etymology as en_etymology
FROM english_words e
JOIN romance_words r ON (
  -- Same first 4 characters (naive cognate detection)
  SUBSTR(LOWER(e.title), 1, 4) = SUBSTR(LOWER(r.title), 1, 4)
  OR e.etymology LIKE '%' || r.title || '%'
)
WHERE e.title != r.title
ORDER BY e.title, r.language
LIMIT 200`,
    benchmarks: ['cross-language-join', 'cognate-analysis'],
  },
  {
    id: 'fts-expert-2',
    name: 'Morphological analysis',
    description: 'Analyze word formation patterns',
    complexity: 'expert',
    sql: `WITH prefixed_words AS (
  SELECT
    title,
    CASE
      WHEN title LIKE 'un%' THEN 'un-'
      WHEN title LIKE 're%' THEN 're-'
      WHEN title LIKE 'pre%' THEN 'pre-'
      WHEN title LIKE 'dis%' THEN 'dis-'
      WHEN title LIKE 'anti%' THEN 'anti-'
      WHEN title LIKE 'inter%' THEN 'inter-'
      ELSE 'other'
    END as prefix,
    word_type
  FROM wiktionary
  WHERE language = 'en'
    AND LENGTH(title) > 5
)
SELECT
  prefix,
  word_type,
  COUNT(*) as count,
  COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY prefix) as pct_of_prefix
FROM prefixed_words
WHERE prefix != 'other'
GROUP BY prefix, word_type
HAVING count > 10
ORDER BY prefix, count DESC`,
    benchmarks: ['string-prefix', 'window-functions', 'morphology'],
  },
  {
    id: 'fts-expert-3',
    name: 'Multi-sense disambiguation',
    description: 'Analyze words with multiple meanings',
    complexity: 'expert',
    sql: `WITH sense_counts AS (
  SELECT
    title,
    word_type,
    -- Count definition entries (separated by numbers or bullets)
    LENGTH(definitions) - LENGTH(REPLACE(REPLACE(definitions, '1.', ''), '2.', '')) as sense_indicators,
    definitions
  FROM wiktionary
  WHERE language = 'en'
    AND definitions IS NOT NULL
)
SELECT
  title,
  word_type,
  sense_indicators as estimated_senses,
  LENGTH(definitions) as def_length,
  definitions
FROM sense_counts
WHERE sense_indicators > 3
ORDER BY sense_indicators DESC
LIMIT 50`,
    benchmarks: ['polysemy-analysis', 'string-parsing'],
  },
  {
    id: 'fts-expert-4',
    name: 'Language family statistics',
    description: 'Analyze etymological origins by language family',
    complexity: 'expert',
    sql: `SELECT
  CASE
    WHEN etymology LIKE '%Proto-Germanic%' THEN 'Germanic'
    WHEN etymology LIKE '%Latin%' OR etymology LIKE '%Romance%' THEN 'Romance/Latin'
    WHEN etymology LIKE '%Greek%' THEN 'Greek'
    WHEN etymology LIKE '%Proto-Indo-European%' THEN 'PIE (direct)'
    WHEN etymology LIKE '%Old French%' THEN 'Old French'
    WHEN etymology LIKE '%Old English%' THEN 'Old English'
    WHEN etymology LIKE '%Arabic%' THEN 'Arabic'
    WHEN etymology LIKE '%Japanese%' THEN 'Japanese'
    WHEN etymology LIKE '%Chinese%' THEN 'Chinese'
    ELSE 'Other/Unknown'
  END as origin_family,
  COUNT(*) as word_count,
  COUNT(*) * 100.0 / (SELECT COUNT(*) FROM wiktionary WHERE language = 'en' AND etymology IS NOT NULL) as percentage
FROM wiktionary
WHERE language = 'en'
  AND etymology IS NOT NULL
GROUP BY origin_family
ORDER BY word_count DESC`,
    benchmarks: ['case-expression', 'etymology-stats', 'percentage-calc'],
  },
]

/**
 * Wiktionary dataset configuration
 */
export const wiktionary: WiktionaryConfig = {
  id: 'wiktionary',
  name: 'Wiktionary Dumps',
  description: `Wiktionary database dumps containing word definitions, translations, etymologies,
pronunciations, and linguistic metadata. Excellent for full-text search benchmarks
and linguistic analysis across 170+ languages.`,
  category: 'full-text',
  size: 'large',
  rowCount: '~7M entries (English), ~40M total',
  compressedSize: '~10GB (all languages)',
  uncompressedSize: '~80GB (parsed JSON/SQL)',
  sourceUrl: 'https://dumps.wikimedia.org/enwiktionary/',
  license: 'CC BY-SA 3.0',
  suitedFor: ['duckdb', 'sqlite', 'postgres', 'db4'],

  languages: [
    {
      code: 'en',
      name: 'English',
      entries: '~7,000,000',
      size: '~2GB compressed',
      url: 'https://dumps.wikimedia.org/enwiktionary/latest/enwiktionary-latest-pages-articles.xml.bz2',
    },
    {
      code: 'fr',
      name: 'French',
      entries: '~4,500,000',
      size: '~1.5GB compressed',
      url: 'https://dumps.wikimedia.org/frwiktionary/latest/frwiktionary-latest-pages-articles.xml.bz2',
    },
    {
      code: 'de',
      name: 'German',
      entries: '~1,000,000',
      size: '~500MB compressed',
      url: 'https://dumps.wikimedia.org/dewiktionary/latest/dewiktionary-latest-pages-articles.xml.bz2',
    },
    {
      code: 'es',
      name: 'Spanish',
      entries: '~1,200,000',
      size: '~400MB compressed',
      url: 'https://dumps.wikimedia.org/eswiktionary/latest/eswiktionary-latest-pages-articles.xml.bz2',
    },
    {
      code: 'ru',
      name: 'Russian',
      entries: '~1,100,000',
      size: '~400MB compressed',
      url: 'https://dumps.wikimedia.org/ruwiktionary/latest/ruwiktionary-latest-pages-articles.xml.bz2',
    },
  ],
  dumpFormats: ['xml.bz2', 'sql.gz', 'json (parsed)'],

  downloadConfigs: {
    local: {
      urls: [
        'https://dumps.wikimedia.org/enwiktionary/latest/enwiktionary-latest-pages-articles.xml.bz2',
      ],
      size: '~200MB (sample)',
      rowCount: '100,000',
      instructions: [
        '# Download English Wiktionary dump',
        'curl -O https://dumps.wikimedia.org/enwiktionary/latest/enwiktionary-latest-pages-articles.xml.bz2',
        '',
        '# For local testing, use wiktextract to parse a subset',
        '# https://github.com/tatuylonen/wiktextract',
        'pip install wiktextract',
        '',
        '# Extract first 100k entries',
        'bzcat enwiktionary-latest-pages-articles.xml.bz2 | head -n 1000000 > sample.xml',
        'wiktextract --language en sample.xml --out wiktionary_sample.json',
      ],
      setupCommands: [
        '# Convert JSON to Parquet for efficient querying',
        'duckdb -c "',
        "  CREATE TABLE wiktionary AS",
        "  SELECT",
        "    word as title,",
        "    pos as word_type,",
        "    lang as language,",
        "    senses[1].glosses[1] as definitions,",
        "    etymology_text as etymology,",
        "    sounds[1].ipa as pronunciation,",
        "    json(translations) as translations",
        "  FROM read_json('wiktionary_sample.json', auto_detect=true, format='newline_delimited')",
        '"',
      ],
    },
    development: {
      urls: [
        'https://dumps.wikimedia.org/enwiktionary/latest/enwiktionary-latest-pages-articles.xml.bz2',
      ],
      size: '~800MB',
      rowCount: '1,000,000',
      instructions: [
        '# Parse 1M entries for development',
        'wiktextract --language en enwiktionary-latest-pages-articles.xml.bz2 \\',
        '  --out wiktionary_dev.json \\',
        '  --num-processes 4',
      ],
      setupCommands: [
        '# Load into DuckDB with full-text search',
        'duckdb wiktionary.db < scripts/create_wiktionary_fts.sql',
      ],
    },
    production: {
      urls: [
        'https://dumps.wikimedia.org/enwiktionary/latest/enwiktionary-latest-pages-articles.xml.bz2',
        'https://dumps.wikimedia.org/frwiktionary/latest/frwiktionary-latest-pages-articles.xml.bz2',
        'https://dumps.wikimedia.org/dewiktionary/latest/dewiktionary-latest-pages-articles.xml.bz2',
      ],
      size: '~10GB compressed',
      rowCount: '~40,000,000',
      instructions: [
        '# Download all major language dumps',
        'for lang in en fr de es ru zh ja; do',
        '  curl -O https://dumps.wikimedia.org/${lang}wiktionary/latest/${lang}wiktionary-latest-pages-articles.xml.bz2',
        'done',
        '',
        '# Parse all languages',
        'for lang in en fr de es ru zh ja; do',
        '  wiktextract --language $lang ${lang}wiktionary-latest-pages-articles.xml.bz2 \\',
        '    --out wiktionary_${lang}.json',
        'done',
      ],
      setupCommands: [
        '# Combine all languages into single database',
        'duckdb wiktionary_full.db < scripts/create_multilang_wiktionary.sql',
      ],
    },
  },

  schema: {
    tableName: 'wiktionary',
    columns: [
      { name: 'id', type: 'INTEGER', nullable: false, description: 'Auto-increment primary key' },
      { name: 'title', type: 'TEXT', nullable: false, description: 'Word or phrase' },
      { name: 'language', type: 'VARCHAR(10)', nullable: false, description: 'ISO 639-1 language code' },
      {
        name: 'word_type',
        type: 'VARCHAR(50)',
        nullable: true,
        description: 'Part of speech (noun, verb, adjective, etc.)',
      },
      { name: 'definitions', type: 'TEXT', nullable: true, description: 'Word definitions (JSON array or text)' },
      { name: 'etymology', type: 'TEXT', nullable: true, description: 'Word origin and history' },
      { name: 'pronunciation', type: 'TEXT', nullable: true, description: 'IPA pronunciation' },
      { name: 'translations', type: 'JSON', nullable: true, description: 'Translations to other languages' },
      { name: 'synonyms', type: 'JSON', nullable: true, description: 'List of synonyms' },
      { name: 'antonyms', type: 'JSON', nullable: true, description: 'List of antonyms' },
      { name: 'related_terms', type: 'JSON', nullable: true, description: 'Related words and phrases' },
      { name: 'derived_terms', type: 'JSON', nullable: true, description: 'Words derived from this entry' },
      { name: 'examples', type: 'TEXT', nullable: true, description: 'Usage examples' },
      { name: 'categories', type: 'JSON', nullable: true, description: 'Wiktionary categories' },
      { name: 'raw_wikitext', type: 'TEXT', nullable: true, description: 'Original wikitext (optional)' },
    ],
    primaryKey: ['id'],
    indexes: [
      { name: 'idx_title', columns: ['title'], type: 'btree', description: 'Word lookup' },
      { name: 'idx_language', columns: ['language'], type: 'btree', description: 'Language filtering' },
      { name: 'idx_word_type', columns: ['word_type'], type: 'btree', description: 'Part of speech filtering' },
      { name: 'idx_title_lang', columns: ['title', 'language'], type: 'btree', description: 'Composite lookup' },
      {
        name: 'fts_definitions',
        columns: ['definitions', 'etymology'],
        type: 'fulltext',
        description: 'Full-text search on definitions',
      },
    ],
    createTableSQL: {
      duckdb: `-- Create base table
CREATE TABLE wiktionary (
  id INTEGER PRIMARY KEY,
  title TEXT NOT NULL,
  language VARCHAR(10) NOT NULL,
  word_type VARCHAR(50),
  definitions TEXT,
  etymology TEXT,
  pronunciation TEXT,
  translations JSON,
  synonyms JSON,
  antonyms JSON,
  related_terms JSON,
  derived_terms JSON,
  examples TEXT,
  categories JSON
);

-- Create FTS index
INSTALL fts;
LOAD fts;
PRAGMA create_fts_index('wiktionary', 'id', 'title', 'definitions', 'etymology');`,

      sqlite: `-- Create base table
CREATE TABLE wiktionary (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  title TEXT NOT NULL,
  language TEXT NOT NULL,
  word_type TEXT,
  definitions TEXT,
  etymology TEXT,
  pronunciation TEXT,
  translations TEXT,
  synonyms TEXT,
  antonyms TEXT,
  related_terms TEXT,
  derived_terms TEXT,
  examples TEXT,
  categories TEXT
);

-- Create FTS5 virtual table
CREATE VIRTUAL TABLE wiktionary_fts USING fts5(
  title,
  definitions,
  etymology,
  content='wiktionary',
  content_rowid='id'
);

-- Triggers to keep FTS in sync
CREATE TRIGGER wiktionary_ai AFTER INSERT ON wiktionary BEGIN
  INSERT INTO wiktionary_fts(rowid, title, definitions, etymology)
  VALUES (new.id, new.title, new.definitions, new.etymology);
END;`,

      postgres: `-- Create base table
CREATE TABLE wiktionary (
  id SERIAL PRIMARY KEY,
  title TEXT NOT NULL,
  language VARCHAR(10) NOT NULL,
  word_type VARCHAR(50),
  definitions TEXT,
  etymology TEXT,
  pronunciation TEXT,
  translations JSONB,
  synonyms JSONB,
  antonyms JSONB,
  related_terms JSONB,
  derived_terms JSONB,
  examples TEXT,
  categories JSONB,
  tsv TSVECTOR GENERATED ALWAYS AS (
    setweight(to_tsvector('english', coalesce(title,'')), 'A') ||
    setweight(to_tsvector('english', coalesce(definitions,'')), 'B') ||
    setweight(to_tsvector('english', coalesce(etymology,'')), 'C')
  ) STORED
);

-- Create indexes
CREATE INDEX idx_wiktionary_title ON wiktionary(title);
CREATE INDEX idx_wiktionary_lang ON wiktionary(language);
CREATE INDEX idx_wiktionary_tsv ON wiktionary USING GIN(tsv);
CREATE INDEX idx_wiktionary_trgm ON wiktionary USING GIN(title gin_trgm_ops);`,

      db4: `-- Same as SQLite
CREATE TABLE wiktionary (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  title TEXT NOT NULL,
  language TEXT NOT NULL,
  word_type TEXT,
  definitions TEXT,
  etymology TEXT,
  pronunciation TEXT,
  translations TEXT,
  synonyms TEXT,
  antonyms TEXT,
  related_terms TEXT,
  derived_terms TEXT,
  examples TEXT,
  categories TEXT
);

CREATE VIRTUAL TABLE wiktionary_fts USING fts5(
  title, definitions, etymology,
  content='wiktionary', content_rowid='id'
);`,

      evodb: `-- Same as db4`,

      clickhouse: `-- ClickHouse is not ideal for full-text search
-- Consider using Manticore or Elasticsearch instead
CREATE TABLE wiktionary (
  id UInt64,
  title String,
  language LowCardinality(String),
  word_type LowCardinality(Nullable(String)),
  definitions String,
  etymology Nullable(String),
  pronunciation Nullable(String)
) ENGINE = MergeTree()
ORDER BY (language, title);`,
    },
  },

  queries: wiktionaryQueries,

  performanceExpectations: {
    duckdb: {
      loadTime: '~5 minutes for 7M entries',
      simpleQueryLatency: '10-50ms (FTS)',
      complexQueryLatency: '100ms-1s',
      storageEfficiency: 'Good with FTS extension',
      concurrency: 'Moderate',
      notes: [
        'FTS extension provides BM25 ranking',
        'Good for analytical queries on metadata',
        'Parquet format efficient for columns',
      ],
    },
    sqlite: {
      loadTime: '~10 minutes',
      simpleQueryLatency: '5-20ms (FTS5)',
      complexQueryLatency: '50-500ms',
      storageEfficiency: 'Good with FTS5',
      concurrency: 'Limited (single writer)',
      notes: [
        'FTS5 is highly optimized',
        'Best for embedded use cases',
        'Excellent prefix search performance',
      ],
    },
    postgres: {
      loadTime: '~15 minutes',
      simpleQueryLatency: '10-50ms',
      complexQueryLatency: '100ms-2s',
      storageEfficiency: 'Moderate',
      concurrency: 'Excellent',
      notes: [
        'pg_trgm for fuzzy search',
        'GIN indexes for full-text',
        'Best for production multi-user',
      ],
    },
    db4: {
      loadTime: '~10 minutes',
      simpleQueryLatency: '5-20ms',
      complexQueryLatency: '50-500ms',
      storageEfficiency: 'Good',
      concurrency: 'Limited',
      notes: ['Same as SQLite', 'FTS5 available', 'Edge deployment friendly'],
    },
    evodb: {
      loadTime: '~10 minutes',
      simpleQueryLatency: '10-30ms',
      complexQueryLatency: '100ms-1s',
      storageEfficiency: 'Moderate',
      concurrency: 'Limited',
      notes: ['Event-sourced storage', 'Good for change tracking', 'FTS via SQLite'],
    },
    clickhouse: {
      loadTime: '~2 minutes',
      simpleQueryLatency: '100ms-500ms (no FTS)',
      complexQueryLatency: '500ms-5s',
      storageEfficiency: 'Excellent compression',
      concurrency: 'Excellent',
      notes: [
        'Not optimized for full-text search',
        'Good for metadata analytics only',
        'Consider Manticore for FTS',
      ],
    },
  },

  r2Config: {
    bucketName: 'bench-datasets',
    pathPrefix: 'wiktionary/',
    format: 'parquet',
    compression: 'zstd',
    partitioning: {
      columns: ['language'],
      format: 'lang={language}/wiktionary_{language}.parquet',
    },
    uploadInstructions: [
      '# Convert parsed JSON to partitioned Parquet',
      'duckdb -c "',
      "  COPY (",
      "    SELECT * FROM read_json('wiktionary_*.json', format='newline_delimited')",
      "  ) TO 'wiktionary_parquet'",
      "  (FORMAT PARQUET, PARTITION_BY (language), COMPRESSION 'zstd');",
      '"',
      '',
      '# Upload to R2',
      'wrangler r2 object put bench-datasets/wiktionary/ --file=wiktionary_parquet/ --recursive',
    ],
    duckdbInstructions: [
      '-- Query specific language from R2',
      "SELECT * FROM read_parquet('s3://bench-datasets/wiktionary/lang=en/*.parquet')",
      "WHERE title LIKE 'auto%';",
      '',
      '-- Query all languages',
      "SELECT language, COUNT(*) FROM read_parquet('s3://bench-datasets/wiktionary/**/*.parquet')",
      'GROUP BY language;',
    ],
  },
}

export default wiktionary
