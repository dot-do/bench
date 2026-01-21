/**
 * IMDb Dataset
 *
 * IMDb is the world's most popular database for movies, TV shows, and celebrities.
 * The non-commercial datasets contain essential information about titles, names,
 * ratings, and principal cast/crew members.
 *
 * Characteristics:
 * - ~10M titles (movies, TV shows, episodes)
 * - ~12M names (actors, directors, writers, etc.)
 * - ~1.4M rated titles with aggregate ratings
 * - ~60M principal cast/crew relationships
 *
 * Excellent for:
 * - Many-to-many relationship queries
 * - Text search on titles and names
 * - Rating aggregations and analytics
 * - Genre and category analysis
 * - Temporal analysis (release years, career spans)
 *
 * Best suited for:
 * - DuckDB (analytical queries, aggregations)
 * - PostgreSQL (full-text search, complex joins)
 * - SQLite (local development, embedded use)
 */

import type { DatasetConfig, BenchmarkQuery, DatabaseType } from '../analytics'

/**
 * IMDb-specific configuration
 */
export interface IMDbConfig extends DatasetConfig {
  /** IMDb data file URLs */
  dataFiles: IMDbDataFile[]
  /** Entity statistics */
  entityStats: Record<string, string>
  /** Update frequency */
  updateFrequency: string
}

/**
 * IMDb data file definition
 */
interface IMDbDataFile {
  name: string
  description: string
  url: string
  size: string
  columns: string[]
}

/**
 * IMDb benchmark queries
 */
const imdbQueries: BenchmarkQuery[] = [
  // Point lookups
  {
    id: 'imdb-lookup-1',
    name: 'Title lookup by ID',
    description: 'Direct lookup of a title by its tconst identifier',
    complexity: 'simple',
    sql: `SELECT *
FROM title_basics
WHERE tconst = 'tt0111161'  -- The Shawshank Redemption`,
    benchmarks: ['point-lookup', 'primary-key'],
    expectedResults: { rowCount: 1, columns: ['tconst', 'titleType', 'primaryTitle'] },
  },
  {
    id: 'imdb-lookup-2',
    name: 'Person lookup by ID',
    description: 'Direct lookup of a person by their nconst identifier',
    complexity: 'simple',
    sql: `SELECT *
FROM name_basics
WHERE nconst = 'nm0000151'  -- Morgan Freeman`,
    benchmarks: ['point-lookup', 'primary-key'],
  },
  {
    id: 'imdb-lookup-3',
    name: 'Rating lookup',
    description: 'Get rating for a specific title',
    complexity: 'simple',
    sql: `SELECT tb.primaryTitle, tr.averageRating, tr.numVotes
FROM title_basics tb
JOIN title_ratings tr ON tr.tconst = tb.tconst
WHERE tb.tconst = 'tt0111161'`,
    benchmarks: ['point-lookup', 'simple-join'],
  },

  // Text search queries
  {
    id: 'imdb-search-1',
    name: 'Title search by name',
    description: 'Find titles matching a search term',
    complexity: 'simple',
    sql: `SELECT tconst, primaryTitle, startYear, titleType
FROM title_basics
WHERE primaryTitle LIKE '%Godfather%'
  AND titleType = 'movie'
ORDER BY startYear
LIMIT 20`,
    benchmarks: ['text-search', 'like-filter'],
  },
  {
    id: 'imdb-search-2',
    name: 'Actor search by name',
    description: 'Find actors/actresses by name',
    complexity: 'simple',
    sql: `SELECT nconst, primaryName, birthYear, primaryProfession
FROM name_basics
WHERE primaryName LIKE '%Tom Hanks%'
LIMIT 20`,
    benchmarks: ['text-search', 'like-filter'],
  },

  // Range scans
  {
    id: 'imdb-range-1',
    name: 'Movies by year range',
    description: 'Find movies released in a specific decade',
    complexity: 'moderate',
    sql: `SELECT tconst, primaryTitle, startYear, genres
FROM title_basics
WHERE titleType = 'movie'
  AND startYear >= 2000
  AND startYear < 2010
ORDER BY startYear
LIMIT 1000`,
    benchmarks: ['range-scan', 'composite-filter'],
  },
  {
    id: 'imdb-range-2',
    name: 'Top rated movies by year',
    description: 'Top rated movies in a specific year',
    complexity: 'moderate',
    sql: `SELECT tb.primaryTitle, tb.startYear, tr.averageRating, tr.numVotes
FROM title_basics tb
JOIN title_ratings tr ON tr.tconst = tb.tconst
WHERE tb.titleType = 'movie'
  AND tb.startYear = 2023
  AND tr.numVotes >= 10000
ORDER BY tr.averageRating DESC
LIMIT 50`,
    benchmarks: ['range-scan', 'join', 'order-by'],
  },
  {
    id: 'imdb-range-3',
    name: 'TV series episodes',
    description: 'Get all episodes of a TV series',
    complexity: 'moderate',
    sql: `SELECT te.seasonNumber, te.episodeNumber, tb.primaryTitle
FROM title_episode te
JOIN title_basics tb ON tb.tconst = te.tconst
WHERE te.parentTconst = 'tt0903747'  -- Breaking Bad
ORDER BY te.seasonNumber, te.episodeNumber`,
    benchmarks: ['range-scan', 'join', 'ordered-results'],
  },

  // Aggregations
  {
    id: 'imdb-agg-1',
    name: 'Movies per year',
    description: 'Count movies released per year',
    complexity: 'moderate',
    sql: `SELECT startYear, COUNT(*) as movie_count
FROM title_basics
WHERE titleType = 'movie'
  AND startYear IS NOT NULL
  AND startYear >= 1900
GROUP BY startYear
ORDER BY startYear`,
    benchmarks: ['aggregation', 'group-by'],
  },
  {
    id: 'imdb-agg-2',
    name: 'Genre popularity',
    description: 'Most common genres by title count',
    complexity: 'moderate',
    sql: `SELECT
  UNNEST(string_to_array(genres, ',')) as genre,
  COUNT(*) as title_count
FROM title_basics
WHERE genres IS NOT NULL
  AND genres != '\\N'
GROUP BY genre
ORDER BY title_count DESC
LIMIT 20`,
    benchmarks: ['aggregation', 'array-unnest', 'group-by'],
  },
  {
    id: 'imdb-agg-3',
    name: 'Average rating by genre',
    description: 'Average rating for each genre',
    complexity: 'moderate',
    sql: `SELECT
  UNNEST(string_to_array(tb.genres, ',')) as genre,
  ROUND(AVG(tr.averageRating), 2) as avg_rating,
  COUNT(*) as title_count,
  SUM(tr.numVotes) as total_votes
FROM title_basics tb
JOIN title_ratings tr ON tr.tconst = tb.tconst
WHERE tb.genres IS NOT NULL
  AND tb.genres != '\\N'
GROUP BY genre
HAVING COUNT(*) >= 100
ORDER BY avg_rating DESC`,
    benchmarks: ['aggregation', 'join-groupby', 'having'],
  },
  {
    id: 'imdb-agg-4',
    name: 'Rating distribution',
    description: 'Distribution of ratings (histogram)',
    complexity: 'simple',
    sql: `SELECT
  FLOOR(averageRating) as rating_bucket,
  COUNT(*) as title_count
FROM title_ratings
GROUP BY rating_bucket
ORDER BY rating_bucket`,
    benchmarks: ['aggregation', 'bucketing'],
  },

  // Complex joins - filmography
  {
    id: 'imdb-join-1',
    name: 'Actor filmography',
    description: 'Get complete filmography for an actor',
    complexity: 'complex',
    sql: `SELECT
  tb.primaryTitle,
  tb.startYear,
  tb.titleType,
  tp.category,
  tp.characters,
  tr.averageRating
FROM title_principals tp
JOIN title_basics tb ON tb.tconst = tp.tconst
LEFT JOIN title_ratings tr ON tr.tconst = tp.tconst
WHERE tp.nconst = 'nm0000151'  -- Morgan Freeman
ORDER BY tb.startYear DESC`,
    benchmarks: ['multi-join', 'filmography'],
  },
  {
    id: 'imdb-join-2',
    name: 'Movie cast and crew',
    description: 'Get full cast and crew for a movie',
    complexity: 'complex',
    sql: `SELECT
  nb.primaryName,
  tp.category,
  tp.job,
  tp.characters,
  tp.ordering
FROM title_principals tp
JOIN name_basics nb ON nb.nconst = tp.nconst
WHERE tp.tconst = 'tt0111161'  -- The Shawshank Redemption
ORDER BY tp.ordering`,
    benchmarks: ['join', 'cast-crew'],
  },
  {
    id: 'imdb-join-3',
    name: 'Directors and writers',
    description: 'Get directors and writers for a title',
    complexity: 'moderate',
    sql: `SELECT
  tc.tconst,
  tb.primaryTitle,
  d.primaryName as director,
  w.primaryName as writer
FROM title_crew tc
JOIN title_basics tb ON tb.tconst = tc.tconst
LEFT JOIN name_basics d ON d.nconst = SPLIT_PART(tc.directors, ',', 1)
LEFT JOIN name_basics w ON w.nconst = SPLIT_PART(tc.writers, ',', 1)
WHERE tc.tconst = 'tt0111161'`,
    benchmarks: ['join', 'string-split'],
  },

  // Complex analytical queries
  {
    id: 'imdb-complex-1',
    name: 'Most prolific actors',
    description: 'Actors with most movie appearances',
    complexity: 'complex',
    sql: `SELECT
  nb.nconst,
  nb.primaryName,
  COUNT(DISTINCT tp.tconst) as movie_count,
  MIN(tb.startYear) as first_movie,
  MAX(tb.startYear) as last_movie,
  ROUND(AVG(tr.averageRating), 2) as avg_movie_rating
FROM name_basics nb
JOIN title_principals tp ON tp.nconst = nb.nconst
JOIN title_basics tb ON tb.tconst = tp.tconst AND tb.titleType = 'movie'
LEFT JOIN title_ratings tr ON tr.tconst = tp.tconst
WHERE tp.category IN ('actor', 'actress')
GROUP BY nb.nconst, nb.primaryName
HAVING COUNT(DISTINCT tp.tconst) >= 50
ORDER BY movie_count DESC
LIMIT 100`,
    benchmarks: ['multi-join', 'aggregation', 'prolific-analysis'],
  },
  {
    id: 'imdb-complex-2',
    name: 'Co-star analysis',
    description: 'Find actors who frequently work together',
    complexity: 'expert',
    sql: `WITH actor_pairs AS (
  SELECT
    tp1.nconst as actor1,
    tp2.nconst as actor2,
    tp1.tconst
  FROM title_principals tp1
  JOIN title_principals tp2 ON tp1.tconst = tp2.tconst
    AND tp1.nconst < tp2.nconst
  WHERE tp1.category IN ('actor', 'actress')
    AND tp2.category IN ('actor', 'actress')
)
SELECT
  nb1.primaryName as actor1_name,
  nb2.primaryName as actor2_name,
  COUNT(*) as movies_together
FROM actor_pairs ap
JOIN name_basics nb1 ON nb1.nconst = ap.actor1
JOIN name_basics nb2 ON nb2.nconst = ap.actor2
GROUP BY nb1.primaryName, nb2.primaryName
HAVING COUNT(*) >= 5
ORDER BY movies_together DESC
LIMIT 50`,
    benchmarks: ['self-join', 'co-occurrence', 'cte'],
  },
  {
    id: 'imdb-complex-3',
    name: 'Director career analysis',
    description: 'Analyze director careers over time',
    complexity: 'expert',
    sql: `WITH director_movies AS (
  SELECT
    SPLIT_PART(tc.directors, ',', 1) as director_nconst,
    tc.tconst,
    tb.startYear,
    tr.averageRating,
    tr.numVotes
  FROM title_crew tc
  JOIN title_basics tb ON tb.tconst = tc.tconst
    AND tb.titleType = 'movie'
  LEFT JOIN title_ratings tr ON tr.tconst = tc.tconst
  WHERE tc.directors IS NOT NULL
    AND tc.directors != '\\N'
)
SELECT
  nb.primaryName as director,
  COUNT(*) as movie_count,
  MIN(dm.startYear) as career_start,
  MAX(dm.startYear) as career_end,
  MAX(dm.startYear) - MIN(dm.startYear) as career_span,
  ROUND(AVG(dm.averageRating), 2) as avg_rating,
  SUM(dm.numVotes) as total_votes
FROM director_movies dm
JOIN name_basics nb ON nb.nconst = dm.director_nconst
GROUP BY nb.primaryName
HAVING COUNT(*) >= 10
ORDER BY avg_rating DESC
LIMIT 50`,
    benchmarks: ['cte', 'career-analysis', 'multi-aggregate'],
  },
  {
    id: 'imdb-complex-4',
    name: 'Franchise analysis',
    description: 'Analyze movie franchises/series',
    complexity: 'expert',
    sql: `WITH franchise_candidates AS (
  SELECT
    tb.primaryTitle,
    tb.tconst,
    tb.startYear,
    tr.averageRating,
    tr.numVotes,
    -- Extract potential franchise name (first word or phrase before colon/number)
    REGEXP_REPLACE(tb.primaryTitle, ':\\s.*|\\s[0-9]+.*|\\s(Part|Episode|Chapter)\\s.*', '', 'i') as franchise_name
  FROM title_basics tb
  JOIN title_ratings tr ON tr.tconst = tb.tconst
  WHERE tb.titleType = 'movie'
    AND tr.numVotes >= 10000
)
SELECT
  franchise_name,
  COUNT(*) as movie_count,
  MIN(startYear) as first_movie,
  MAX(startYear) as last_movie,
  ROUND(AVG(averageRating), 2) as avg_rating,
  SUM(numVotes) as total_votes
FROM franchise_candidates
GROUP BY franchise_name
HAVING COUNT(*) >= 3
ORDER BY total_votes DESC
LIMIT 50`,
    benchmarks: ['regex', 'cte', 'franchise-analysis'],
  },

  // Full-text and fuzzy search
  {
    id: 'imdb-fts-1',
    name: 'Full-text title search',
    description: 'Search titles using full-text index',
    complexity: 'moderate',
    sql: {
      postgres: `SELECT tconst, primaryTitle, startYear, titleType
FROM title_basics
WHERE to_tsvector('english', primaryTitle) @@ to_tsquery('english', 'star & wars')
  AND titleType = 'movie'
ORDER BY startYear DESC
LIMIT 20`,
      duckdb: `SELECT tconst, primaryTitle, startYear, titleType
FROM title_basics
WHERE primaryTitle ILIKE '%star%' AND primaryTitle ILIKE '%wars%'
  AND titleType = 'movie'
ORDER BY startYear DESC
LIMIT 20`,
      sqlite: `SELECT tconst, primaryTitle, startYear, titleType
FROM title_basics
WHERE primaryTitle LIKE '%star%' AND primaryTitle LIKE '%wars%'
  AND titleType = 'movie'
ORDER BY startYear DESC
LIMIT 20`,
    },
    benchmarks: ['full-text-search', 'text-matching'],
  },
]

/**
 * IMDb dataset configuration
 */
export const imdb: IMDbConfig = {
  id: 'imdb',
  name: 'IMDb Dataset',
  description: `IMDb non-commercial datasets containing ~10M titles, ~12M names, and ~60M
cast/crew relationships. Excellent for testing many-to-many relationships, text search,
rating aggregations, and genre analysis.`,
  category: 'full-text',
  size: 'large',
  rowCount: '~10M titles, ~12M names, ~60M principals',
  compressedSize: '~1.5GB (gzipped TSV)',
  uncompressedSize: '~3-4GB',
  sourceUrl: 'https://datasets.imdbws.com/',
  license: 'IMDb Non-Commercial License',
  suitedFor: ['duckdb', 'postgres', 'sqlite', 'clickhouse'],
  updateFrequency: 'Daily',

  dataFiles: [
    {
      name: 'title.basics.tsv.gz',
      description: 'Basic title information',
      url: 'https://datasets.imdbws.com/title.basics.tsv.gz',
      size: '~150MB',
      columns: ['tconst', 'titleType', 'primaryTitle', 'originalTitle', 'isAdult', 'startYear', 'endYear', 'runtimeMinutes', 'genres'],
    },
    {
      name: 'name.basics.tsv.gz',
      description: 'Basic name information',
      url: 'https://datasets.imdbws.com/name.basics.tsv.gz',
      size: '~250MB',
      columns: ['nconst', 'primaryName', 'birthYear', 'deathYear', 'primaryProfession', 'knownForTitles'],
    },
    {
      name: 'title.ratings.tsv.gz',
      description: 'IMDb ratings and votes',
      url: 'https://datasets.imdbws.com/title.ratings.tsv.gz',
      size: '~7MB',
      columns: ['tconst', 'averageRating', 'numVotes'],
    },
    {
      name: 'title.principals.tsv.gz',
      description: 'Principal cast/crew for titles',
      url: 'https://datasets.imdbws.com/title.principals.tsv.gz',
      size: '~500MB',
      columns: ['tconst', 'ordering', 'nconst', 'category', 'job', 'characters'],
    },
    {
      name: 'title.crew.tsv.gz',
      description: 'Director and writer information',
      url: 'https://datasets.imdbws.com/title.crew.tsv.gz',
      size: '~70MB',
      columns: ['tconst', 'directors', 'writers'],
    },
    {
      name: 'title.episode.tsv.gz',
      description: 'TV episode information',
      url: 'https://datasets.imdbws.com/title.episode.tsv.gz',
      size: '~50MB',
      columns: ['tconst', 'parentTconst', 'seasonNumber', 'episodeNumber'],
    },
  ],

  entityStats: {
    titles: '~10M total',
    movies: '~600K',
    tvSeries: '~250K',
    tvEpisodes: '~8M',
    names: '~12M',
    ratings: '~1.4M rated titles',
    principals: '~60M cast/crew relationships',
  },

  downloadConfigs: {
    local: {
      urls: [
        'https://datasets.imdbws.com/title.basics.tsv.gz',
        'https://datasets.imdbws.com/name.basics.tsv.gz',
        'https://datasets.imdbws.com/title.ratings.tsv.gz',
        'https://datasets.imdbws.com/title.principals.tsv.gz',
      ],
      size: '~1GB compressed',
      rowCount: '~10M titles, ~12M names',
      instructions: [
        '# Download IMDb datasets',
        'mkdir -p imdb_data && cd imdb_data',
        'curl -O https://datasets.imdbws.com/title.basics.tsv.gz',
        'curl -O https://datasets.imdbws.com/name.basics.tsv.gz',
        'curl -O https://datasets.imdbws.com/title.ratings.tsv.gz',
        'curl -O https://datasets.imdbws.com/title.principals.tsv.gz',
        'curl -O https://datasets.imdbws.com/title.crew.tsv.gz',
        'curl -O https://datasets.imdbws.com/title.episode.tsv.gz',
        '',
        '# Decompress files',
        'gunzip *.gz',
      ],
      setupCommands: [
        '# Load into DuckDB',
        "duckdb imdb.db -c \"",
        "CREATE TABLE title_basics AS SELECT * FROM read_csv('title.basics.tsv', delim='\\t', header=true, nullstr='\\\\N');",
        "CREATE TABLE name_basics AS SELECT * FROM read_csv('name.basics.tsv', delim='\\t', header=true, nullstr='\\\\N');",
        "CREATE TABLE title_ratings AS SELECT * FROM read_csv('title.ratings.tsv', delim='\\t', header=true, nullstr='\\\\N');",
        "CREATE TABLE title_principals AS SELECT * FROM read_csv('title.principals.tsv', delim='\\t', header=true, nullstr='\\\\N');",
        "\"",
      ],
    },
    development: {
      urls: [
        'https://datasets.imdbws.com/title.basics.tsv.gz',
        'https://datasets.imdbws.com/name.basics.tsv.gz',
        'https://datasets.imdbws.com/title.ratings.tsv.gz',
        'https://datasets.imdbws.com/title.principals.tsv.gz',
      ],
      size: '~500MB (movies only)',
      rowCount: '~600K movies',
      instructions: [
        '# Download and filter to movies only',
        'curl https://datasets.imdbws.com/title.basics.tsv.gz | gunzip | \\',
        "  awk -F'\\t' 'NR==1 || $2==\"movie\"' > title.basics.movies.tsv",
        '',
        '# Filter principals to those movies',
        "cut -f1 title.basics.movies.tsv > movie_ids.txt",
        'curl https://datasets.imdbws.com/title.principals.tsv.gz | gunzip | \\',
        "  awk -F'\\t' 'NR==1 || FNR==NR{a[$1];next} $1 in a' movie_ids.txt - > title.principals.movies.tsv",
      ],
      setupCommands: [
        '# Load filtered data into DuckDB',
        'duckdb imdb_movies.db < scripts/load_imdb_movies.sql',
      ],
    },
    production: {
      urls: [
        'https://datasets.imdbws.com/title.basics.tsv.gz',
        'https://datasets.imdbws.com/name.basics.tsv.gz',
        'https://datasets.imdbws.com/title.ratings.tsv.gz',
        'https://datasets.imdbws.com/title.principals.tsv.gz',
        'https://datasets.imdbws.com/title.crew.tsv.gz',
        'https://datasets.imdbws.com/title.episode.tsv.gz',
        'https://datasets.imdbws.com/title.akas.tsv.gz',
      ],
      size: '~1.5GB compressed, ~4GB uncompressed',
      rowCount: '~10M titles, ~12M names, ~60M principals',
      instructions: [
        '# Download all IMDb datasets',
        'for f in title.basics title.akas title.crew title.episode title.principals title.ratings name.basics; do',
        '  curl -O "https://datasets.imdbws.com/${f}.tsv.gz"',
        'done',
        '',
        '# Decompress all files',
        'gunzip *.gz',
      ],
      setupCommands: [
        '# Create optimized database with indexes',
        'duckdb imdb_full.db < scripts/load_imdb_full.sql',
        'duckdb imdb_full.db < scripts/create_imdb_indexes.sql',
      ],
    },
  },

  schema: {
    tableName: 'title_basics',
    columns: [
      { name: 'tconst', type: 'VARCHAR(12)', nullable: false, description: 'Title identifier (tt0000001 format)' },
      { name: 'titleType', type: 'VARCHAR(20)', nullable: false, description: 'Type: movie, tvSeries, tvEpisode, etc.' },
      { name: 'primaryTitle', type: 'TEXT', nullable: true, description: 'Primary title used for display' },
      { name: 'originalTitle', type: 'TEXT', nullable: true, description: 'Original title in original language' },
      { name: 'isAdult', type: 'BOOLEAN', nullable: false, description: 'Adult content flag' },
      { name: 'startYear', type: 'INTEGER', nullable: true, description: 'Release year or series start year' },
      { name: 'endYear', type: 'INTEGER', nullable: true, description: 'Series end year (NULL for movies)' },
      { name: 'runtimeMinutes', type: 'INTEGER', nullable: true, description: 'Runtime in minutes' },
      { name: 'genres', type: 'TEXT', nullable: true, description: 'Comma-separated list of genres' },
    ],
    primaryKey: ['tconst'],
    indexes: [
      { name: 'idx_title_type', columns: ['titleType'], type: 'btree', description: 'Filter by title type' },
      { name: 'idx_title_year', columns: ['startYear'], type: 'btree', description: 'Filter/sort by year' },
      { name: 'idx_title_primary', columns: ['primaryTitle'], type: 'btree', description: 'Title text search' },
    ],
    createTableSQL: {
      duckdb: `-- Title basics
CREATE TABLE title_basics (
  tconst VARCHAR(12) PRIMARY KEY,
  titleType VARCHAR(20) NOT NULL,
  primaryTitle TEXT,
  originalTitle TEXT,
  isAdult BOOLEAN,
  startYear INTEGER,
  endYear INTEGER,
  runtimeMinutes INTEGER,
  genres TEXT
);

-- Name basics
CREATE TABLE name_basics (
  nconst VARCHAR(12) PRIMARY KEY,
  primaryName TEXT NOT NULL,
  birthYear INTEGER,
  deathYear INTEGER,
  primaryProfession TEXT,
  knownForTitles TEXT
);

-- Title ratings
CREATE TABLE title_ratings (
  tconst VARCHAR(12) PRIMARY KEY,
  averageRating DECIMAL(3,1) NOT NULL,
  numVotes INTEGER NOT NULL
);

-- Title principals (cast/crew)
CREATE TABLE title_principals (
  tconst VARCHAR(12) NOT NULL,
  ordering INTEGER NOT NULL,
  nconst VARCHAR(12) NOT NULL,
  category VARCHAR(50),
  job TEXT,
  characters TEXT,
  PRIMARY KEY (tconst, ordering)
);

-- Title crew (directors/writers)
CREATE TABLE title_crew (
  tconst VARCHAR(12) PRIMARY KEY,
  directors TEXT,
  writers TEXT
);

-- Title episodes
CREATE TABLE title_episode (
  tconst VARCHAR(12) PRIMARY KEY,
  parentTconst VARCHAR(12) NOT NULL,
  seasonNumber INTEGER,
  episodeNumber INTEGER
);

-- Indexes
CREATE INDEX idx_principals_nconst ON title_principals(nconst);
CREATE INDEX idx_principals_category ON title_principals(category);
CREATE INDEX idx_episode_parent ON title_episode(parentTconst);
CREATE INDEX idx_title_type ON title_basics(titleType);
CREATE INDEX idx_title_year ON title_basics(startYear);`,

      postgres: `-- Title basics with full-text search
CREATE TABLE title_basics (
  tconst VARCHAR(12) PRIMARY KEY,
  titleType VARCHAR(20) NOT NULL,
  primaryTitle TEXT,
  originalTitle TEXT,
  isAdult BOOLEAN,
  startYear INTEGER,
  endYear INTEGER,
  runtimeMinutes INTEGER,
  genres TEXT,
  title_tsv TSVECTOR GENERATED ALWAYS AS (to_tsvector('english', COALESCE(primaryTitle, ''))) STORED
);

CREATE TABLE name_basics (
  nconst VARCHAR(12) PRIMARY KEY,
  primaryName TEXT NOT NULL,
  birthYear INTEGER,
  deathYear INTEGER,
  primaryProfession TEXT,
  knownForTitles TEXT,
  name_tsv TSVECTOR GENERATED ALWAYS AS (to_tsvector('english', COALESCE(primaryName, ''))) STORED
);

CREATE TABLE title_ratings (
  tconst VARCHAR(12) PRIMARY KEY REFERENCES title_basics(tconst),
  averageRating DECIMAL(3,1) NOT NULL,
  numVotes INTEGER NOT NULL
);

CREATE TABLE title_principals (
  tconst VARCHAR(12) NOT NULL REFERENCES title_basics(tconst),
  ordering INTEGER NOT NULL,
  nconst VARCHAR(12) NOT NULL REFERENCES name_basics(nconst),
  category VARCHAR(50),
  job TEXT,
  characters TEXT,
  PRIMARY KEY (tconst, ordering)
);

CREATE TABLE title_crew (
  tconst VARCHAR(12) PRIMARY KEY REFERENCES title_basics(tconst),
  directors TEXT,
  writers TEXT
);

CREATE TABLE title_episode (
  tconst VARCHAR(12) PRIMARY KEY REFERENCES title_basics(tconst),
  parentTconst VARCHAR(12) NOT NULL REFERENCES title_basics(tconst),
  seasonNumber INTEGER,
  episodeNumber INTEGER
);

-- Indexes
CREATE INDEX idx_title_tsv ON title_basics USING GIN(title_tsv);
CREATE INDEX idx_name_tsv ON name_basics USING GIN(name_tsv);
CREATE INDEX idx_principals_nconst ON title_principals(nconst);
CREATE INDEX idx_title_type ON title_basics(titleType);
CREATE INDEX idx_title_year ON title_basics(startYear);
CREATE INDEX idx_episode_parent ON title_episode(parentTconst);`,

      sqlite: `-- Title basics
CREATE TABLE title_basics (
  tconst TEXT PRIMARY KEY,
  titleType TEXT NOT NULL,
  primaryTitle TEXT,
  originalTitle TEXT,
  isAdult INTEGER,
  startYear INTEGER,
  endYear INTEGER,
  runtimeMinutes INTEGER,
  genres TEXT
);

CREATE TABLE name_basics (
  nconst TEXT PRIMARY KEY,
  primaryName TEXT NOT NULL,
  birthYear INTEGER,
  deathYear INTEGER,
  primaryProfession TEXT,
  knownForTitles TEXT
);

CREATE TABLE title_ratings (
  tconst TEXT PRIMARY KEY,
  averageRating REAL NOT NULL,
  numVotes INTEGER NOT NULL
);

CREATE TABLE title_principals (
  tconst TEXT NOT NULL,
  ordering INTEGER NOT NULL,
  nconst TEXT NOT NULL,
  category TEXT,
  job TEXT,
  characters TEXT,
  PRIMARY KEY (tconst, ordering)
);

CREATE TABLE title_crew (
  tconst TEXT PRIMARY KEY,
  directors TEXT,
  writers TEXT
);

CREATE TABLE title_episode (
  tconst TEXT PRIMARY KEY,
  parentTconst TEXT NOT NULL,
  seasonNumber INTEGER,
  episodeNumber INTEGER
);

-- Indexes
CREATE INDEX idx_principals_nconst ON title_principals(nconst);
CREATE INDEX idx_title_type ON title_basics(titleType);
CREATE INDEX idx_title_year ON title_basics(startYear);
CREATE INDEX idx_episode_parent ON title_episode(parentTconst);

-- FTS5 for full-text search
CREATE VIRTUAL TABLE title_fts USING fts5(primaryTitle, content=title_basics, content_rowid=rowid);
CREATE VIRTUAL TABLE name_fts USING fts5(primaryName, content=name_basics, content_rowid=rowid);`,

      clickhouse: `-- Optimized for analytics
CREATE TABLE title_basics (
  tconst String,
  titleType LowCardinality(String),
  primaryTitle Nullable(String),
  originalTitle Nullable(String),
  isAdult UInt8,
  startYear Nullable(UInt16),
  endYear Nullable(UInt16),
  runtimeMinutes Nullable(UInt16),
  genres Nullable(String)
) ENGINE = MergeTree()
ORDER BY (titleType, startYear, tconst);

CREATE TABLE name_basics (
  nconst String,
  primaryName String,
  birthYear Nullable(UInt16),
  deathYear Nullable(UInt16),
  primaryProfession Nullable(String),
  knownForTitles Nullable(String)
) ENGINE = MergeTree()
ORDER BY nconst;

CREATE TABLE title_ratings (
  tconst String,
  averageRating Float32,
  numVotes UInt32
) ENGINE = MergeTree()
ORDER BY tconst;

CREATE TABLE title_principals (
  tconst String,
  ordering UInt8,
  nconst String,
  category LowCardinality(Nullable(String)),
  job Nullable(String),
  characters Nullable(String)
) ENGINE = MergeTree()
ORDER BY (nconst, tconst, ordering);`,

      db4: `-- Same as SQLite`,
      evodb: `-- Same as SQLite`,
    },
  },

  queries: imdbQueries,

  performanceExpectations: {
    duckdb: {
      loadTime: '~2 minutes',
      simpleQueryLatency: '<50ms',
      complexQueryLatency: '100ms-2s',
      storageEfficiency: 'Excellent',
      concurrency: 'Good',
      notes: [
        'Best for analytical queries',
        'Vectorized string operations',
        'Efficient aggregations',
        'Good JOIN performance',
      ],
    },
    postgres: {
      loadTime: '~10 minutes',
      simpleQueryLatency: '<20ms',
      complexQueryLatency: '100ms-5s',
      storageEfficiency: 'Good',
      concurrency: 'Excellent',
      notes: [
        'Best full-text search with pg_trgm',
        'Excellent for complex JOINs',
        'Good MVCC for concurrent access',
        'Consider partitioning title_principals',
      ],
    },
    sqlite: {
      loadTime: '~15 minutes',
      simpleQueryLatency: '10-100ms',
      complexQueryLatency: '500ms-10s',
      storageEfficiency: 'Moderate',
      concurrency: 'Limited',
      notes: [
        'Good for embedded/local use',
        'FTS5 for text search',
        'Single-writer limitation',
        'Consider smaller subsets',
      ],
    },
    clickhouse: {
      loadTime: '~1 minute',
      simpleQueryLatency: '<50ms',
      complexQueryLatency: '50ms-1s',
      storageEfficiency: 'Excellent',
      concurrency: 'Excellent',
      notes: [
        'Best for aggregation queries',
        'Fast GROUP BY operations',
        'LowCardinality for category columns',
        'Less optimal for point lookups',
      ],
    },
    db4: {
      loadTime: '~15 minutes',
      simpleQueryLatency: '10-100ms',
      complexQueryLatency: '500ms-10s',
      storageEfficiency: 'Moderate',
      concurrency: 'Limited',
      notes: ['Same as SQLite', 'Good for edge deployment'],
    },
    evodb: {
      loadTime: '~15 minutes',
      simpleQueryLatency: '10-100ms',
      complexQueryLatency: '500ms-10s',
      storageEfficiency: 'Moderate',
      concurrency: 'Limited',
      notes: ['Same as SQLite'],
    },
  },

  r2Config: {
    bucketName: 'bench-datasets',
    pathPrefix: 'imdb/',
    format: 'parquet',
    compression: 'zstd',
    partitioning: {
      columns: ['titleType', 'startYear'],
      format: 'titles/type={titleType}/year={startYear}/*.parquet',
    },
    uploadInstructions: [
      '# Convert TSV to partitioned Parquet',
      'duckdb -c "',
      "  COPY (SELECT * FROM read_csv('title.basics.tsv', delim='\\t', header=true, nullstr='\\\\N'))",
      "  TO 'imdb_parquet/titles' (FORMAT PARQUET, PARTITION_BY (titleType), COMPRESSION 'zstd');",
      '"',
      '',
      '# Upload to R2',
      'wrangler r2 object put bench-datasets/imdb/ --file=imdb_parquet/ --recursive',
    ],
    duckdbInstructions: [
      '-- Query titles from R2 with partition pruning',
      "SELECT * FROM read_parquet('s3://bench-datasets/imdb/titles/type=movie/*.parquet')",
      "WHERE startYear = 2023",
      'LIMIT 100;',
      '',
      '-- Join with ratings',
      "SELECT t.primaryTitle, r.averageRating FROM read_parquet('s3://bench-datasets/imdb/titles/**/*.parquet') t",
      "JOIN read_parquet('s3://bench-datasets/imdb/ratings/*.parquet') r ON r.tconst = t.tconst",
      'ORDER BY r.averageRating DESC LIMIT 50;',
    ],
  },
}

export default imdb
