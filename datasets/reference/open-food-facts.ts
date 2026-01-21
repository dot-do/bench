/**
 * Open Food Facts Dataset
 *
 * Open Food Facts is a collaborative, free, and open database of food products
 * from around the world. It contains detailed nutritional information, ingredients,
 * allergens, labels, and packaging data for millions of products.
 *
 * Characteristics:
 * - ~3.5M food products worldwide
 * - Rich nutritional data (100+ nutrition facts per product)
 * - Multi-language product names and ingredients
 * - Barcode-based lookup (EAN/UPC)
 * - Crowdsourced with quality scoring
 *
 * Excellent for:
 * - Full-text search on product names and ingredients
 * - JSON document handling (complex nested data)
 * - Faceted search (brands, categories, origins)
 * - Nutrition analysis and aggregations
 * - Barcode/SKU lookup patterns
 *
 * Best suited for:
 * - DuckDB (analytical queries, JSON support)
 * - PostgreSQL (full-text, JSONB operations)
 * - SQLite (embedded use, FTS5)
 * - Elasticsearch (if pure search workload)
 */

import type { DatasetConfig, BenchmarkQuery, DatabaseType } from '../analytics'

/**
 * Open Food Facts-specific configuration
 */
export interface OpenFoodFactsConfig extends DatasetConfig {
  /** Available export formats */
  exportFormats: OpenFoodFactsExport[]
  /** Country-specific subsets */
  countrySubsets: string[]
  /** Data quality tiers */
  qualityTiers: Record<string, string>
  /** API endpoint */
  apiEndpoint: string
}

/**
 * Open Food Facts export format
 */
interface OpenFoodFactsExport {
  format: string
  url: string
  size: string
  description: string
}

/**
 * Open Food Facts benchmark queries
 */
const openFoodFactsQueries: BenchmarkQuery[] = [
  // Point lookups by barcode
  {
    id: 'off-lookup-1',
    name: 'Product lookup by barcode',
    description: 'Direct lookup of a product by its barcode (EAN/UPC)',
    complexity: 'simple',
    sql: `SELECT code, product_name, brands, categories, nutriscore_grade
FROM products
WHERE code = '3017620422003'  -- Nutella`,
    benchmarks: ['point-lookup', 'barcode-search'],
    expectedResults: { rowCount: 1, columns: ['code', 'product_name', 'brands'] },
  },
  {
    id: 'off-lookup-2',
    name: 'Product lookup by multiple barcodes',
    description: 'Batch lookup of products by barcode array',
    complexity: 'simple',
    sql: `SELECT code, product_name, brands, nutriscore_grade
FROM products
WHERE code IN ('3017620422003', '5449000000996', '8076809513388')
ORDER BY code`,
    benchmarks: ['batch-lookup', 'in-clause'],
  },

  // Text search queries
  {
    id: 'off-search-1',
    name: 'Product search by name',
    description: 'Search products by name text',
    complexity: 'simple',
    sql: `SELECT code, product_name, brands, nutriscore_grade
FROM products
WHERE product_name ILIKE '%chocolate%'
  AND countries_tags LIKE '%france%'
ORDER BY completeness DESC
LIMIT 50`,
    benchmarks: ['text-search', 'like-filter'],
  },
  {
    id: 'off-search-2',
    name: 'Ingredient search',
    description: 'Find products containing specific ingredients',
    complexity: 'moderate',
    sql: `SELECT code, product_name, brands, ingredients_text
FROM products
WHERE ingredients_text ILIKE '%palm oil%'
  AND nutriscore_grade IN ('d', 'e')
LIMIT 100`,
    benchmarks: ['text-search', 'ingredient-analysis'],
  },
  {
    id: 'off-search-3',
    name: 'Allergen search',
    description: 'Find products with specific allergens',
    complexity: 'moderate',
    sql: `SELECT code, product_name, brands, allergens_tags
FROM products
WHERE allergens_tags LIKE '%gluten%'
  OR allergens_tags LIKE '%en:gluten%'
LIMIT 100`,
    benchmarks: ['text-search', 'allergen-filter'],
  },

  // Brand and category analysis
  {
    id: 'off-facet-1',
    name: 'Products by brand',
    description: 'Find all products from a specific brand',
    complexity: 'simple',
    sql: `SELECT code, product_name, categories, nutriscore_grade
FROM products
WHERE brands ILIKE '%nestle%'
ORDER BY completeness DESC
LIMIT 100`,
    benchmarks: ['brand-filter', 'faceted-search'],
  },
  {
    id: 'off-facet-2',
    name: 'Products by category',
    description: 'Browse products in a category hierarchy',
    complexity: 'moderate',
    sql: `SELECT code, product_name, brands, nutriscore_grade
FROM products
WHERE categories_tags LIKE '%en:breakfast-cereals%'
  AND countries_tags LIKE '%united-states%'
ORDER BY popularity_tags DESC NULLS LAST
LIMIT 50`,
    benchmarks: ['category-filter', 'hierarchical-browse'],
  },
  {
    id: 'off-facet-3',
    name: 'Top brands by product count',
    description: 'Rank brands by number of products',
    complexity: 'moderate',
    sql: `SELECT
  brands,
  COUNT(*) as product_count,
  ROUND(AVG(CASE nutriscore_grade
    WHEN 'a' THEN 5 WHEN 'b' THEN 4 WHEN 'c' THEN 3
    WHEN 'd' THEN 2 WHEN 'e' THEN 1 ELSE NULL END), 2) as avg_nutriscore
FROM products
WHERE brands IS NOT NULL
  AND brands != ''
GROUP BY brands
HAVING COUNT(*) >= 50
ORDER BY product_count DESC
LIMIT 50`,
    benchmarks: ['aggregation', 'brand-ranking'],
  },

  // Nutrition analysis
  {
    id: 'off-nutrition-1',
    name: 'Nutrition profile lookup',
    description: 'Get full nutrition data for a product',
    complexity: 'simple',
    sql: `SELECT
  code,
  product_name,
  energy_100g,
  fat_100g,
  saturated_fat_100g,
  carbohydrates_100g,
  sugars_100g,
  fiber_100g,
  proteins_100g,
  salt_100g,
  sodium_100g,
  nutriscore_grade,
  nova_group
FROM products
WHERE code = '3017620422003'`,
    benchmarks: ['point-lookup', 'nutrition-data'],
  },
  {
    id: 'off-nutrition-2',
    name: 'Low sugar products',
    description: 'Find products with low sugar content',
    complexity: 'moderate',
    sql: `SELECT
  code,
  product_name,
  brands,
  sugars_100g,
  nutriscore_grade
FROM products
WHERE sugars_100g IS NOT NULL
  AND sugars_100g < 5
  AND categories_tags LIKE '%en:beverages%'
  AND countries_tags LIKE '%united-states%'
ORDER BY sugars_100g ASC
LIMIT 50`,
    benchmarks: ['range-scan', 'nutrition-filter'],
  },
  {
    id: 'off-nutrition-3',
    name: 'High protein products',
    description: 'Find products with high protein content',
    complexity: 'moderate',
    sql: `SELECT
  code,
  product_name,
  brands,
  proteins_100g,
  energy_100g,
  ROUND(proteins_100g * 4.0 / NULLIF(energy_100g / 4.184, 0) * 100, 1) as protein_pct_calories
FROM products
WHERE proteins_100g IS NOT NULL
  AND proteins_100g > 20
  AND energy_100g > 0
ORDER BY proteins_100g DESC
LIMIT 50`,
    benchmarks: ['range-scan', 'calculated-field'],
  },

  // Aggregations
  {
    id: 'off-agg-1',
    name: 'Nutriscore distribution',
    description: 'Distribution of nutriscore grades',
    complexity: 'simple',
    sql: `SELECT
  nutriscore_grade,
  COUNT(*) as product_count,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
FROM products
WHERE nutriscore_grade IS NOT NULL
  AND nutriscore_grade IN ('a', 'b', 'c', 'd', 'e')
GROUP BY nutriscore_grade
ORDER BY nutriscore_grade`,
    benchmarks: ['aggregation', 'window-function'],
  },
  {
    id: 'off-agg-2',
    name: 'NOVA group distribution',
    description: 'Distribution of food processing levels',
    complexity: 'simple',
    sql: `SELECT
  nova_group,
  CASE nova_group
    WHEN 1 THEN 'Unprocessed'
    WHEN 2 THEN 'Processed culinary ingredients'
    WHEN 3 THEN 'Processed foods'
    WHEN 4 THEN 'Ultra-processed'
    ELSE 'Unknown'
  END as nova_description,
  COUNT(*) as product_count
FROM products
WHERE nova_group IS NOT NULL
GROUP BY nova_group
ORDER BY nova_group`,
    benchmarks: ['aggregation', 'case-expression'],
  },
  {
    id: 'off-agg-3',
    name: 'Average nutrition by category',
    description: 'Nutrition averages for product categories',
    complexity: 'complex',
    sql: `WITH category_unnest AS (
  SELECT
    code,
    UNNEST(string_to_array(categories_tags, ',')) as category,
    energy_100g,
    sugars_100g,
    fat_100g,
    proteins_100g,
    salt_100g
  FROM products
  WHERE categories_tags IS NOT NULL
    AND energy_100g IS NOT NULL
)
SELECT
  TRIM(category) as category,
  COUNT(*) as product_count,
  ROUND(AVG(energy_100g), 0) as avg_energy,
  ROUND(AVG(sugars_100g), 1) as avg_sugars,
  ROUND(AVG(fat_100g), 1) as avg_fat,
  ROUND(AVG(proteins_100g), 1) as avg_proteins,
  ROUND(AVG(salt_100g), 2) as avg_salt
FROM category_unnest
GROUP BY TRIM(category)
HAVING COUNT(*) >= 100
ORDER BY product_count DESC
LIMIT 50`,
    benchmarks: ['aggregation', 'array-unnest', 'cte'],
  },
  {
    id: 'off-agg-4',
    name: 'Country product statistics',
    description: 'Product and quality statistics by country',
    complexity: 'complex',
    sql: `WITH country_unnest AS (
  SELECT
    code,
    UNNEST(string_to_array(countries_tags, ',')) as country,
    completeness,
    nutriscore_grade
  FROM products
  WHERE countries_tags IS NOT NULL
)
SELECT
  TRIM(country) as country,
  COUNT(*) as product_count,
  ROUND(AVG(completeness), 2) as avg_completeness,
  COUNT(CASE WHEN nutriscore_grade = 'a' THEN 1 END) as grade_a_count,
  COUNT(CASE WHEN nutriscore_grade = 'e' THEN 1 END) as grade_e_count
FROM country_unnest
GROUP BY TRIM(country)
HAVING COUNT(*) >= 1000
ORDER BY product_count DESC
LIMIT 30`,
    benchmarks: ['aggregation', 'conditional-count', 'geographic'],
  },

  // Complex analytical queries
  {
    id: 'off-complex-1',
    name: 'Healthier alternatives',
    description: 'Find healthier alternatives in the same category',
    complexity: 'complex',
    sql: `WITH target_product AS (
  SELECT
    code,
    product_name,
    categories_tags,
    nutriscore_grade,
    energy_100g
  FROM products
  WHERE code = '3017620422003'  -- Nutella
)
SELECT
  p.code,
  p.product_name,
  p.brands,
  p.nutriscore_grade,
  p.energy_100g,
  p.sugars_100g,
  tp.nutriscore_grade as original_grade
FROM products p
CROSS JOIN target_product tp
WHERE p.code != tp.code
  AND p.categories_tags IS NOT NULL
  AND tp.categories_tags IS NOT NULL
  AND p.categories_tags LIKE '%spreads%'
  AND p.nutriscore_grade < tp.nutriscore_grade
  AND p.completeness > 0.5
ORDER BY p.nutriscore_grade, p.energy_100g
LIMIT 20`,
    benchmarks: ['cte', 'cross-join', 'recommendation'],
  },
  {
    id: 'off-complex-2',
    name: 'Brand nutrition comparison',
    description: 'Compare nutrition profiles across brands in a category',
    complexity: 'complex',
    sql: `SELECT
  brands,
  COUNT(*) as product_count,
  ROUND(AVG(energy_100g), 0) as avg_energy,
  ROUND(AVG(sugars_100g), 1) as avg_sugars,
  ROUND(AVG(fat_100g), 1) as avg_fat,
  ROUND(AVG(salt_100g), 2) as avg_salt,
  ROUND(AVG(CASE nutriscore_grade
    WHEN 'a' THEN 1 WHEN 'b' THEN 2 WHEN 'c' THEN 3
    WHEN 'd' THEN 4 WHEN 'e' THEN 5 ELSE NULL END), 2) as avg_nutriscore_num,
  SUM(CASE WHEN nutriscore_grade IN ('a', 'b') THEN 1 ELSE 0 END) * 100.0 /
    COUNT(CASE WHEN nutriscore_grade IS NOT NULL THEN 1 END) as pct_good_grade
FROM products
WHERE categories_tags LIKE '%en:breakfast-cereals%'
  AND brands IS NOT NULL
  AND brands != ''
GROUP BY brands
HAVING COUNT(*) >= 10
ORDER BY avg_nutriscore_num ASC
LIMIT 20`,
    benchmarks: ['brand-comparison', 'conditional-aggregate', 'ranking'],
  },
  {
    id: 'off-complex-3',
    name: 'Ingredient frequency analysis',
    description: 'Most common ingredients across products',
    complexity: 'expert',
    sql: `WITH ingredient_words AS (
  SELECT
    code,
    LOWER(TRIM(UNNEST(string_to_array(
      REGEXP_REPLACE(ingredients_text, '[^a-zA-Z, ]', '', 'g'),
      ','
    )))) as ingredient
  FROM products
  WHERE ingredients_text IS NOT NULL
    AND LENGTH(ingredients_text) > 10
    AND categories_tags LIKE '%en:snacks%'
)
SELECT
  ingredient,
  COUNT(DISTINCT code) as product_count
FROM ingredient_words
WHERE LENGTH(ingredient) > 3
  AND ingredient NOT IN ('and', 'the', 'with', 'from', 'contains', 'may', 'contain')
GROUP BY ingredient
HAVING COUNT(DISTINCT code) >= 100
ORDER BY product_count DESC
LIMIT 50`,
    benchmarks: ['text-analysis', 'regex', 'ingredient-frequency'],
  },
  {
    id: 'off-complex-4',
    name: 'Nutrition trend by additives',
    description: 'Analyze nutrition impact of additives',
    complexity: 'expert',
    sql: `SELECT
  CASE
    WHEN additives_n IS NULL OR additives_n = 0 THEN '0 additives'
    WHEN additives_n <= 3 THEN '1-3 additives'
    WHEN additives_n <= 6 THEN '4-6 additives'
    WHEN additives_n <= 10 THEN '7-10 additives'
    ELSE '10+ additives'
  END as additive_range,
  COUNT(*) as product_count,
  ROUND(AVG(energy_100g), 0) as avg_energy,
  ROUND(AVG(sugars_100g), 1) as avg_sugars,
  ROUND(AVG(fat_100g), 1) as avg_fat,
  ROUND(AVG(nova_group), 2) as avg_nova_group,
  COUNT(CASE WHEN nutriscore_grade IN ('d', 'e') THEN 1 END) * 100.0 /
    COUNT(CASE WHEN nutriscore_grade IS NOT NULL THEN 1 END) as pct_poor_nutriscore
FROM products
WHERE additives_n IS NOT NULL
GROUP BY additive_range
ORDER BY
  CASE additive_range
    WHEN '0 additives' THEN 1
    WHEN '1-3 additives' THEN 2
    WHEN '4-6 additives' THEN 3
    WHEN '7-10 additives' THEN 4
    ELSE 5
  END`,
    benchmarks: ['bucketing', 'additive-analysis', 'health-correlation'],
  },

  // Full-text search
  {
    id: 'off-fts-1',
    name: 'Full-text product search',
    description: 'Search products using full-text index',
    complexity: 'moderate',
    sql: {
      postgres: `SELECT code, product_name, brands, nutriscore_grade
FROM products
WHERE to_tsvector('english', COALESCE(product_name, '') || ' ' || COALESCE(brands, ''))
  @@ to_tsquery('english', 'organic & chocolate')
ORDER BY completeness DESC
LIMIT 50`,
      duckdb: `SELECT code, product_name, brands, nutriscore_grade
FROM products
WHERE (product_name ILIKE '%organic%' AND product_name ILIKE '%chocolate%')
   OR (brands ILIKE '%organic%' AND product_name ILIKE '%chocolate%')
ORDER BY completeness DESC
LIMIT 50`,
      sqlite: `SELECT p.code, p.product_name, p.brands, p.nutriscore_grade
FROM products p
JOIN products_fts fts ON fts.rowid = p.rowid
WHERE products_fts MATCH 'organic chocolate'
ORDER BY p.completeness DESC
LIMIT 50`,
    },
    benchmarks: ['full-text-search', 'multi-field'],
  },

  // JSON/complex data handling
  {
    id: 'off-json-1',
    name: 'Nutrient JSON extraction',
    description: 'Extract specific nutrients from JSON column',
    complexity: 'moderate',
    sql: {
      duckdb: `SELECT
  code,
  product_name,
  nutriments->>'energy-kcal_100g' as energy_kcal,
  nutriments->>'carbohydrates_100g' as carbs,
  nutriments->>'proteins_100g' as protein,
  nutriments->>'fat_100g' as fat
FROM products
WHERE nutriments IS NOT NULL
  AND nutriments->>'energy-kcal_100g' IS NOT NULL
LIMIT 100`,
      postgres: `SELECT
  code,
  product_name,
  nutriments->>'energy-kcal_100g' as energy_kcal,
  nutriments->>'carbohydrates_100g' as carbs,
  nutriments->>'proteins_100g' as protein,
  nutriments->>'fat_100g' as fat
FROM products
WHERE nutriments IS NOT NULL
  AND nutriments ? 'energy-kcal_100g'
LIMIT 100`,
    },
    benchmarks: ['json-extraction', 'document-query'],
  },
]

/**
 * Open Food Facts dataset configuration
 */
export const openFoodFacts: OpenFoodFactsConfig = {
  id: 'open-food-facts',
  name: 'Open Food Facts',
  description: `Open Food Facts is a free, collaborative database of food products containing
~3.5M products worldwide with detailed nutrition, ingredients, allergens, and labels.
Excellent for full-text search, faceted browse, and nutrition analysis.`,
  category: 'full-text',
  size: 'large',
  rowCount: '~3.5M products',
  compressedSize: '~2GB (compressed CSV/JSON)',
  uncompressedSize: '~4GB',
  sourceUrl: 'https://world.openfoodfacts.org/data',
  license: 'Open Database License (ODbL)',
  suitedFor: ['duckdb', 'postgres', 'sqlite'],
  apiEndpoint: 'https://world.openfoodfacts.org/api/v2/',

  exportFormats: [
    {
      format: 'CSV',
      url: 'https://static.openfoodfacts.org/data/en.openfoodfacts.org.products.csv.gz',
      size: '~2GB compressed',
      description: 'Full database export in CSV format',
    },
    {
      format: 'JSONL',
      url: 'https://static.openfoodfacts.org/data/openfoodfacts-products.jsonl.gz',
      size: '~3GB compressed',
      description: 'Full database export in JSON Lines format',
    },
    {
      format: 'MongoDB dump',
      url: 'https://static.openfoodfacts.org/data/openfoodfacts-mongodbdump.gz',
      size: '~4GB compressed',
      description: 'MongoDB BSON dump for direct import',
    },
    {
      format: 'Delta updates',
      url: 'https://static.openfoodfacts.org/data/delta/',
      size: 'Variable',
      description: 'Daily incremental updates in JSONL format',
    },
  ],

  countrySubsets: [
    'world',
    'us',
    'fr',
    'de',
    'uk',
    'es',
    'it',
    'be',
    'ch',
    'nl',
  ],

  qualityTiers: {
    complete: 'Products with all required fields (completeness > 0.8)',
    good: 'Products with most fields (completeness 0.5-0.8)',
    partial: 'Products with basic info (completeness 0.2-0.5)',
    minimal: 'Products with only barcode and name (completeness < 0.2)',
  },

  downloadConfigs: {
    local: {
      urls: [
        'https://static.openfoodfacts.org/data/en.openfoodfacts.org.products.csv.gz',
      ],
      size: '~2GB compressed',
      rowCount: '~3.5M products',
      instructions: [
        '# Download Open Food Facts CSV export',
        'mkdir -p openfoodfacts_data && cd openfoodfacts_data',
        'curl -O https://static.openfoodfacts.org/data/en.openfoodfacts.org.products.csv.gz',
        '',
        '# Decompress',
        'gunzip en.openfoodfacts.org.products.csv.gz',
        '',
        '# Note: CSV is ~4GB uncompressed with 180+ columns',
        '# For smaller subset, use US or country-specific export:',
        'curl -O https://static.openfoodfacts.org/data/us.openfoodfacts.org.products.csv.gz',
      ],
      setupCommands: [
        '# Load into DuckDB (auto-detects schema)',
        "duckdb openfoodfacts.db -c \"",
        "CREATE TABLE products AS SELECT * FROM read_csv_auto('en.openfoodfacts.org.products.csv',",
        "  sample_size=100000, ignore_errors=true);",
        "\"",
        '',
        '# Create indexes for common queries',
        "duckdb openfoodfacts.db -c \"",
        "CREATE INDEX idx_code ON products(code);",
        "CREATE INDEX idx_brands ON products(brands);",
        "CREATE INDEX idx_nutriscore ON products(nutriscore_grade);",
        "\"",
      ],
    },
    development: {
      urls: [
        'https://static.openfoodfacts.org/data/us.openfoodfacts.org.products.csv.gz',
      ],
      size: '~200MB compressed',
      rowCount: '~300K products (US only)',
      instructions: [
        '# Download US-only subset for development',
        'curl -O https://static.openfoodfacts.org/data/us.openfoodfacts.org.products.csv.gz',
        'gunzip us.openfoodfacts.org.products.csv.gz',
      ],
      setupCommands: [
        '# Load US subset',
        'duckdb openfoodfacts_us.db < scripts/load_off_us.sql',
      ],
    },
    production: {
      urls: [
        'https://static.openfoodfacts.org/data/en.openfoodfacts.org.products.csv.gz',
        'https://static.openfoodfacts.org/data/openfoodfacts-products.jsonl.gz',
      ],
      size: '~4GB (CSV) or ~6GB (JSONL)',
      rowCount: '~3.5M products',
      instructions: [
        '# Download full dataset (CSV or JSONL)',
        'curl -O https://static.openfoodfacts.org/data/en.openfoodfacts.org.products.csv.gz',
        '',
        '# Or for richer JSON data with nested fields:',
        'curl -O https://static.openfoodfacts.org/data/openfoodfacts-products.jsonl.gz',
        '',
        '# Set up daily delta updates',
        '# See https://static.openfoodfacts.org/data/delta/',
      ],
      setupCommands: [
        '# Load full dataset with optimizations',
        'duckdb openfoodfacts_full.db < scripts/load_off_full.sql',
        '',
        '# Create materialized views for common aggregations',
        'duckdb openfoodfacts_full.db < scripts/create_off_views.sql',
      ],
    },
  },

  schema: {
    tableName: 'products',
    columns: [
      { name: 'code', type: 'VARCHAR(20)', nullable: false, description: 'Barcode (EAN-13, UPC, etc.)' },
      { name: 'url', type: 'TEXT', nullable: true, description: 'Product page URL' },
      { name: 'creator', type: 'VARCHAR(100)', nullable: true, description: 'User who created the entry' },
      { name: 'created_t', type: 'BIGINT', nullable: true, description: 'Creation timestamp (Unix)' },
      { name: 'last_modified_t', type: 'BIGINT', nullable: true, description: 'Last modification timestamp' },
      { name: 'product_name', type: 'TEXT', nullable: true, description: 'Product name' },
      { name: 'abbreviated_product_name', type: 'TEXT', nullable: true, description: 'Short product name' },
      { name: 'generic_name', type: 'TEXT', nullable: true, description: 'Generic product description' },
      { name: 'quantity', type: 'VARCHAR(100)', nullable: true, description: 'Product quantity/size' },
      { name: 'packaging', type: 'TEXT', nullable: true, description: 'Packaging type' },
      { name: 'packaging_tags', type: 'TEXT', nullable: true, description: 'Packaging tags (comma-separated)' },
      { name: 'brands', type: 'TEXT', nullable: true, description: 'Brand name(s)' },
      { name: 'brands_tags', type: 'TEXT', nullable: true, description: 'Brand tags' },
      { name: 'categories', type: 'TEXT', nullable: true, description: 'Product categories' },
      { name: 'categories_tags', type: 'TEXT', nullable: true, description: 'Category tags (hierarchical)' },
      { name: 'origins', type: 'TEXT', nullable: true, description: 'Origin of ingredients' },
      { name: 'origins_tags', type: 'TEXT', nullable: true, description: 'Origin tags' },
      { name: 'manufacturing_places', type: 'TEXT', nullable: true, description: 'Manufacturing locations' },
      { name: 'labels', type: 'TEXT', nullable: true, description: 'Labels (organic, fair trade, etc.)' },
      { name: 'labels_tags', type: 'TEXT', nullable: true, description: 'Label tags' },
      { name: 'stores', type: 'TEXT', nullable: true, description: 'Stores selling the product' },
      { name: 'countries', type: 'TEXT', nullable: true, description: 'Countries where sold' },
      { name: 'countries_tags', type: 'TEXT', nullable: true, description: 'Country tags' },
      { name: 'ingredients_text', type: 'TEXT', nullable: true, description: 'Ingredients list' },
      { name: 'allergens', type: 'TEXT', nullable: true, description: 'Allergens present' },
      { name: 'allergens_tags', type: 'TEXT', nullable: true, description: 'Allergen tags' },
      { name: 'traces', type: 'TEXT', nullable: true, description: 'May contain traces of' },
      { name: 'traces_tags', type: 'TEXT', nullable: true, description: 'Trace tags' },
      { name: 'serving_size', type: 'VARCHAR(50)', nullable: true, description: 'Serving size' },
      { name: 'serving_quantity', type: 'DECIMAL(10,2)', nullable: true, description: 'Serving quantity in grams' },
      { name: 'nutriscore_grade', type: 'CHAR(1)', nullable: true, description: 'Nutri-Score (a-e)' },
      { name: 'nutriscore_score', type: 'INTEGER', nullable: true, description: 'Nutri-Score numeric value' },
      { name: 'nova_group', type: 'INTEGER', nullable: true, description: 'NOVA food processing classification (1-4)' },
      { name: 'additives_n', type: 'INTEGER', nullable: true, description: 'Number of additives' },
      { name: 'additives_tags', type: 'TEXT', nullable: true, description: 'Additive tags' },
      { name: 'ingredients_from_palm_oil_n', type: 'INTEGER', nullable: true, description: 'Palm oil ingredients count' },
      { name: 'energy_100g', type: 'DECIMAL(10,2)', nullable: true, description: 'Energy per 100g (kJ)' },
      { name: 'energy_kcal_100g', type: 'DECIMAL(10,2)', nullable: true, description: 'Energy per 100g (kcal)' },
      { name: 'fat_100g', type: 'DECIMAL(10,2)', nullable: true, description: 'Fat per 100g' },
      { name: 'saturated_fat_100g', type: 'DECIMAL(10,2)', nullable: true, description: 'Saturated fat per 100g' },
      { name: 'carbohydrates_100g', type: 'DECIMAL(10,2)', nullable: true, description: 'Carbohydrates per 100g' },
      { name: 'sugars_100g', type: 'DECIMAL(10,2)', nullable: true, description: 'Sugars per 100g' },
      { name: 'fiber_100g', type: 'DECIMAL(10,2)', nullable: true, description: 'Fiber per 100g' },
      { name: 'proteins_100g', type: 'DECIMAL(10,2)', nullable: true, description: 'Proteins per 100g' },
      { name: 'salt_100g', type: 'DECIMAL(10,2)', nullable: true, description: 'Salt per 100g' },
      { name: 'sodium_100g', type: 'DECIMAL(10,2)', nullable: true, description: 'Sodium per 100g' },
      { name: 'completeness', type: 'DECIMAL(5,4)', nullable: true, description: 'Data completeness score (0-1)' },
      { name: 'popularity_tags', type: 'TEXT', nullable: true, description: 'Popularity indicators' },
      { name: 'nutriments', type: 'JSON', nullable: true, description: 'Full nutrient data (JSON)' },
    ],
    primaryKey: ['code'],
    indexes: [
      { name: 'idx_products_code', columns: ['code'], type: 'btree', description: 'Barcode lookup' },
      { name: 'idx_products_brands', columns: ['brands'], type: 'btree', description: 'Brand filter' },
      { name: 'idx_products_nutriscore', columns: ['nutriscore_grade'], type: 'btree', description: 'Nutri-Score filter' },
      { name: 'idx_products_countries', columns: ['countries_tags'], type: 'btree', description: 'Country filter' },
    ],
    createTableSQL: {
      duckdb: `-- Products table
CREATE TABLE products (
  code VARCHAR(20) PRIMARY KEY,
  url TEXT,
  creator VARCHAR(100),
  created_t BIGINT,
  last_modified_t BIGINT,
  product_name TEXT,
  abbreviated_product_name TEXT,
  generic_name TEXT,
  quantity VARCHAR(100),
  packaging TEXT,
  packaging_tags TEXT,
  brands TEXT,
  brands_tags TEXT,
  categories TEXT,
  categories_tags TEXT,
  origins TEXT,
  origins_tags TEXT,
  manufacturing_places TEXT,
  labels TEXT,
  labels_tags TEXT,
  stores TEXT,
  countries TEXT,
  countries_tags TEXT,
  ingredients_text TEXT,
  allergens TEXT,
  allergens_tags TEXT,
  traces TEXT,
  traces_tags TEXT,
  serving_size VARCHAR(50),
  serving_quantity DECIMAL(10,2),
  nutriscore_grade CHAR(1),
  nutriscore_score INTEGER,
  nova_group INTEGER,
  additives_n INTEGER,
  additives_tags TEXT,
  ingredients_from_palm_oil_n INTEGER,
  energy_100g DECIMAL(10,2),
  energy_kcal_100g DECIMAL(10,2),
  fat_100g DECIMAL(10,2),
  saturated_fat_100g DECIMAL(10,2),
  carbohydrates_100g DECIMAL(10,2),
  sugars_100g DECIMAL(10,2),
  fiber_100g DECIMAL(10,2),
  proteins_100g DECIMAL(10,2),
  salt_100g DECIMAL(10,2),
  sodium_100g DECIMAL(10,2),
  completeness DECIMAL(5,4),
  popularity_tags TEXT,
  nutriments JSON
);

-- Indexes
CREATE INDEX idx_products_brands ON products(brands);
CREATE INDEX idx_products_nutriscore ON products(nutriscore_grade);
CREATE INDEX idx_products_nova ON products(nova_group);
CREATE INDEX idx_products_completeness ON products(completeness);`,

      postgres: `-- Products table with full-text search
CREATE TABLE products (
  code VARCHAR(20) PRIMARY KEY,
  url TEXT,
  creator VARCHAR(100),
  created_t BIGINT,
  last_modified_t BIGINT,
  product_name TEXT,
  abbreviated_product_name TEXT,
  generic_name TEXT,
  quantity VARCHAR(100),
  packaging TEXT,
  packaging_tags TEXT,
  brands TEXT,
  brands_tags TEXT,
  categories TEXT,
  categories_tags TEXT,
  origins TEXT,
  origins_tags TEXT,
  manufacturing_places TEXT,
  labels TEXT,
  labels_tags TEXT,
  stores TEXT,
  countries TEXT,
  countries_tags TEXT,
  ingredients_text TEXT,
  allergens TEXT,
  allergens_tags TEXT,
  traces TEXT,
  traces_tags TEXT,
  serving_size VARCHAR(50),
  serving_quantity DECIMAL(10,2),
  nutriscore_grade CHAR(1),
  nutriscore_score INTEGER,
  nova_group INTEGER,
  additives_n INTEGER,
  additives_tags TEXT,
  ingredients_from_palm_oil_n INTEGER,
  energy_100g DECIMAL(10,2),
  energy_kcal_100g DECIMAL(10,2),
  fat_100g DECIMAL(10,2),
  saturated_fat_100g DECIMAL(10,2),
  carbohydrates_100g DECIMAL(10,2),
  sugars_100g DECIMAL(10,2),
  fiber_100g DECIMAL(10,2),
  proteins_100g DECIMAL(10,2),
  salt_100g DECIMAL(10,2),
  sodium_100g DECIMAL(10,2),
  completeness DECIMAL(5,4),
  popularity_tags TEXT,
  nutriments JSONB,
  search_vector TSVECTOR GENERATED ALWAYS AS (
    setweight(to_tsvector('english', COALESCE(product_name, '')), 'A') ||
    setweight(to_tsvector('english', COALESCE(brands, '')), 'B') ||
    setweight(to_tsvector('english', COALESCE(categories, '')), 'C') ||
    setweight(to_tsvector('english', COALESCE(ingredients_text, '')), 'D')
  ) STORED
);

-- Indexes
CREATE INDEX idx_products_brands ON products(brands);
CREATE INDEX idx_products_nutriscore ON products(nutriscore_grade);
CREATE INDEX idx_products_search ON products USING GIN(search_vector);
CREATE INDEX idx_products_nutriments ON products USING GIN(nutriments);
CREATE INDEX idx_products_brands_trgm ON products USING GIN(brands gin_trgm_ops);`,

      sqlite: `-- Products table
CREATE TABLE products (
  code TEXT PRIMARY KEY,
  url TEXT,
  creator TEXT,
  created_t INTEGER,
  last_modified_t INTEGER,
  product_name TEXT,
  abbreviated_product_name TEXT,
  generic_name TEXT,
  quantity TEXT,
  packaging TEXT,
  packaging_tags TEXT,
  brands TEXT,
  brands_tags TEXT,
  categories TEXT,
  categories_tags TEXT,
  origins TEXT,
  origins_tags TEXT,
  manufacturing_places TEXT,
  labels TEXT,
  labels_tags TEXT,
  stores TEXT,
  countries TEXT,
  countries_tags TEXT,
  ingredients_text TEXT,
  allergens TEXT,
  allergens_tags TEXT,
  traces TEXT,
  traces_tags TEXT,
  serving_size TEXT,
  serving_quantity REAL,
  nutriscore_grade TEXT,
  nutriscore_score INTEGER,
  nova_group INTEGER,
  additives_n INTEGER,
  additives_tags TEXT,
  ingredients_from_palm_oil_n INTEGER,
  energy_100g REAL,
  energy_kcal_100g REAL,
  fat_100g REAL,
  saturated_fat_100g REAL,
  carbohydrates_100g REAL,
  sugars_100g REAL,
  fiber_100g REAL,
  proteins_100g REAL,
  salt_100g REAL,
  sodium_100g REAL,
  completeness REAL,
  popularity_tags TEXT,
  nutriments TEXT
);

-- Indexes
CREATE INDEX idx_products_brands ON products(brands);
CREATE INDEX idx_products_nutriscore ON products(nutriscore_grade);
CREATE INDEX idx_products_countries ON products(countries_tags);

-- FTS5 for full-text search
CREATE VIRTUAL TABLE products_fts USING fts5(
  product_name,
  brands,
  categories,
  ingredients_text,
  content=products,
  content_rowid=rowid
);`,

      clickhouse: `-- Optimized for analytics
CREATE TABLE products (
  code String,
  product_name Nullable(String),
  brands Nullable(String),
  brands_tags Nullable(String),
  categories Nullable(String),
  categories_tags Nullable(String),
  countries_tags Nullable(String),
  ingredients_text Nullable(String),
  allergens_tags Nullable(String),
  nutriscore_grade LowCardinality(Nullable(String)),
  nutriscore_score Nullable(Int8),
  nova_group Nullable(UInt8),
  additives_n Nullable(UInt8),
  energy_100g Nullable(Float32),
  fat_100g Nullable(Float32),
  saturated_fat_100g Nullable(Float32),
  carbohydrates_100g Nullable(Float32),
  sugars_100g Nullable(Float32),
  fiber_100g Nullable(Float32),
  proteins_100g Nullable(Float32),
  salt_100g Nullable(Float32),
  completeness Nullable(Float32),
  last_modified_t Nullable(Int64)
) ENGINE = MergeTree()
ORDER BY (nutriscore_grade, code);

-- Materialized view for brand statistics
CREATE MATERIALIZED VIEW brand_stats
ENGINE = SummingMergeTree()
ORDER BY brands
AS SELECT
  brands,
  count() as product_count,
  avg(nutriscore_score) as avg_nutriscore,
  avg(nova_group) as avg_nova
FROM products
WHERE brands IS NOT NULL
GROUP BY brands;`,

      db4: `-- Same as SQLite`,
      evodb: `-- Same as SQLite`,
    },
  },

  queries: openFoodFactsQueries,

  performanceExpectations: {
    duckdb: {
      loadTime: '~3 minutes',
      simpleQueryLatency: '<50ms',
      complexQueryLatency: '100ms-2s',
      storageEfficiency: 'Excellent',
      concurrency: 'Good',
      notes: [
        'Best for analytical queries',
        'Excellent CSV loading performance',
        'Good string operations',
        'Native JSON support',
      ],
    },
    postgres: {
      loadTime: '~15 minutes',
      simpleQueryLatency: '<30ms',
      complexQueryLatency: '100ms-5s',
      storageEfficiency: 'Good',
      concurrency: 'Excellent',
      notes: [
        'Best full-text search (tsvector)',
        'Excellent JSONB operations',
        'pg_trgm for fuzzy matching',
        'GIN indexes for tags',
      ],
    },
    sqlite: {
      loadTime: '~20 minutes',
      simpleQueryLatency: '10-100ms',
      complexQueryLatency: '500ms-10s',
      storageEfficiency: 'Moderate',
      concurrency: 'Limited',
      notes: [
        'Good for embedded/local use',
        'FTS5 for full-text search',
        'JSON1 extension for nutriments',
        'Consider US subset for development',
      ],
    },
    clickhouse: {
      loadTime: '~2 minutes',
      simpleQueryLatency: '<50ms',
      complexQueryLatency: '50ms-1s',
      storageEfficiency: 'Excellent',
      concurrency: 'Excellent',
      notes: [
        'Best for large-scale aggregations',
        'LowCardinality for nutriscore/nova',
        'Materialized views for brand stats',
        'Less optimal for text search',
      ],
    },
    db4: {
      loadTime: '~20 minutes',
      simpleQueryLatency: '10-100ms',
      complexQueryLatency: '500ms-10s',
      storageEfficiency: 'Moderate',
      concurrency: 'Limited',
      notes: ['Same as SQLite', 'Good for edge/embedded use'],
    },
    evodb: {
      loadTime: '~20 minutes',
      simpleQueryLatency: '10-100ms',
      complexQueryLatency: '500ms-10s',
      storageEfficiency: 'Moderate',
      concurrency: 'Limited',
      notes: ['Same as SQLite'],
    },
  },

  r2Config: {
    bucketName: 'bench-datasets',
    pathPrefix: 'open-food-facts/',
    format: 'parquet',
    compression: 'zstd',
    partitioning: {
      columns: ['countries_tags', 'nutriscore_grade'],
      format: 'country={country}/nutriscore={grade}/*.parquet',
    },
    uploadInstructions: [
      '# Convert CSV to partitioned Parquet',
      'duckdb -c "',
      "  COPY (",
      "    SELECT *,",
      "    SPLIT_PART(countries_tags, ',', 1) as primary_country",
      "    FROM read_csv_auto('en.openfoodfacts.org.products.csv')",
      "  ) TO 'off_parquet'",
      "  (FORMAT PARQUET, PARTITION_BY (primary_country), COMPRESSION 'zstd');",
      '"',
      '',
      '# Upload to R2',
      'wrangler r2 object put bench-datasets/open-food-facts/ --file=off_parquet/ --recursive',
    ],
    duckdbInstructions: [
      '-- Query US products from R2',
      "SELECT * FROM read_parquet('s3://bench-datasets/open-food-facts/country=en:united-states/*.parquet')",
      "WHERE nutriscore_grade = 'a'",
      'LIMIT 100;',
      '',
      '-- Cross-country analysis',
      "SELECT primary_country, COUNT(*), AVG(completeness) FROM read_parquet('s3://bench-datasets/open-food-facts/**/*.parquet')",
      'GROUP BY primary_country',
      'ORDER BY COUNT(*) DESC;',
    ],
  },
}

export default openFoodFacts
