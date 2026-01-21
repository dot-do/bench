/**
 * E-commerce OLTP Dataset
 *
 * A comprehensive e-commerce dataset with orders, products, customers, and reviews.
 * Represents a typical online retail workload with:
 * - High read volume on product catalog
 * - Order placement and processing
 * - Customer account management
 * - Product reviews and ratings
 */

import {
  DatasetConfig,
  TableConfig,
  RelationshipConfig,
  SizeTierConfig,
  WorkloadProfile,
  BenchmarkQuery,
  registerDataset,
} from './index'

// ============================================================================
// Tables
// ============================================================================

const customersTable: TableConfig = {
  name: 'customers',
  columns: [
    { name: 'id', type: 'uuid', primaryKey: true, generator: { type: 'uuid' } },
    { name: 'email', type: 'string', maxLength: 255, unique: true, indexed: true, generator: { type: 'faker', fakerMethod: 'internet.email' } },
    { name: 'password_hash', type: 'string', maxLength: 255, generator: { type: 'random-string', length: 64 } },
    { name: 'first_name', type: 'string', maxLength: 100, generator: { type: 'faker', fakerMethod: 'person.firstName' } },
    { name: 'last_name', type: 'string', maxLength: 100, generator: { type: 'faker', fakerMethod: 'person.lastName' } },
    { name: 'phone', type: 'string', maxLength: 20, nullable: true, generator: { type: 'faker', fakerMethod: 'phone.number' } },
    { name: 'tier', type: 'string', maxLength: 20, default: 'standard', generator: { type: 'weighted-enum', values: ['standard', 'premium', 'vip'], weights: [0.7, 0.25, 0.05] } },
    { name: 'total_spent', type: 'decimal', precision: 12, scale: 2, default: 0, generator: { type: 'random-decimal', min: 0, max: 50000, precision: 2 } },
    { name: 'order_count', type: 'integer', default: 0, generator: { type: 'random-int', min: 0, max: 100 } },
    { name: 'created_at', type: 'timestamp', generator: { type: 'timestamp-range', start: '2020-01-01', end: '2024-01-01' } },
    { name: 'updated_at', type: 'timestamp', generator: { type: 'timestamp-range', start: '2023-01-01', end: '2024-12-31' } },
    { name: 'last_login_at', type: 'timestamp', nullable: true, generator: { type: 'timestamp-range', start: '2024-01-01', end: '2024-12-31' } },
    { name: 'metadata', type: 'json', nullable: true },
  ],
  indexes: [
    { name: 'idx_customers_tier', columns: ['tier'] },
    { name: 'idx_customers_created', columns: ['created_at'] },
    { name: 'idx_customers_last_login', columns: ['last_login_at'] },
  ],
}

const addressesTable: TableConfig = {
  name: 'addresses',
  columns: [
    { name: 'id', type: 'uuid', primaryKey: true, generator: { type: 'uuid' } },
    { name: 'customer_id', type: 'uuid', indexed: true, references: { table: 'customers', column: 'id' }, generator: { type: 'reference', referenceTable: 'customers', referenceColumn: 'id' } },
    { name: 'type', type: 'string', maxLength: 20, generator: { type: 'enum', values: ['billing', 'shipping'] } },
    { name: 'is_default', type: 'boolean', default: false, generator: { type: 'weighted-enum', values: [true, false], weights: [0.3, 0.7] } },
    { name: 'street_1', type: 'string', maxLength: 255, generator: { type: 'faker', fakerMethod: 'location.streetAddress' } },
    { name: 'street_2', type: 'string', maxLength: 255, nullable: true, generator: { type: 'faker', fakerMethod: 'location.secondaryAddress' } },
    { name: 'city', type: 'string', maxLength: 100, generator: { type: 'faker', fakerMethod: 'location.city' } },
    { name: 'state', type: 'string', maxLength: 100, generator: { type: 'faker', fakerMethod: 'location.state' } },
    { name: 'postal_code', type: 'string', maxLength: 20, generator: { type: 'faker', fakerMethod: 'location.zipCode' } },
    { name: 'country', type: 'string', maxLength: 2, default: 'US', generator: { type: 'faker', fakerMethod: 'location.countryCode' } },
    { name: 'created_at', type: 'timestamp', generator: { type: 'timestamp-range', start: '2020-01-01', end: '2024-12-31' } },
  ],
  indexes: [
    { name: 'idx_addresses_customer', columns: ['customer_id'] },
    { name: 'idx_addresses_customer_default', columns: ['customer_id', 'type', 'is_default'] },
  ],
}

const categoriesTable: TableConfig = {
  name: 'categories',
  columns: [
    { name: 'id', type: 'uuid', primaryKey: true, generator: { type: 'uuid' } },
    { name: 'parent_id', type: 'uuid', nullable: true, indexed: true, references: { table: 'categories', column: 'id' }, generator: { type: 'reference', referenceTable: 'categories', referenceColumn: 'id', distribution: 'zipf' } },
    { name: 'name', type: 'string', maxLength: 100, generator: { type: 'faker', fakerMethod: 'commerce.department' } },
    { name: 'slug', type: 'string', maxLength: 100, unique: true, generator: { type: 'faker', fakerMethod: 'helpers.slugify' } },
    { name: 'description', type: 'text', nullable: true, generator: { type: 'faker', fakerMethod: 'commerce.productDescription' } },
    { name: 'image_url', type: 'string', maxLength: 500, nullable: true, generator: { type: 'faker', fakerMethod: 'image.url' } },
    { name: 'is_active', type: 'boolean', default: true, generator: { type: 'weighted-enum', values: [true, false], weights: [0.95, 0.05] } },
    { name: 'sort_order', type: 'integer', default: 0, generator: { type: 'random-int', min: 0, max: 100 } },
    { name: 'created_at', type: 'timestamp', generator: { type: 'timestamp-range', start: '2020-01-01', end: '2024-01-01' } },
  ],
  indexes: [
    { name: 'idx_categories_parent', columns: ['parent_id'] },
    { name: 'idx_categories_active', columns: ['is_active', 'sort_order'] },
  ],
}

const productsTable: TableConfig = {
  name: 'products',
  columns: [
    { name: 'id', type: 'uuid', primaryKey: true, generator: { type: 'uuid' } },
    { name: 'sku', type: 'string', maxLength: 50, unique: true, indexed: true, generator: { type: 'faker', fakerMethod: 'string.alphanumeric' } },
    { name: 'name', type: 'string', maxLength: 255, indexed: true, generator: { type: 'faker', fakerMethod: 'commerce.productName' } },
    { name: 'slug', type: 'string', maxLength: 255, unique: true, generator: { type: 'faker', fakerMethod: 'helpers.slugify' } },
    { name: 'description', type: 'text', nullable: true, generator: { type: 'faker', fakerMethod: 'commerce.productDescription' } },
    { name: 'category_id', type: 'uuid', indexed: true, references: { table: 'categories', column: 'id' }, generator: { type: 'reference', referenceTable: 'categories', referenceColumn: 'id', distribution: 'zipf' } },
    { name: 'brand', type: 'string', maxLength: 100, nullable: true, indexed: true, generator: { type: 'faker', fakerMethod: 'company.name' } },
    { name: 'price', type: 'decimal', precision: 10, scale: 2, indexed: true, generator: { type: 'random-decimal', min: 1, max: 5000, precision: 2 } },
    { name: 'cost', type: 'decimal', precision: 10, scale: 2, generator: { type: 'random-decimal', min: 0.5, max: 2500, precision: 2 } },
    { name: 'compare_at_price', type: 'decimal', precision: 10, scale: 2, nullable: true, generator: { type: 'random-decimal', min: 1, max: 6000, precision: 2 } },
    { name: 'currency', type: 'string', maxLength: 3, default: 'USD', generator: { type: 'enum', values: ['USD', 'EUR', 'GBP', 'CAD'] } },
    { name: 'stock_quantity', type: 'integer', default: 0, indexed: true, generator: { type: 'random-int', min: 0, max: 1000 } },
    { name: 'low_stock_threshold', type: 'integer', default: 10, generator: { type: 'random-int', min: 5, max: 50 } },
    { name: 'weight', type: 'decimal', precision: 8, scale: 2, nullable: true, generator: { type: 'random-decimal', min: 0.1, max: 100, precision: 2 } },
    { name: 'weight_unit', type: 'string', maxLength: 10, default: 'kg', generator: { type: 'enum', values: ['kg', 'lb', 'oz', 'g'] } },
    { name: 'is_active', type: 'boolean', default: true, indexed: true, generator: { type: 'weighted-enum', values: [true, false], weights: [0.9, 0.1] } },
    { name: 'is_featured', type: 'boolean', default: false, indexed: true, generator: { type: 'weighted-enum', values: [true, false], weights: [0.05, 0.95] } },
    { name: 'rating_avg', type: 'decimal', precision: 3, scale: 2, default: 0, indexed: true, generator: { type: 'random-decimal', min: 1, max: 5, precision: 2 } },
    { name: 'rating_count', type: 'integer', default: 0, generator: { type: 'random-int', min: 0, max: 500 } },
    { name: 'view_count', type: 'integer', default: 0, generator: { type: 'random-int', min: 0, max: 100000 } },
    { name: 'tags', type: 'array', nullable: true, generator: { type: 'faker', fakerMethod: 'helpers.arrayElements' } },
    { name: 'attributes', type: 'json', nullable: true },
    { name: 'created_at', type: 'timestamp', indexed: true, generator: { type: 'timestamp-range', start: '2020-01-01', end: '2024-12-31' } },
    { name: 'updated_at', type: 'timestamp', generator: { type: 'timestamp-range', start: '2023-01-01', end: '2024-12-31' } },
  ],
  indexes: [
    { name: 'idx_products_category', columns: ['category_id'] },
    { name: 'idx_products_category_active', columns: ['category_id', 'is_active'] },
    { name: 'idx_products_price', columns: ['price'] },
    { name: 'idx_products_stock', columns: ['stock_quantity'], where: 'stock_quantity > 0' },
    { name: 'idx_products_featured', columns: ['is_featured', 'rating_avg'], where: 'is_active = true' },
    { name: 'idx_products_search', columns: ['name', 'brand'] },
  ],
}

const productImagesTable: TableConfig = {
  name: 'product_images',
  columns: [
    { name: 'id', type: 'uuid', primaryKey: true, generator: { type: 'uuid' } },
    { name: 'product_id', type: 'uuid', indexed: true, references: { table: 'products', column: 'id' }, generator: { type: 'reference', referenceTable: 'products', referenceColumn: 'id' } },
    { name: 'url', type: 'string', maxLength: 500, generator: { type: 'faker', fakerMethod: 'image.url' } },
    { name: 'alt_text', type: 'string', maxLength: 255, nullable: true, generator: { type: 'faker', fakerMethod: 'lorem.sentence' } },
    { name: 'sort_order', type: 'integer', default: 0, generator: { type: 'sequence' } },
    { name: 'is_primary', type: 'boolean', default: false, generator: { type: 'weighted-enum', values: [true, false], weights: [0.25, 0.75] } },
    { name: 'created_at', type: 'timestamp', generator: { type: 'timestamp-range', start: '2020-01-01', end: '2024-12-31' } },
  ],
  indexes: [
    { name: 'idx_product_images_product', columns: ['product_id', 'sort_order'] },
    { name: 'idx_product_images_primary', columns: ['product_id', 'is_primary'] },
  ],
  embedded: ['url', 'alt_text', 'sort_order', 'is_primary'],
}

const ordersTable: TableConfig = {
  name: 'orders',
  columns: [
    { name: 'id', type: 'uuid', primaryKey: true, generator: { type: 'uuid' } },
    { name: 'order_number', type: 'string', maxLength: 50, unique: true, indexed: true, generator: { type: 'sequence' } },
    { name: 'customer_id', type: 'uuid', indexed: true, references: { table: 'customers', column: 'id' }, generator: { type: 'reference', referenceTable: 'customers', referenceColumn: 'id', distribution: 'zipf' } },
    { name: 'status', type: 'string', maxLength: 20, indexed: true, generator: { type: 'weighted-enum', values: ['pending', 'confirmed', 'processing', 'shipped', 'delivered', 'cancelled', 'refunded'], weights: [0.05, 0.1, 0.15, 0.2, 0.4, 0.05, 0.05] } },
    { name: 'payment_status', type: 'string', maxLength: 20, indexed: true, generator: { type: 'weighted-enum', values: ['pending', 'authorized', 'captured', 'failed', 'refunded'], weights: [0.05, 0.1, 0.75, 0.05, 0.05] } },
    { name: 'fulfillment_status', type: 'string', maxLength: 20, indexed: true, generator: { type: 'weighted-enum', values: ['unfulfilled', 'partial', 'fulfilled'], weights: [0.2, 0.1, 0.7] } },
    { name: 'subtotal', type: 'decimal', precision: 12, scale: 2, generator: { type: 'random-decimal', min: 10, max: 5000, precision: 2 } },
    { name: 'discount_amount', type: 'decimal', precision: 12, scale: 2, default: 0, generator: { type: 'random-decimal', min: 0, max: 500, precision: 2 } },
    { name: 'tax_amount', type: 'decimal', precision: 12, scale: 2, default: 0, generator: { type: 'random-decimal', min: 0, max: 500, precision: 2 } },
    { name: 'shipping_amount', type: 'decimal', precision: 12, scale: 2, default: 0, generator: { type: 'random-decimal', min: 0, max: 50, precision: 2 } },
    { name: 'total', type: 'decimal', precision: 12, scale: 2, indexed: true, generator: { type: 'random-decimal', min: 10, max: 6000, precision: 2 } },
    { name: 'currency', type: 'string', maxLength: 3, default: 'USD', generator: { type: 'enum', values: ['USD', 'EUR', 'GBP', 'CAD'] } },
    { name: 'shipping_address_id', type: 'uuid', nullable: true, references: { table: 'addresses', column: 'id' }, generator: { type: 'reference', referenceTable: 'addresses', referenceColumn: 'id' } },
    { name: 'billing_address_id', type: 'uuid', nullable: true, references: { table: 'addresses', column: 'id' }, generator: { type: 'reference', referenceTable: 'addresses', referenceColumn: 'id' } },
    { name: 'shipping_method', type: 'string', maxLength: 50, nullable: true, generator: { type: 'enum', values: ['standard', 'express', 'overnight', 'pickup'] } },
    { name: 'tracking_number', type: 'string', maxLength: 100, nullable: true, generator: { type: 'random-string', length: 20 } },
    { name: 'notes', type: 'text', nullable: true, generator: { type: 'faker', fakerMethod: 'lorem.paragraph' } },
    { name: 'ip_address', type: 'string', maxLength: 45, nullable: true, generator: { type: 'faker', fakerMethod: 'internet.ip' } },
    { name: 'user_agent', type: 'string', maxLength: 500, nullable: true, generator: { type: 'faker', fakerMethod: 'internet.userAgent' } },
    { name: 'created_at', type: 'timestamp', indexed: true, generator: { type: 'timestamp-range', start: '2023-01-01', end: '2024-12-31' } },
    { name: 'updated_at', type: 'timestamp', generator: { type: 'timestamp-range', start: '2023-01-01', end: '2024-12-31' } },
    { name: 'completed_at', type: 'timestamp', nullable: true, indexed: true, generator: { type: 'timestamp-range', start: '2023-01-01', end: '2024-12-31' } },
  ],
  indexes: [
    { name: 'idx_orders_customer', columns: ['customer_id'] },
    { name: 'idx_orders_customer_status', columns: ['customer_id', 'status'] },
    { name: 'idx_orders_status', columns: ['status'] },
    { name: 'idx_orders_created', columns: ['created_at'] },
    { name: 'idx_orders_completed', columns: ['completed_at'], where: 'completed_at IS NOT NULL' },
    { name: 'idx_orders_status_created', columns: ['status', 'created_at'] },
  ],
  partitionBy: {
    type: 'range',
    column: 'created_at',
  },
}

const orderItemsTable: TableConfig = {
  name: 'order_items',
  columns: [
    { name: 'id', type: 'uuid', primaryKey: true, generator: { type: 'uuid' } },
    { name: 'order_id', type: 'uuid', indexed: true, references: { table: 'orders', column: 'id' }, generator: { type: 'reference', referenceTable: 'orders', referenceColumn: 'id' } },
    { name: 'product_id', type: 'uuid', indexed: true, references: { table: 'products', column: 'id' }, generator: { type: 'reference', referenceTable: 'products', referenceColumn: 'id', distribution: 'zipf' } },
    { name: 'sku', type: 'string', maxLength: 50, generator: { type: 'faker', fakerMethod: 'string.alphanumeric' } },
    { name: 'name', type: 'string', maxLength: 255, generator: { type: 'faker', fakerMethod: 'commerce.productName' } },
    { name: 'quantity', type: 'integer', generator: { type: 'random-int', min: 1, max: 10 } },
    { name: 'unit_price', type: 'decimal', precision: 10, scale: 2, generator: { type: 'random-decimal', min: 1, max: 5000, precision: 2 } },
    { name: 'discount_amount', type: 'decimal', precision: 10, scale: 2, default: 0, generator: { type: 'random-decimal', min: 0, max: 100, precision: 2 } },
    { name: 'tax_amount', type: 'decimal', precision: 10, scale: 2, default: 0, generator: { type: 'random-decimal', min: 0, max: 100, precision: 2 } },
    { name: 'total', type: 'decimal', precision: 10, scale: 2, generator: { type: 'random-decimal', min: 1, max: 50000, precision: 2 } },
    { name: 'fulfillment_status', type: 'string', maxLength: 20, default: 'pending', generator: { type: 'weighted-enum', values: ['pending', 'shipped', 'delivered', 'returned'], weights: [0.2, 0.2, 0.55, 0.05] } },
    { name: 'created_at', type: 'timestamp', generator: { type: 'timestamp-range', start: '2023-01-01', end: '2024-12-31' } },
  ],
  indexes: [
    { name: 'idx_order_items_order', columns: ['order_id'] },
    { name: 'idx_order_items_product', columns: ['product_id'] },
  ],
  embedded: ['sku', 'name', 'quantity', 'unit_price', 'discount_amount', 'tax_amount', 'total'],
}

const reviewsTable: TableConfig = {
  name: 'reviews',
  columns: [
    { name: 'id', type: 'uuid', primaryKey: true, generator: { type: 'uuid' } },
    { name: 'product_id', type: 'uuid', indexed: true, references: { table: 'products', column: 'id' }, generator: { type: 'reference', referenceTable: 'products', referenceColumn: 'id', distribution: 'zipf' } },
    { name: 'customer_id', type: 'uuid', indexed: true, references: { table: 'customers', column: 'id' }, generator: { type: 'reference', referenceTable: 'customers', referenceColumn: 'id' } },
    { name: 'order_id', type: 'uuid', nullable: true, indexed: true, references: { table: 'orders', column: 'id' }, generator: { type: 'reference', referenceTable: 'orders', referenceColumn: 'id' } },
    { name: 'rating', type: 'integer', indexed: true, generator: { type: 'weighted-enum', values: [1, 2, 3, 4, 5], weights: [0.05, 0.1, 0.15, 0.3, 0.4] } },
    { name: 'title', type: 'string', maxLength: 255, nullable: true, generator: { type: 'faker', fakerMethod: 'lorem.sentence' } },
    { name: 'body', type: 'text', nullable: true, generator: { type: 'faker', fakerMethod: 'lorem.paragraphs' } },
    { name: 'is_verified_purchase', type: 'boolean', default: false, indexed: true, generator: { type: 'weighted-enum', values: [true, false], weights: [0.7, 0.3] } },
    { name: 'is_featured', type: 'boolean', default: false, generator: { type: 'weighted-enum', values: [true, false], weights: [0.05, 0.95] } },
    { name: 'helpful_count', type: 'integer', default: 0, generator: { type: 'random-int', min: 0, max: 100 } },
    { name: 'not_helpful_count', type: 'integer', default: 0, generator: { type: 'random-int', min: 0, max: 20 } },
    { name: 'status', type: 'string', maxLength: 20, default: 'pending', indexed: true, generator: { type: 'weighted-enum', values: ['pending', 'approved', 'rejected'], weights: [0.1, 0.85, 0.05] } },
    { name: 'created_at', type: 'timestamp', indexed: true, generator: { type: 'timestamp-range', start: '2023-01-01', end: '2024-12-31' } },
    { name: 'updated_at', type: 'timestamp', generator: { type: 'timestamp-range', start: '2023-01-01', end: '2024-12-31' } },
  ],
  indexes: [
    { name: 'idx_reviews_product', columns: ['product_id'] },
    { name: 'idx_reviews_customer', columns: ['customer_id'] },
    { name: 'idx_reviews_product_rating', columns: ['product_id', 'rating'] },
    { name: 'idx_reviews_product_verified', columns: ['product_id', 'is_verified_purchase', 'status'] },
    { name: 'idx_reviews_created', columns: ['created_at'] },
  ],
}

const cartsTable: TableConfig = {
  name: 'carts',
  columns: [
    { name: 'id', type: 'uuid', primaryKey: true, generator: { type: 'uuid' } },
    { name: 'customer_id', type: 'uuid', nullable: true, indexed: true, references: { table: 'customers', column: 'id' }, generator: { type: 'reference', referenceTable: 'customers', referenceColumn: 'id' } },
    { name: 'session_id', type: 'string', maxLength: 100, indexed: true, generator: { type: 'uuid' } },
    { name: 'status', type: 'string', maxLength: 20, default: 'active', indexed: true, generator: { type: 'weighted-enum', values: ['active', 'abandoned', 'converted'], weights: [0.3, 0.5, 0.2] } },
    { name: 'subtotal', type: 'decimal', precision: 12, scale: 2, default: 0, generator: { type: 'random-decimal', min: 0, max: 2000, precision: 2 } },
    { name: 'currency', type: 'string', maxLength: 3, default: 'USD', generator: { type: 'enum', values: ['USD', 'EUR', 'GBP'] } },
    { name: 'item_count', type: 'integer', default: 0, generator: { type: 'random-int', min: 0, max: 20 } },
    { name: 'created_at', type: 'timestamp', generator: { type: 'timestamp-range', start: '2024-01-01', end: '2024-12-31' } },
    { name: 'updated_at', type: 'timestamp', indexed: true, generator: { type: 'timestamp-range', start: '2024-01-01', end: '2024-12-31' } },
    { name: 'expires_at', type: 'timestamp', indexed: true, generator: { type: 'timestamp-range', start: '2024-06-01', end: '2025-06-01' } },
  ],
  indexes: [
    { name: 'idx_carts_customer', columns: ['customer_id'] },
    { name: 'idx_carts_session', columns: ['session_id'] },
    { name: 'idx_carts_status_updated', columns: ['status', 'updated_at'] },
    { name: 'idx_carts_expires', columns: ['expires_at'], where: "status = 'active'" },
  ],
}

const cartItemsTable: TableConfig = {
  name: 'cart_items',
  columns: [
    { name: 'id', type: 'uuid', primaryKey: true, generator: { type: 'uuid' } },
    { name: 'cart_id', type: 'uuid', indexed: true, references: { table: 'carts', column: 'id' }, generator: { type: 'reference', referenceTable: 'carts', referenceColumn: 'id' } },
    { name: 'product_id', type: 'uuid', indexed: true, references: { table: 'products', column: 'id' }, generator: { type: 'reference', referenceTable: 'products', referenceColumn: 'id', distribution: 'zipf' } },
    { name: 'quantity', type: 'integer', generator: { type: 'random-int', min: 1, max: 5 } },
    { name: 'unit_price', type: 'decimal', precision: 10, scale: 2, generator: { type: 'random-decimal', min: 1, max: 1000, precision: 2 } },
    { name: 'created_at', type: 'timestamp', generator: { type: 'timestamp-range', start: '2024-01-01', end: '2024-12-31' } },
    { name: 'updated_at', type: 'timestamp', generator: { type: 'timestamp-range', start: '2024-01-01', end: '2024-12-31' } },
  ],
  indexes: [
    { name: 'idx_cart_items_cart', columns: ['cart_id'] },
    { name: 'idx_cart_items_cart_product', columns: ['cart_id', 'product_id'], unique: true },
  ],
  embedded: ['quantity', 'unit_price'],
}

const inventoryTable: TableConfig = {
  name: 'inventory',
  columns: [
    { name: 'id', type: 'uuid', primaryKey: true, generator: { type: 'uuid' } },
    { name: 'product_id', type: 'uuid', indexed: true, references: { table: 'products', column: 'id' }, generator: { type: 'reference', referenceTable: 'products', referenceColumn: 'id' } },
    { name: 'warehouse_id', type: 'string', maxLength: 50, indexed: true, generator: { type: 'enum', values: ['WH-EAST', 'WH-WEST', 'WH-CENTRAL', 'WH-SOUTH'] } },
    { name: 'quantity_available', type: 'integer', generator: { type: 'random-int', min: 0, max: 500 } },
    { name: 'quantity_reserved', type: 'integer', default: 0, generator: { type: 'random-int', min: 0, max: 50 } },
    { name: 'quantity_incoming', type: 'integer', default: 0, generator: { type: 'random-int', min: 0, max: 100 } },
    { name: 'reorder_point', type: 'integer', default: 10, generator: { type: 'random-int', min: 5, max: 50 } },
    { name: 'reorder_quantity', type: 'integer', default: 100, generator: { type: 'random-int', min: 50, max: 500 } },
    { name: 'last_counted_at', type: 'timestamp', nullable: true, generator: { type: 'timestamp-range', start: '2024-01-01', end: '2024-12-31' } },
    { name: 'updated_at', type: 'timestamp', generator: { type: 'timestamp-range', start: '2024-01-01', end: '2024-12-31' } },
  ],
  indexes: [
    { name: 'idx_inventory_product', columns: ['product_id'] },
    { name: 'idx_inventory_product_warehouse', columns: ['product_id', 'warehouse_id'], unique: true },
    { name: 'idx_inventory_warehouse', columns: ['warehouse_id'] },
    { name: 'idx_inventory_low_stock', columns: ['warehouse_id', 'quantity_available'], where: 'quantity_available < reorder_point' },
  ],
}

// ============================================================================
// Tables Array
// ============================================================================

const tables: TableConfig[] = [
  customersTable,
  addressesTable,
  categoriesTable,
  productsTable,
  productImagesTable,
  ordersTable,
  orderItemsTable,
  reviewsTable,
  cartsTable,
  cartItemsTable,
  inventoryTable,
]

// ============================================================================
// Relationships
// ============================================================================

const relationships: RelationshipConfig[] = [
  {
    name: 'customer_addresses',
    type: 'one-to-many',
    from: { table: 'customers', column: 'id' },
    to: { table: 'addresses', column: 'customer_id' },
    onDelete: 'cascade',
  },
  {
    name: 'category_parent',
    type: 'one-to-many',
    from: { table: 'categories', column: 'id' },
    to: { table: 'categories', column: 'parent_id' },
    onDelete: 'set-null',
  },
  {
    name: 'product_category',
    type: 'one-to-many',
    from: { table: 'categories', column: 'id' },
    to: { table: 'products', column: 'category_id' },
    onDelete: 'restrict',
  },
  {
    name: 'product_images',
    type: 'one-to-many',
    from: { table: 'products', column: 'id' },
    to: { table: 'product_images', column: 'product_id' },
    onDelete: 'cascade',
    embed: true,
  },
  {
    name: 'customer_orders',
    type: 'one-to-many',
    from: { table: 'customers', column: 'id' },
    to: { table: 'orders', column: 'customer_id' },
    onDelete: 'restrict',
  },
  {
    name: 'order_items',
    type: 'one-to-many',
    from: { table: 'orders', column: 'id' },
    to: { table: 'order_items', column: 'order_id' },
    onDelete: 'cascade',
    embed: true,
  },
  {
    name: 'order_item_product',
    type: 'one-to-many',
    from: { table: 'products', column: 'id' },
    to: { table: 'order_items', column: 'product_id' },
    onDelete: 'restrict',
  },
  {
    name: 'product_reviews',
    type: 'one-to-many',
    from: { table: 'products', column: 'id' },
    to: { table: 'reviews', column: 'product_id' },
    onDelete: 'cascade',
  },
  {
    name: 'customer_reviews',
    type: 'one-to-many',
    from: { table: 'customers', column: 'id' },
    to: { table: 'reviews', column: 'customer_id' },
    onDelete: 'cascade',
  },
  {
    name: 'customer_carts',
    type: 'one-to-many',
    from: { table: 'customers', column: 'id' },
    to: { table: 'carts', column: 'customer_id' },
    onDelete: 'cascade',
  },
  {
    name: 'cart_items',
    type: 'one-to-many',
    from: { table: 'carts', column: 'id' },
    to: { table: 'cart_items', column: 'cart_id' },
    onDelete: 'cascade',
    embed: true,
  },
  {
    name: 'product_inventory',
    type: 'one-to-many',
    from: { table: 'products', column: 'id' },
    to: { table: 'inventory', column: 'product_id' },
    onDelete: 'cascade',
  },
]

// ============================================================================
// Size Tiers
// ============================================================================

const sizeTiers: SizeTierConfig[] = [
  {
    size: '1mb',
    seedCount: {
      customers: 200,
      addresses: 350,
      categories: 50,
      products: 500,
      product_images: 1500,
      orders: 1000,
      order_items: 3000,
      reviews: 800,
      carts: 300,
      cart_items: 600,
      inventory: 2000,
    },
    estimatedBytes: 1_048_576, // 1 MB
    recommendedMemoryMB: 128,
  },
  {
    size: '10mb',
    seedCount: {
      customers: 2000,
      addresses: 3500,
      categories: 100,
      products: 5000,
      product_images: 15000,
      orders: 10000,
      order_items: 30000,
      reviews: 8000,
      carts: 3000,
      cart_items: 6000,
      inventory: 20000,
    },
    estimatedBytes: 10_485_760, // 10 MB
    recommendedMemoryMB: 256,
  },
  {
    size: '100mb',
    seedCount: {
      customers: 20000,
      addresses: 35000,
      categories: 200,
      products: 50000,
      product_images: 150000,
      orders: 100000,
      order_items: 300000,
      reviews: 80000,
      carts: 30000,
      cart_items: 60000,
      inventory: 200000,
    },
    estimatedBytes: 104_857_600, // 100 MB
    recommendedMemoryMB: 512,
  },
  {
    size: '1gb',
    seedCount: {
      customers: 200000,
      addresses: 350000,
      categories: 500,
      products: 500000,
      product_images: 1500000,
      orders: 1000000,
      order_items: 3000000,
      reviews: 800000,
      carts: 300000,
      cart_items: 600000,
      inventory: 2000000,
    },
    estimatedBytes: 1_073_741_824, // 1 GB
    recommendedMemoryMB: 2048,
    recommendedCores: 2,
  },
  {
    size: '10gb',
    seedCount: {
      customers: 2000000,
      addresses: 3500000,
      categories: 1000,
      products: 5000000,
      product_images: 15000000,
      orders: 10000000,
      order_items: 30000000,
      reviews: 8000000,
      carts: 3000000,
      cart_items: 6000000,
      inventory: 20000000,
    },
    estimatedBytes: 10_737_418_240, // 10 GB
    recommendedMemoryMB: 8192,
    recommendedCores: 4,
  },
  {
    size: '20gb',
    seedCount: {
      customers: 4000000,
      addresses: 7000000,
      categories: 1500,
      products: 10000000,
      product_images: 30000000,
      orders: 20000000,
      order_items: 60000000,
      reviews: 16000000,
      carts: 6000000,
      cart_items: 12000000,
      inventory: 40000000,
    },
    estimatedBytes: 21_474_836_480, // 20 GB
    recommendedMemoryMB: 16384,
    recommendedCores: 8,
  },
  {
    size: '30gb',
    seedCount: {
      customers: 6000000,
      addresses: 10500000,
      categories: 2000,
      products: 15000000,
      product_images: 45000000,
      orders: 30000000,
      order_items: 90000000,
      reviews: 24000000,
      carts: 9000000,
      cart_items: 18000000,
      inventory: 60000000,
    },
    estimatedBytes: 32_212_254_720, // 30 GB
    recommendedMemoryMB: 24576,
    recommendedCores: 12,
  },
  {
    size: '50gb',
    seedCount: {
      customers: 10000000,
      addresses: 17500000,
      categories: 3000,
      products: 25000000,
      product_images: 75000000,
      orders: 50000000,
      order_items: 150000000,
      reviews: 40000000,
      carts: 15000000,
      cart_items: 30000000,
      inventory: 100000000,
    },
    estimatedBytes: 53_687_091_200, // 50 GB
    recommendedMemoryMB: 32768,
    recommendedCores: 16,
  },
]

// ============================================================================
// Benchmark Queries
// ============================================================================

const benchmarkQueries: BenchmarkQuery[] = [
  // Point lookups
  {
    name: 'get_customer_by_id',
    description: 'Fetch a single customer by primary key',
    category: 'point-lookup',
    sql: 'SELECT * FROM customers WHERE id = $1',
    documentQuery: {
      collection: 'customers',
      operation: 'find',
      filter: { _id: '$1' },
    },
    parameters: [{ type: 'reference', referenceTable: 'customers', referenceColumn: 'id' }],
    expectedComplexity: 'O(1)',
    weight: 15,
  },
  {
    name: 'get_customer_by_email',
    description: 'Fetch a customer by email (unique index)',
    category: 'point-lookup',
    sql: 'SELECT * FROM customers WHERE email = $1',
    documentQuery: {
      collection: 'customers',
      operation: 'find',
      filter: { email: '$1' },
    },
    parameters: [{ type: 'faker', fakerMethod: 'internet.email' }],
    expectedComplexity: 'O(1)',
    weight: 10,
  },
  {
    name: 'get_product_by_sku',
    description: 'Fetch a product by SKU (unique index)',
    category: 'point-lookup',
    sql: 'SELECT * FROM products WHERE sku = $1',
    documentQuery: {
      collection: 'products',
      operation: 'find',
      filter: { sku: '$1' },
    },
    parameters: [{ type: 'reference', referenceTable: 'products', referenceColumn: 'sku' }],
    expectedComplexity: 'O(1)',
    weight: 10,
  },
  {
    name: 'get_order_by_number',
    description: 'Fetch an order by order number',
    category: 'point-lookup',
    sql: 'SELECT * FROM orders WHERE order_number = $1',
    documentQuery: {
      collection: 'orders',
      operation: 'find',
      filter: { order_number: '$1' },
    },
    parameters: [{ type: 'reference', referenceTable: 'orders', referenceColumn: 'order_number' }],
    expectedComplexity: 'O(1)',
    weight: 8,
  },

  // Range scans
  {
    name: 'list_products_by_category',
    description: 'List active products in a category with pagination',
    category: 'range-scan',
    sql: `SELECT * FROM products
          WHERE category_id = $1 AND is_active = true
          ORDER BY created_at DESC
          LIMIT 20 OFFSET $2`,
    documentQuery: {
      collection: 'products',
      operation: 'find',
      filter: { category_id: '$1', is_active: true },
    },
    parameters: [
      { type: 'reference', referenceTable: 'categories', referenceColumn: 'id' },
      { type: 'random-int', min: 0, max: 100 },
    ],
    expectedComplexity: 'O(log n)',
    weight: 12,
  },
  {
    name: 'list_products_by_price_range',
    description: 'List products within a price range',
    category: 'range-scan',
    sql: `SELECT * FROM products
          WHERE price BETWEEN $1 AND $2 AND is_active = true
          ORDER BY price ASC
          LIMIT 50`,
    documentQuery: {
      collection: 'products',
      operation: 'find',
      filter: { price: { $gte: '$1', $lte: '$2' }, is_active: true },
    },
    parameters: [
      { type: 'random-decimal', min: 10, max: 100, precision: 2 },
      { type: 'random-decimal', min: 100, max: 1000, precision: 2 },
    ],
    expectedComplexity: 'O(log n)',
    weight: 5,
  },
  {
    name: 'list_customer_orders',
    description: 'List recent orders for a customer',
    category: 'range-scan',
    sql: `SELECT * FROM orders
          WHERE customer_id = $1
          ORDER BY created_at DESC
          LIMIT 10`,
    documentQuery: {
      collection: 'orders',
      operation: 'find',
      filter: { customer_id: '$1' },
    },
    parameters: [{ type: 'reference', referenceTable: 'customers', referenceColumn: 'id', distribution: 'zipf' }],
    expectedComplexity: 'O(log n)',
    weight: 8,
  },
  {
    name: 'list_orders_by_status',
    description: 'List orders by status with date range',
    category: 'range-scan',
    sql: `SELECT * FROM orders
          WHERE status = $1 AND created_at >= $2 AND created_at < $3
          ORDER BY created_at DESC
          LIMIT 100`,
    documentQuery: {
      collection: 'orders',
      operation: 'find',
      filter: { status: '$1', created_at: { $gte: '$2', $lt: '$3' } },
    },
    parameters: [
      { type: 'enum', values: ['pending', 'processing', 'shipped'] },
      { type: 'timestamp-range', start: '2024-01-01', end: '2024-06-01' },
      { type: 'timestamp-range', start: '2024-06-01', end: '2024-12-31' },
    ],
    expectedComplexity: 'O(log n)',
    weight: 5,
  },
  {
    name: 'list_product_reviews',
    description: 'List approved reviews for a product',
    category: 'range-scan',
    sql: `SELECT * FROM reviews
          WHERE product_id = $1 AND status = 'approved'
          ORDER BY created_at DESC
          LIMIT 20`,
    documentQuery: {
      collection: 'reviews',
      operation: 'find',
      filter: { product_id: '$1', status: 'approved' },
    },
    parameters: [{ type: 'reference', referenceTable: 'products', referenceColumn: 'id', distribution: 'zipf' }],
    expectedComplexity: 'O(log n)',
    weight: 6,
  },

  // Joins
  {
    name: 'get_order_with_items',
    description: 'Fetch order with all line items',
    category: 'join',
    sql: `SELECT o.*, oi.*
          FROM orders o
          JOIN order_items oi ON o.id = oi.order_id
          WHERE o.id = $1`,
    documentQuery: {
      collection: 'orders',
      operation: 'find',
      filter: { _id: '$1' },
    },
    parameters: [{ type: 'reference', referenceTable: 'orders', referenceColumn: 'id' }],
    expectedComplexity: 'O(log n)',
    weight: 8,
  },
  {
    name: 'get_product_with_images',
    description: 'Fetch product with all images',
    category: 'join',
    sql: `SELECT p.*, pi.*
          FROM products p
          LEFT JOIN product_images pi ON p.id = pi.product_id
          WHERE p.id = $1
          ORDER BY pi.sort_order`,
    documentQuery: {
      collection: 'products',
      operation: 'find',
      filter: { _id: '$1' },
    },
    parameters: [{ type: 'reference', referenceTable: 'products', referenceColumn: 'id' }],
    expectedComplexity: 'O(log n)',
    weight: 5,
  },
  {
    name: 'get_customer_with_addresses',
    description: 'Fetch customer with all addresses',
    category: 'join',
    sql: `SELECT c.*, a.*
          FROM customers c
          LEFT JOIN addresses a ON c.id = a.customer_id
          WHERE c.id = $1`,
    documentQuery: {
      collection: 'customers',
      operation: 'find',
      filter: { _id: '$1' },
    },
    parameters: [{ type: 'reference', referenceTable: 'customers', referenceColumn: 'id' }],
    expectedComplexity: 'O(log n)',
    weight: 4,
  },

  // Aggregates
  {
    name: 'count_orders_by_status',
    description: 'Count orders grouped by status',
    category: 'aggregate',
    sql: `SELECT status, COUNT(*) as count, SUM(total) as total_revenue
          FROM orders
          WHERE created_at >= $1
          GROUP BY status`,
    documentQuery: {
      collection: 'orders',
      operation: 'aggregate',
      pipeline: [
        { $match: { created_at: { $gte: '$1' } } },
        { $group: { _id: '$status', count: { $sum: 1 }, total_revenue: { $sum: '$total' } } },
      ],
    },
    parameters: [{ type: 'timestamp-range', start: '2024-01-01', end: '2024-06-01' }],
    expectedComplexity: 'O(n)',
    weight: 3,
  },
  {
    name: 'top_selling_products',
    description: 'Find top selling products by quantity',
    category: 'aggregate',
    sql: `SELECT p.id, p.name, SUM(oi.quantity) as total_sold
          FROM products p
          JOIN order_items oi ON p.id = oi.product_id
          JOIN orders o ON oi.order_id = o.id
          WHERE o.created_at >= $1 AND o.status NOT IN ('cancelled', 'refunded')
          GROUP BY p.id, p.name
          ORDER BY total_sold DESC
          LIMIT 20`,
    documentQuery: {
      collection: 'order_items',
      operation: 'aggregate',
      pipeline: [
        { $group: { _id: '$product_id', total_sold: { $sum: '$quantity' } } },
        { $sort: { total_sold: -1 } },
        { $limit: 20 },
      ],
    },
    parameters: [{ type: 'timestamp-range', start: '2024-01-01', end: '2024-06-01' }],
    expectedComplexity: 'O(n)',
    weight: 2,
  },
  {
    name: 'customer_lifetime_value',
    description: 'Calculate customer lifetime value',
    category: 'aggregate',
    sql: `SELECT c.id, c.email, c.tier,
            COUNT(o.id) as order_count,
            SUM(o.total) as lifetime_value,
            AVG(o.total) as avg_order_value
          FROM customers c
          LEFT JOIN orders o ON c.id = o.customer_id
          WHERE c.id = $1
          GROUP BY c.id, c.email, c.tier`,
    documentQuery: {
      collection: 'orders',
      operation: 'aggregate',
      pipeline: [
        { $match: { customer_id: '$1' } },
        { $group: { _id: '$customer_id', order_count: { $sum: 1 }, lifetime_value: { $sum: '$total' }, avg_order_value: { $avg: '$total' } } },
      ],
    },
    parameters: [{ type: 'reference', referenceTable: 'customers', referenceColumn: 'id' }],
    expectedComplexity: 'O(log n)',
    weight: 2,
  },
  {
    name: 'product_rating_summary',
    description: 'Get rating distribution for a product',
    category: 'aggregate',
    sql: `SELECT rating, COUNT(*) as count
          FROM reviews
          WHERE product_id = $1 AND status = 'approved'
          GROUP BY rating
          ORDER BY rating`,
    documentQuery: {
      collection: 'reviews',
      operation: 'aggregate',
      pipeline: [
        { $match: { product_id: '$1', status: 'approved' } },
        { $group: { _id: '$rating', count: { $sum: 1 } } },
        { $sort: { _id: 1 } },
      ],
    },
    parameters: [{ type: 'reference', referenceTable: 'products', referenceColumn: 'id', distribution: 'zipf' }],
    expectedComplexity: 'O(log n)',
    weight: 2,
  },
  {
    name: 'inventory_alerts',
    description: 'Find products with low inventory',
    category: 'aggregate',
    sql: `SELECT p.id, p.name, p.sku, i.warehouse_id, i.quantity_available, i.reorder_point
          FROM products p
          JOIN inventory i ON p.id = i.product_id
          WHERE i.quantity_available <= i.reorder_point AND p.is_active = true
          ORDER BY i.quantity_available ASC
          LIMIT 100`,
    documentQuery: {
      collection: 'inventory',
      operation: 'aggregate',
      pipeline: [
        { $match: { $expr: { $lte: ['$quantity_available', '$reorder_point'] } } },
        { $sort: { quantity_available: 1 } },
        { $limit: 100 },
      ],
    },
    parameters: [],
    expectedComplexity: 'O(n)',
    weight: 1,
  },

  // Write operations
  {
    name: 'create_order',
    description: 'Create a new order',
    category: 'write',
    sql: `INSERT INTO orders (id, order_number, customer_id, status, payment_status, fulfillment_status, subtotal, total, currency, created_at, updated_at)
          VALUES ($1, $2, $3, 'pending', 'pending', 'unfulfilled', $4, $5, 'USD', NOW(), NOW())
          RETURNING *`,
    documentQuery: {
      collection: 'orders',
      operation: 'insert',
    },
    parameters: [
      { type: 'uuid' },
      { type: 'sequence' },
      { type: 'reference', referenceTable: 'customers', referenceColumn: 'id' },
      { type: 'random-decimal', min: 10, max: 500, precision: 2 },
      { type: 'random-decimal', min: 10, max: 600, precision: 2 },
    ],
    expectedComplexity: 'O(log n)',
    weight: 3,
  },
  {
    name: 'add_order_item',
    description: 'Add an item to an order',
    category: 'write',
    sql: `INSERT INTO order_items (id, order_id, product_id, sku, name, quantity, unit_price, total, created_at)
          VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW())
          RETURNING *`,
    documentQuery: {
      collection: 'order_items',
      operation: 'insert',
    },
    parameters: [
      { type: 'uuid' },
      { type: 'reference', referenceTable: 'orders', referenceColumn: 'id' },
      { type: 'reference', referenceTable: 'products', referenceColumn: 'id' },
      { type: 'random-string', length: 10 },
      { type: 'faker', fakerMethod: 'commerce.productName' },
      { type: 'random-int', min: 1, max: 5 },
      { type: 'random-decimal', min: 10, max: 500, precision: 2 },
      { type: 'random-decimal', min: 10, max: 2500, precision: 2 },
    ],
    expectedComplexity: 'O(log n)',
    weight: 2,
  },
  {
    name: 'update_order_status',
    description: 'Update order status',
    category: 'write',
    sql: `UPDATE orders
          SET status = $2, updated_at = NOW()
          WHERE id = $1
          RETURNING *`,
    documentQuery: {
      collection: 'orders',
      operation: 'update',
      filter: { _id: '$1' },
    },
    parameters: [
      { type: 'reference', referenceTable: 'orders', referenceColumn: 'id' },
      { type: 'enum', values: ['confirmed', 'processing', 'shipped', 'delivered'] },
    ],
    expectedComplexity: 'O(log n)',
    weight: 2,
  },
  {
    name: 'update_inventory',
    description: 'Decrement inventory after order',
    category: 'write',
    sql: `UPDATE inventory
          SET quantity_available = quantity_available - $3, updated_at = NOW()
          WHERE product_id = $1 AND warehouse_id = $2 AND quantity_available >= $3
          RETURNING *`,
    documentQuery: {
      collection: 'inventory',
      operation: 'update',
      filter: { product_id: '$1', warehouse_id: '$2' },
    },
    parameters: [
      { type: 'reference', referenceTable: 'products', referenceColumn: 'id' },
      { type: 'enum', values: ['WH-EAST', 'WH-WEST', 'WH-CENTRAL', 'WH-SOUTH'] },
      { type: 'random-int', min: 1, max: 5 },
    ],
    expectedComplexity: 'O(log n)',
    weight: 1,
  },
  {
    name: 'add_to_cart',
    description: 'Add or update item in shopping cart',
    category: 'write',
    sql: `INSERT INTO cart_items (id, cart_id, product_id, quantity, unit_price, created_at, updated_at)
          VALUES ($1, $2, $3, $4, $5, NOW(), NOW())
          ON CONFLICT (cart_id, product_id)
          DO UPDATE SET quantity = cart_items.quantity + $4, updated_at = NOW()
          RETURNING *`,
    documentQuery: {
      collection: 'cart_items',
      operation: 'update',
      filter: { cart_id: '$2', product_id: '$3' },
    },
    parameters: [
      { type: 'uuid' },
      { type: 'reference', referenceTable: 'carts', referenceColumn: 'id' },
      { type: 'reference', referenceTable: 'products', referenceColumn: 'id', distribution: 'zipf' },
      { type: 'random-int', min: 1, max: 3 },
      { type: 'random-decimal', min: 10, max: 500, precision: 2 },
    ],
    expectedComplexity: 'O(log n)',
    weight: 2,
  },
  {
    name: 'create_review',
    description: 'Submit a product review',
    category: 'write',
    sql: `INSERT INTO reviews (id, product_id, customer_id, rating, title, body, is_verified_purchase, status, created_at, updated_at)
          VALUES ($1, $2, $3, $4, $5, $6, $7, 'pending', NOW(), NOW())
          RETURNING *`,
    documentQuery: {
      collection: 'reviews',
      operation: 'insert',
    },
    parameters: [
      { type: 'uuid' },
      { type: 'reference', referenceTable: 'products', referenceColumn: 'id', distribution: 'zipf' },
      { type: 'reference', referenceTable: 'customers', referenceColumn: 'id' },
      { type: 'random-int', min: 1, max: 5 },
      { type: 'faker', fakerMethod: 'lorem.sentence' },
      { type: 'faker', fakerMethod: 'lorem.paragraph' },
      { type: 'random-boolean' },
    ],
    expectedComplexity: 'O(log n)',
    weight: 1,
  },
]

// ============================================================================
// Workload Profiles
// ============================================================================

const workloads: WorkloadProfile[] = [
  {
    name: 'read_heavy',
    description: 'Typical e-commerce read-heavy workload (90% reads, 10% writes)',
    readWriteRatio: 0.9,
    queries: benchmarkQueries.filter(q => q.category !== 'write').map(q => ({ ...q, weight: q.weight })),
    targetOps: 10000,
    concurrency: 100,
    duration: 300,
  },
  {
    name: 'write_heavy',
    description: 'High-traffic checkout scenario (50% reads, 50% writes)',
    readWriteRatio: 0.5,
    queries: benchmarkQueries,
    targetOps: 5000,
    concurrency: 50,
    duration: 300,
  },
  {
    name: 'catalog_browse',
    description: 'Product catalog browsing workload (99% reads)',
    readWriteRatio: 0.99,
    queries: benchmarkQueries.filter(q =>
      ['list_products_by_category', 'list_products_by_price_range', 'get_product_by_sku', 'get_product_with_images', 'list_product_reviews'].includes(q.name)
    ),
    targetOps: 20000,
    concurrency: 200,
    duration: 300,
  },
  {
    name: 'order_processing',
    description: 'Order placement and processing workload',
    readWriteRatio: 0.6,
    queries: benchmarkQueries.filter(q =>
      ['get_customer_by_id', 'get_order_by_number', 'get_order_with_items', 'list_customer_orders', 'create_order', 'add_order_item', 'update_order_status', 'update_inventory'].includes(q.name)
    ),
    targetOps: 3000,
    concurrency: 30,
    duration: 300,
  },
  {
    name: 'analytics',
    description: 'Business analytics queries (aggregations)',
    readWriteRatio: 1.0,
    queries: benchmarkQueries.filter(q => q.category === 'aggregate'),
    targetOps: 100,
    concurrency: 10,
    duration: 300,
  },
]

// ============================================================================
// Dataset Configuration
// ============================================================================

export const ecommerceDataset: DatasetConfig = {
  name: 'ecommerce',
  description: 'E-commerce OLTP dataset with orders, products, customers, and reviews',
  version: '1.0.0',
  tables,
  relationships,
  sizeTiers,
  workloads,
  metadata: {
    domain: 'retail',
    characteristics: [
      'High read volume on product catalog',
      'Zipf distribution on popular products',
      'Seasonal order patterns',
      'Multi-warehouse inventory',
    ],
  },
}

// Register the dataset
registerDataset(ecommerceDataset)
