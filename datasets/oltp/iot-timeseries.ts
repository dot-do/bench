/**
 * IoT Timeseries OLTP Dataset
 *
 * A comprehensive IoT sensor data dataset with devices, readings, and alerts.
 * Represents a typical IoT/timeseries workload with:
 * - High-volume sensor data ingestion
 * - Time-range queries and aggregations
 * - Device fleet management
 * - Anomaly detection and alerting
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

const organizationsTable: TableConfig = {
  name: 'organizations',
  columns: [
    { name: 'id', type: 'uuid', primaryKey: true, generator: { type: 'uuid' } },
    { name: 'name', type: 'string', maxLength: 255, indexed: true, generator: { type: 'faker', fakerMethod: 'company.name' } },
    { name: 'slug', type: 'string', maxLength: 100, unique: true, generator: { type: 'faker', fakerMethod: 'helpers.slugify' } },
    { name: 'plan', type: 'string', maxLength: 50, indexed: true, generator: { type: 'weighted-enum', values: ['free', 'starter', 'professional', 'enterprise'], weights: [0.3, 0.35, 0.25, 0.1] } },
    { name: 'max_devices', type: 'integer', generator: { type: 'weighted-enum', values: [10, 100, 1000, 10000], weights: [0.3, 0.35, 0.25, 0.1] } },
    { name: 'retention_days', type: 'integer', default: 30, generator: { type: 'weighted-enum', values: [7, 30, 90, 365], weights: [0.2, 0.4, 0.25, 0.15] } },
    { name: 'settings', type: 'json', nullable: true },
    { name: 'created_at', type: 'timestamp', generator: { type: 'timestamp-range', start: '2020-01-01', end: '2024-12-31' } },
    { name: 'updated_at', type: 'timestamp', generator: { type: 'timestamp-range', start: '2023-01-01', end: '2024-12-31' } },
  ],
  indexes: [
    { name: 'idx_organizations_slug', columns: ['slug'] },
    { name: 'idx_organizations_plan', columns: ['plan'] },
  ],
}

const locationsTable: TableConfig = {
  name: 'locations',
  columns: [
    { name: 'id', type: 'uuid', primaryKey: true, generator: { type: 'uuid' } },
    { name: 'org_id', type: 'uuid', indexed: true, references: { table: 'organizations', column: 'id' }, generator: { type: 'reference', referenceTable: 'organizations', referenceColumn: 'id' } },
    { name: 'parent_id', type: 'uuid', nullable: true, indexed: true, references: { table: 'locations', column: 'id' }, generator: { type: 'reference', referenceTable: 'locations', referenceColumn: 'id', distribution: 'zipf' } },
    { name: 'name', type: 'string', maxLength: 255, indexed: true, generator: { type: 'faker', fakerMethod: 'location.city' } },
    { name: 'type', type: 'string', maxLength: 50, indexed: true, generator: { type: 'enum', values: ['building', 'floor', 'room', 'zone', 'outdoor', 'vehicle'] } },
    { name: 'address', type: 'text', nullable: true, generator: { type: 'faker', fakerMethod: 'location.streetAddress' } },
    { name: 'latitude', type: 'decimal', precision: 10, scale: 8, nullable: true, indexed: true, generator: { type: 'random-decimal', min: -90, max: 90, precision: 8 } },
    { name: 'longitude', type: 'decimal', precision: 11, scale: 8, nullable: true, indexed: true, generator: { type: 'random-decimal', min: -180, max: 180, precision: 8 } },
    { name: 'timezone', type: 'string', maxLength: 50, default: 'UTC', generator: { type: 'enum', values: ['UTC', 'America/New_York', 'America/Los_Angeles', 'Europe/London', 'Asia/Tokyo'] } },
    { name: 'metadata', type: 'json', nullable: true },
    { name: 'created_at', type: 'timestamp', generator: { type: 'timestamp-range', start: '2020-01-01', end: '2024-12-31' } },
  ],
  indexes: [
    { name: 'idx_locations_org', columns: ['org_id'] },
    { name: 'idx_locations_parent', columns: ['parent_id'] },
    { name: 'idx_locations_type', columns: ['org_id', 'type'] },
    { name: 'idx_locations_geo', columns: ['latitude', 'longitude'] },
  ],
}

const deviceTypesTable: TableConfig = {
  name: 'device_types',
  columns: [
    { name: 'id', type: 'uuid', primaryKey: true, generator: { type: 'uuid' } },
    { name: 'org_id', type: 'uuid', nullable: true, indexed: true, references: { table: 'organizations', column: 'id' }, generator: { type: 'reference', referenceTable: 'organizations', referenceColumn: 'id' } },
    { name: 'name', type: 'string', maxLength: 100, indexed: true, generator: { type: 'enum', values: ['temperature_sensor', 'humidity_sensor', 'pressure_sensor', 'motion_detector', 'smart_meter', 'air_quality', 'vibration_sensor', 'flow_meter', 'level_sensor', 'gateway'] } },
    { name: 'manufacturer', type: 'string', maxLength: 100, nullable: true, generator: { type: 'faker', fakerMethod: 'company.name' } },
    { name: 'model', type: 'string', maxLength: 100, nullable: true, generator: { type: 'faker', fakerMethod: 'string.alphanumeric' } },
    { name: 'category', type: 'string', maxLength: 50, indexed: true, generator: { type: 'enum', values: ['sensor', 'actuator', 'gateway', 'controller', 'meter'] } },
    { name: 'schema', type: 'json', generator: { type: 'faker', fakerMethod: 'datatype.json' } },
    { name: 'default_interval_seconds', type: 'integer', default: 60, generator: { type: 'weighted-enum', values: [1, 5, 15, 60, 300, 900, 3600], weights: [0.05, 0.1, 0.2, 0.3, 0.2, 0.1, 0.05] } },
    { name: 'is_builtin', type: 'boolean', default: false, generator: { type: 'weighted-enum', values: [true, false], weights: [0.3, 0.7] } },
    { name: 'created_at', type: 'timestamp', generator: { type: 'timestamp-range', start: '2019-01-01', end: '2024-12-31' } },
  ],
  indexes: [
    { name: 'idx_device_types_org', columns: ['org_id'] },
    { name: 'idx_device_types_name', columns: ['name'] },
    { name: 'idx_device_types_category', columns: ['category'] },
  ],
}

const devicesTable: TableConfig = {
  name: 'devices',
  columns: [
    { name: 'id', type: 'uuid', primaryKey: true, generator: { type: 'uuid' } },
    { name: 'org_id', type: 'uuid', indexed: true, references: { table: 'organizations', column: 'id' }, generator: { type: 'reference', referenceTable: 'organizations', referenceColumn: 'id' } },
    { name: 'device_type_id', type: 'uuid', indexed: true, references: { table: 'device_types', column: 'id' }, generator: { type: 'reference', referenceTable: 'device_types', referenceColumn: 'id' } },
    { name: 'location_id', type: 'uuid', nullable: true, indexed: true, references: { table: 'locations', column: 'id' }, generator: { type: 'reference', referenceTable: 'locations', referenceColumn: 'id' } },
    { name: 'serial_number', type: 'string', maxLength: 100, unique: true, indexed: true, generator: { type: 'faker', fakerMethod: 'string.alphanumeric' } },
    { name: 'name', type: 'string', maxLength: 255, indexed: true, generator: { type: 'faker', fakerMethod: 'lorem.words' } },
    { name: 'description', type: 'text', nullable: true, generator: { type: 'faker', fakerMethod: 'lorem.sentence' } },
    { name: 'firmware_version', type: 'string', maxLength: 50, nullable: true, generator: { type: 'faker', fakerMethod: 'system.semver' } },
    { name: 'status', type: 'string', maxLength: 20, indexed: true, generator: { type: 'weighted-enum', values: ['online', 'offline', 'maintenance', 'error', 'provisioning'], weights: [0.7, 0.15, 0.05, 0.05, 0.05] } },
    { name: 'is_active', type: 'boolean', default: true, indexed: true, generator: { type: 'weighted-enum', values: [true, false], weights: [0.9, 0.1] } },
    { name: 'report_interval_seconds', type: 'integer', default: 60, generator: { type: 'weighted-enum', values: [1, 5, 15, 60, 300, 900], weights: [0.05, 0.1, 0.2, 0.4, 0.15, 0.1] } },
    { name: 'battery_level', type: 'integer', nullable: true, generator: { type: 'random-int', min: 0, max: 100 } },
    { name: 'signal_strength', type: 'integer', nullable: true, generator: { type: 'random-int', min: -100, max: 0 } },
    { name: 'ip_address', type: 'string', maxLength: 45, nullable: true, generator: { type: 'faker', fakerMethod: 'internet.ip' } },
    { name: 'mac_address', type: 'string', maxLength: 17, nullable: true, generator: { type: 'faker', fakerMethod: 'internet.mac' } },
    { name: 'tags', type: 'array', nullable: true, generator: { type: 'faker', fakerMethod: 'helpers.arrayElements' } },
    { name: 'config', type: 'json', nullable: true },
    { name: 'last_seen_at', type: 'timestamp', nullable: true, indexed: true, generator: { type: 'timestamp-range', start: '2024-01-01', end: '2024-12-31' } },
    { name: 'registered_at', type: 'timestamp', indexed: true, generator: { type: 'timestamp-range', start: '2020-01-01', end: '2024-12-31' } },
    { name: 'updated_at', type: 'timestamp', generator: { type: 'timestamp-range', start: '2024-01-01', end: '2024-12-31' } },
  ],
  indexes: [
    { name: 'idx_devices_org', columns: ['org_id'] },
    { name: 'idx_devices_serial', columns: ['serial_number'] },
    { name: 'idx_devices_type', columns: ['device_type_id'] },
    { name: 'idx_devices_location', columns: ['location_id'] },
    { name: 'idx_devices_status', columns: ['org_id', 'status'] },
    { name: 'idx_devices_active', columns: ['org_id', 'is_active'] },
    { name: 'idx_devices_last_seen', columns: ['last_seen_at'] },
    { name: 'idx_devices_org_type', columns: ['org_id', 'device_type_id'] },
  ],
}

const readingsTable: TableConfig = {
  name: 'readings',
  columns: [
    { name: 'id', type: 'bigint', primaryKey: true, generator: { type: 'sequence' } },
    { name: 'device_id', type: 'uuid', indexed: true, references: { table: 'devices', column: 'id' }, generator: { type: 'reference', referenceTable: 'devices', referenceColumn: 'id', distribution: 'uniform' } },
    { name: 'timestamp', type: 'timestamp', indexed: true, generator: { type: 'timestamp-range', start: '2024-01-01', end: '2024-12-31' } },
    { name: 'metric_name', type: 'string', maxLength: 100, indexed: true, generator: { type: 'enum', values: ['temperature', 'humidity', 'pressure', 'co2', 'pm25', 'voltage', 'current', 'power', 'flow_rate', 'vibration'] } },
    { name: 'value', type: 'decimal', precision: 20, scale: 6, indexed: true, generator: { type: 'random-decimal', min: -100, max: 1000, precision: 6 } },
    { name: 'unit', type: 'string', maxLength: 20, generator: { type: 'enum', values: ['celsius', 'fahrenheit', 'percent', 'hPa', 'ppm', 'ug/m3', 'volts', 'amps', 'watts', 'l/min', 'mm/s'] } },
    { name: 'quality', type: 'string', maxLength: 20, generator: { type: 'weighted-enum', values: ['good', 'uncertain', 'bad', 'unknown'], weights: [0.92, 0.05, 0.02, 0.01] } },
    { name: 'raw_value', type: 'decimal', precision: 20, scale: 6, nullable: true, generator: { type: 'random-decimal', min: 0, max: 65535, precision: 6 } },
    { name: 'metadata', type: 'json', nullable: true },
  ],
  indexes: [
    { name: 'idx_readings_device_time', columns: ['device_id', 'timestamp'] },
    { name: 'idx_readings_device_metric_time', columns: ['device_id', 'metric_name', 'timestamp'] },
    { name: 'idx_readings_timestamp', columns: ['timestamp'] },
    { name: 'idx_readings_metric', columns: ['metric_name', 'timestamp'] },
    { name: 'idx_readings_value_range', columns: ['metric_name', 'value'] },
  ],
  partitionBy: {
    type: 'range',
    column: 'timestamp',
  },
}

const readingsHourlyTable: TableConfig = {
  name: 'readings_hourly',
  columns: [
    { name: 'id', type: 'bigint', primaryKey: true, generator: { type: 'sequence' } },
    { name: 'device_id', type: 'uuid', indexed: true, references: { table: 'devices', column: 'id' }, generator: { type: 'reference', referenceTable: 'devices', referenceColumn: 'id' } },
    { name: 'metric_name', type: 'string', maxLength: 100, indexed: true, generator: { type: 'enum', values: ['temperature', 'humidity', 'pressure', 'co2', 'pm25', 'voltage', 'power'] } },
    { name: 'hour', type: 'timestamp', indexed: true, generator: { type: 'timestamp-range', start: '2024-01-01', end: '2024-12-31' } },
    { name: 'min_value', type: 'decimal', precision: 20, scale: 6, generator: { type: 'random-decimal', min: -50, max: 500, precision: 6 } },
    { name: 'max_value', type: 'decimal', precision: 20, scale: 6, generator: { type: 'random-decimal', min: -50, max: 500, precision: 6 } },
    { name: 'avg_value', type: 'decimal', precision: 20, scale: 6, indexed: true, generator: { type: 'random-decimal', min: -50, max: 500, precision: 6 } },
    { name: 'sum_value', type: 'decimal', precision: 30, scale: 6, generator: { type: 'random-decimal', min: 0, max: 100000, precision: 6 } },
    { name: 'count', type: 'integer', generator: { type: 'random-int', min: 1, max: 3600 } },
    { name: 'std_dev', type: 'decimal', precision: 20, scale: 6, nullable: true, generator: { type: 'random-decimal', min: 0, max: 50, precision: 6 } },
    { name: 'good_count', type: 'integer', default: 0, generator: { type: 'random-int', min: 0, max: 3600 } },
    { name: 'bad_count', type: 'integer', default: 0, generator: { type: 'random-int', min: 0, max: 100 } },
  ],
  indexes: [
    { name: 'idx_readings_hourly_device_hour', columns: ['device_id', 'hour'] },
    { name: 'idx_readings_hourly_device_metric_hour', columns: ['device_id', 'metric_name', 'hour'] },
    { name: 'idx_readings_hourly_hour', columns: ['hour'] },
    { name: 'idx_readings_hourly_metric', columns: ['metric_name', 'hour'] },
  ],
  partitionBy: {
    type: 'range',
    column: 'hour',
  },
}

const readingsDailyTable: TableConfig = {
  name: 'readings_daily',
  columns: [
    { name: 'id', type: 'bigint', primaryKey: true, generator: { type: 'sequence' } },
    { name: 'device_id', type: 'uuid', indexed: true, references: { table: 'devices', column: 'id' }, generator: { type: 'reference', referenceTable: 'devices', referenceColumn: 'id' } },
    { name: 'metric_name', type: 'string', maxLength: 100, indexed: true, generator: { type: 'enum', values: ['temperature', 'humidity', 'pressure', 'co2', 'pm25', 'voltage', 'power'] } },
    { name: 'date', type: 'date', indexed: true, generator: { type: 'date-range', start: '2024-01-01', end: '2024-12-31' } },
    { name: 'min_value', type: 'decimal', precision: 20, scale: 6, generator: { type: 'random-decimal', min: -50, max: 500, precision: 6 } },
    { name: 'max_value', type: 'decimal', precision: 20, scale: 6, generator: { type: 'random-decimal', min: -50, max: 500, precision: 6 } },
    { name: 'avg_value', type: 'decimal', precision: 20, scale: 6, indexed: true, generator: { type: 'random-decimal', min: -50, max: 500, precision: 6 } },
    { name: 'sum_value', type: 'decimal', precision: 30, scale: 6, generator: { type: 'random-decimal', min: 0, max: 1000000, precision: 6 } },
    { name: 'count', type: 'integer', generator: { type: 'random-int', min: 1, max: 86400 } },
    { name: 'std_dev', type: 'decimal', precision: 20, scale: 6, nullable: true, generator: { type: 'random-decimal', min: 0, max: 50, precision: 6 } },
  ],
  indexes: [
    { name: 'idx_readings_daily_device_date', columns: ['device_id', 'date'] },
    { name: 'idx_readings_daily_device_metric_date', columns: ['device_id', 'metric_name', 'date'] },
    { name: 'idx_readings_daily_date', columns: ['date'] },
    { name: 'idx_readings_daily_metric', columns: ['metric_name', 'date'] },
  ],
}

const alertRulesTable: TableConfig = {
  name: 'alert_rules',
  columns: [
    { name: 'id', type: 'uuid', primaryKey: true, generator: { type: 'uuid' } },
    { name: 'org_id', type: 'uuid', indexed: true, references: { table: 'organizations', column: 'id' }, generator: { type: 'reference', referenceTable: 'organizations', referenceColumn: 'id' } },
    { name: 'name', type: 'string', maxLength: 255, generator: { type: 'faker', fakerMethod: 'lorem.words' } },
    { name: 'description', type: 'text', nullable: true, generator: { type: 'faker', fakerMethod: 'lorem.sentence' } },
    { name: 'device_type_id', type: 'uuid', nullable: true, indexed: true, references: { table: 'device_types', column: 'id' }, generator: { type: 'reference', referenceTable: 'device_types', referenceColumn: 'id' } },
    { name: 'device_id', type: 'uuid', nullable: true, indexed: true, references: { table: 'devices', column: 'id' }, generator: { type: 'reference', referenceTable: 'devices', referenceColumn: 'id' } },
    { name: 'location_id', type: 'uuid', nullable: true, indexed: true, references: { table: 'locations', column: 'id' }, generator: { type: 'reference', referenceTable: 'locations', referenceColumn: 'id' } },
    { name: 'metric_name', type: 'string', maxLength: 100, indexed: true, generator: { type: 'enum', values: ['temperature', 'humidity', 'pressure', 'co2', 'pm25', 'voltage', 'power'] } },
    { name: 'condition', type: 'string', maxLength: 20, generator: { type: 'enum', values: ['gt', 'gte', 'lt', 'lte', 'eq', 'neq', 'between', 'outside'] } },
    { name: 'threshold', type: 'decimal', precision: 20, scale: 6, generator: { type: 'random-decimal', min: 0, max: 100, precision: 6 } },
    { name: 'threshold_high', type: 'decimal', precision: 20, scale: 6, nullable: true, generator: { type: 'random-decimal', min: 50, max: 200, precision: 6 } },
    { name: 'severity', type: 'string', maxLength: 20, indexed: true, generator: { type: 'weighted-enum', values: ['critical', 'warning', 'info'], weights: [0.15, 0.5, 0.35] } },
    { name: 'duration_seconds', type: 'integer', default: 0, generator: { type: 'random-int', min: 0, max: 3600 } },
    { name: 'cooldown_seconds', type: 'integer', default: 300, generator: { type: 'random-int', min: 60, max: 3600 } },
    { name: 'is_active', type: 'boolean', default: true, indexed: true, generator: { type: 'weighted-enum', values: [true, false], weights: [0.9, 0.1] } },
    { name: 'notification_channels', type: 'array', generator: { type: 'faker', fakerMethod: 'helpers.arrayElements' } },
    { name: 'created_at', type: 'timestamp', generator: { type: 'timestamp-range', start: '2022-01-01', end: '2024-12-31' } },
    { name: 'updated_at', type: 'timestamp', generator: { type: 'timestamp-range', start: '2024-01-01', end: '2024-12-31' } },
  ],
  indexes: [
    { name: 'idx_alert_rules_org', columns: ['org_id'] },
    { name: 'idx_alert_rules_device_type', columns: ['device_type_id'] },
    { name: 'idx_alert_rules_device', columns: ['device_id'] },
    { name: 'idx_alert_rules_location', columns: ['location_id'] },
    { name: 'idx_alert_rules_metric', columns: ['metric_name'] },
    { name: 'idx_alert_rules_active', columns: ['org_id', 'is_active'] },
  ],
}

const alertsTable: TableConfig = {
  name: 'alerts',
  columns: [
    { name: 'id', type: 'uuid', primaryKey: true, generator: { type: 'uuid' } },
    { name: 'org_id', type: 'uuid', indexed: true, references: { table: 'organizations', column: 'id' }, generator: { type: 'reference', referenceTable: 'organizations', referenceColumn: 'id' } },
    { name: 'rule_id', type: 'uuid', indexed: true, references: { table: 'alert_rules', column: 'id' }, generator: { type: 'reference', referenceTable: 'alert_rules', referenceColumn: 'id' } },
    { name: 'device_id', type: 'uuid', indexed: true, references: { table: 'devices', column: 'id' }, generator: { type: 'reference', referenceTable: 'devices', referenceColumn: 'id' } },
    { name: 'metric_name', type: 'string', maxLength: 100, indexed: true, generator: { type: 'enum', values: ['temperature', 'humidity', 'pressure', 'co2', 'pm25', 'voltage', 'power'] } },
    { name: 'severity', type: 'string', maxLength: 20, indexed: true, generator: { type: 'weighted-enum', values: ['critical', 'warning', 'info'], weights: [0.1, 0.4, 0.5] } },
    { name: 'status', type: 'string', maxLength: 20, indexed: true, generator: { type: 'weighted-enum', values: ['active', 'acknowledged', 'resolved', 'escalated'], weights: [0.2, 0.2, 0.55, 0.05] } },
    { name: 'value', type: 'decimal', precision: 20, scale: 6, generator: { type: 'random-decimal', min: -100, max: 500, precision: 6 } },
    { name: 'threshold', type: 'decimal', precision: 20, scale: 6, generator: { type: 'random-decimal', min: 0, max: 100, precision: 6 } },
    { name: 'message', type: 'text', generator: { type: 'faker', fakerMethod: 'lorem.sentence' } },
    { name: 'acknowledged_by', type: 'uuid', nullable: true, generator: { type: 'uuid' } },
    { name: 'acknowledged_at', type: 'timestamp', nullable: true, generator: { type: 'timestamp-range', start: '2024-01-01', end: '2024-12-31' } },
    { name: 'resolved_at', type: 'timestamp', nullable: true, indexed: true, generator: { type: 'timestamp-range', start: '2024-01-01', end: '2024-12-31' } },
    { name: 'triggered_at', type: 'timestamp', indexed: true, generator: { type: 'timestamp-range', start: '2024-01-01', end: '2024-12-31' } },
    { name: 'created_at', type: 'timestamp', indexed: true, generator: { type: 'timestamp-range', start: '2024-01-01', end: '2024-12-31' } },
  ],
  indexes: [
    { name: 'idx_alerts_org', columns: ['org_id'] },
    { name: 'idx_alerts_rule', columns: ['rule_id'] },
    { name: 'idx_alerts_device', columns: ['device_id'] },
    { name: 'idx_alerts_status', columns: ['org_id', 'status'] },
    { name: 'idx_alerts_severity', columns: ['org_id', 'severity'] },
    { name: 'idx_alerts_triggered', columns: ['triggered_at'] },
    { name: 'idx_alerts_active', columns: ['org_id', 'status', 'triggered_at'], where: "status = 'active'" },
    { name: 'idx_alerts_device_time', columns: ['device_id', 'triggered_at'] },
  ],
  partitionBy: {
    type: 'range',
    column: 'triggered_at',
  },
}

const commandsTable: TableConfig = {
  name: 'commands',
  columns: [
    { name: 'id', type: 'uuid', primaryKey: true, generator: { type: 'uuid' } },
    { name: 'org_id', type: 'uuid', indexed: true, references: { table: 'organizations', column: 'id' }, generator: { type: 'reference', referenceTable: 'organizations', referenceColumn: 'id' } },
    { name: 'device_id', type: 'uuid', indexed: true, references: { table: 'devices', column: 'id' }, generator: { type: 'reference', referenceTable: 'devices', referenceColumn: 'id' } },
    { name: 'command_type', type: 'string', maxLength: 50, indexed: true, generator: { type: 'enum', values: ['reboot', 'configure', 'update_firmware', 'calibrate', 'set_interval', 'enable', 'disable'] } },
    { name: 'payload', type: 'json', nullable: true },
    { name: 'status', type: 'string', maxLength: 20, indexed: true, generator: { type: 'weighted-enum', values: ['pending', 'sent', 'acknowledged', 'completed', 'failed', 'timeout'], weights: [0.1, 0.15, 0.1, 0.5, 0.1, 0.05] } },
    { name: 'priority', type: 'integer', default: 0, indexed: true, generator: { type: 'random-int', min: 0, max: 10 } },
    { name: 'expires_at', type: 'timestamp', nullable: true, indexed: true, generator: { type: 'timestamp-range', start: '2024-01-01', end: '2025-01-01' } },
    { name: 'sent_at', type: 'timestamp', nullable: true, generator: { type: 'timestamp-range', start: '2024-01-01', end: '2024-12-31' } },
    { name: 'completed_at', type: 'timestamp', nullable: true, generator: { type: 'timestamp-range', start: '2024-01-01', end: '2024-12-31' } },
    { name: 'response', type: 'json', nullable: true },
    { name: 'error_message', type: 'text', nullable: true, generator: { type: 'faker', fakerMethod: 'lorem.sentence' } },
    { name: 'created_by', type: 'uuid', nullable: true, generator: { type: 'uuid' } },
    { name: 'created_at', type: 'timestamp', indexed: true, generator: { type: 'timestamp-range', start: '2024-01-01', end: '2024-12-31' } },
  ],
  indexes: [
    { name: 'idx_commands_org', columns: ['org_id'] },
    { name: 'idx_commands_device', columns: ['device_id'] },
    { name: 'idx_commands_device_status', columns: ['device_id', 'status'] },
    { name: 'idx_commands_status', columns: ['status'] },
    { name: 'idx_commands_pending', columns: ['device_id', 'priority', 'created_at'], where: "status = 'pending'" },
    { name: 'idx_commands_created', columns: ['created_at'] },
  ],
}

const deviceEventsTable: TableConfig = {
  name: 'device_events',
  columns: [
    { name: 'id', type: 'bigint', primaryKey: true, generator: { type: 'sequence' } },
    { name: 'device_id', type: 'uuid', indexed: true, references: { table: 'devices', column: 'id' }, generator: { type: 'reference', referenceTable: 'devices', referenceColumn: 'id' } },
    { name: 'event_type', type: 'string', maxLength: 50, indexed: true, generator: { type: 'weighted-enum', values: ['connected', 'disconnected', 'error', 'warning', 'config_changed', 'firmware_updated', 'calibrated', 'battery_low'], weights: [0.25, 0.25, 0.1, 0.15, 0.1, 0.05, 0.05, 0.05] } },
    { name: 'message', type: 'text', nullable: true, generator: { type: 'faker', fakerMethod: 'lorem.sentence' } },
    { name: 'details', type: 'json', nullable: true },
    { name: 'severity', type: 'string', maxLength: 20, indexed: true, generator: { type: 'weighted-enum', values: ['error', 'warning', 'info', 'debug'], weights: [0.1, 0.2, 0.5, 0.2] } },
    { name: 'timestamp', type: 'timestamp', indexed: true, generator: { type: 'timestamp-range', start: '2024-01-01', end: '2024-12-31' } },
  ],
  indexes: [
    { name: 'idx_device_events_device', columns: ['device_id'] },
    { name: 'idx_device_events_device_time', columns: ['device_id', 'timestamp'] },
    { name: 'idx_device_events_type', columns: ['event_type', 'timestamp'] },
    { name: 'idx_device_events_severity', columns: ['severity', 'timestamp'] },
    { name: 'idx_device_events_timestamp', columns: ['timestamp'] },
  ],
  partitionBy: {
    type: 'range',
    column: 'timestamp',
  },
}

const dashboardsTable: TableConfig = {
  name: 'dashboards',
  columns: [
    { name: 'id', type: 'uuid', primaryKey: true, generator: { type: 'uuid' } },
    { name: 'org_id', type: 'uuid', indexed: true, references: { table: 'organizations', column: 'id' }, generator: { type: 'reference', referenceTable: 'organizations', referenceColumn: 'id' } },
    { name: 'name', type: 'string', maxLength: 255, indexed: true, generator: { type: 'faker', fakerMethod: 'lorem.words' } },
    { name: 'description', type: 'text', nullable: true, generator: { type: 'faker', fakerMethod: 'lorem.sentence' } },
    { name: 'is_default', type: 'boolean', default: false, generator: { type: 'weighted-enum', values: [true, false], weights: [0.1, 0.9] } },
    { name: 'is_public', type: 'boolean', default: false, generator: { type: 'weighted-enum', values: [true, false], weights: [0.2, 0.8] } },
    { name: 'layout', type: 'json', generator: { type: 'faker', fakerMethod: 'datatype.json' } },
    { name: 'refresh_interval_seconds', type: 'integer', default: 60, generator: { type: 'weighted-enum', values: [10, 30, 60, 300, 900], weights: [0.1, 0.2, 0.4, 0.2, 0.1] } },
    { name: 'time_range', type: 'string', maxLength: 50, default: '1h', generator: { type: 'enum', values: ['15m', '1h', '6h', '24h', '7d', '30d'] } },
    { name: 'created_by', type: 'uuid', generator: { type: 'uuid' } },
    { name: 'created_at', type: 'timestamp', generator: { type: 'timestamp-range', start: '2022-01-01', end: '2024-12-31' } },
    { name: 'updated_at', type: 'timestamp', generator: { type: 'timestamp-range', start: '2024-01-01', end: '2024-12-31' } },
  ],
  indexes: [
    { name: 'idx_dashboards_org', columns: ['org_id'] },
    { name: 'idx_dashboards_default', columns: ['org_id', 'is_default'] },
    { name: 'idx_dashboards_public', columns: ['is_public'] },
  ],
}

const widgetsTable: TableConfig = {
  name: 'widgets',
  columns: [
    { name: 'id', type: 'uuid', primaryKey: true, generator: { type: 'uuid' } },
    { name: 'dashboard_id', type: 'uuid', indexed: true, references: { table: 'dashboards', column: 'id' }, generator: { type: 'reference', referenceTable: 'dashboards', referenceColumn: 'id' } },
    { name: 'name', type: 'string', maxLength: 255, generator: { type: 'faker', fakerMethod: 'lorem.words' } },
    { name: 'type', type: 'string', maxLength: 50, indexed: true, generator: { type: 'enum', values: ['line_chart', 'gauge', 'map', 'table', 'stat', 'heatmap', 'bar_chart', 'pie_chart'] } },
    { name: 'device_id', type: 'uuid', nullable: true, indexed: true, references: { table: 'devices', column: 'id' }, generator: { type: 'reference', referenceTable: 'devices', referenceColumn: 'id' } },
    { name: 'device_type_id', type: 'uuid', nullable: true, indexed: true, references: { table: 'device_types', column: 'id' }, generator: { type: 'reference', referenceTable: 'device_types', referenceColumn: 'id' } },
    { name: 'location_id', type: 'uuid', nullable: true, indexed: true, references: { table: 'locations', column: 'id' }, generator: { type: 'reference', referenceTable: 'locations', referenceColumn: 'id' } },
    { name: 'metric_names', type: 'array', generator: { type: 'faker', fakerMethod: 'helpers.arrayElements' } },
    { name: 'aggregation', type: 'string', maxLength: 20, generator: { type: 'enum', values: ['avg', 'min', 'max', 'sum', 'count', 'last'] } },
    { name: 'position_x', type: 'integer', generator: { type: 'random-int', min: 0, max: 12 } },
    { name: 'position_y', type: 'integer', generator: { type: 'random-int', min: 0, max: 20 } },
    { name: 'width', type: 'integer', generator: { type: 'random-int', min: 1, max: 12 } },
    { name: 'height', type: 'integer', generator: { type: 'random-int', min: 1, max: 8 } },
    { name: 'config', type: 'json', nullable: true },
    { name: 'created_at', type: 'timestamp', generator: { type: 'timestamp-range', start: '2022-01-01', end: '2024-12-31' } },
  ],
  indexes: [
    { name: 'idx_widgets_dashboard', columns: ['dashboard_id'] },
    { name: 'idx_widgets_device', columns: ['device_id'] },
    { name: 'idx_widgets_type', columns: ['type'] },
  ],
  embedded: ['name', 'type', 'position_x', 'position_y', 'width', 'height', 'config'],
}

// ============================================================================
// Tables Array
// ============================================================================

const tables: TableConfig[] = [
  organizationsTable,
  locationsTable,
  deviceTypesTable,
  devicesTable,
  readingsTable,
  readingsHourlyTable,
  readingsDailyTable,
  alertRulesTable,
  alertsTable,
  commandsTable,
  deviceEventsTable,
  dashboardsTable,
  widgetsTable,
]

// ============================================================================
// Relationships
// ============================================================================

const relationships: RelationshipConfig[] = [
  {
    name: 'org_locations',
    type: 'one-to-many',
    from: { table: 'organizations', column: 'id' },
    to: { table: 'locations', column: 'org_id' },
    onDelete: 'cascade',
  },
  {
    name: 'location_hierarchy',
    type: 'one-to-many',
    from: { table: 'locations', column: 'id' },
    to: { table: 'locations', column: 'parent_id' },
    onDelete: 'set-null',
  },
  {
    name: 'org_devices',
    type: 'one-to-many',
    from: { table: 'organizations', column: 'id' },
    to: { table: 'devices', column: 'org_id' },
    onDelete: 'cascade',
  },
  {
    name: 'device_type_devices',
    type: 'one-to-many',
    from: { table: 'device_types', column: 'id' },
    to: { table: 'devices', column: 'device_type_id' },
    onDelete: 'restrict',
  },
  {
    name: 'location_devices',
    type: 'one-to-many',
    from: { table: 'locations', column: 'id' },
    to: { table: 'devices', column: 'location_id' },
    onDelete: 'set-null',
  },
  {
    name: 'device_readings',
    type: 'one-to-many',
    from: { table: 'devices', column: 'id' },
    to: { table: 'readings', column: 'device_id' },
    onDelete: 'cascade',
  },
  {
    name: 'device_readings_hourly',
    type: 'one-to-many',
    from: { table: 'devices', column: 'id' },
    to: { table: 'readings_hourly', column: 'device_id' },
    onDelete: 'cascade',
  },
  {
    name: 'device_readings_daily',
    type: 'one-to-many',
    from: { table: 'devices', column: 'id' },
    to: { table: 'readings_daily', column: 'device_id' },
    onDelete: 'cascade',
  },
  {
    name: 'org_alert_rules',
    type: 'one-to-many',
    from: { table: 'organizations', column: 'id' },
    to: { table: 'alert_rules', column: 'org_id' },
    onDelete: 'cascade',
  },
  {
    name: 'rule_alerts',
    type: 'one-to-many',
    from: { table: 'alert_rules', column: 'id' },
    to: { table: 'alerts', column: 'rule_id' },
    onDelete: 'cascade',
  },
  {
    name: 'device_alerts',
    type: 'one-to-many',
    from: { table: 'devices', column: 'id' },
    to: { table: 'alerts', column: 'device_id' },
    onDelete: 'cascade',
  },
  {
    name: 'device_commands',
    type: 'one-to-many',
    from: { table: 'devices', column: 'id' },
    to: { table: 'commands', column: 'device_id' },
    onDelete: 'cascade',
  },
  {
    name: 'device_events',
    type: 'one-to-many',
    from: { table: 'devices', column: 'id' },
    to: { table: 'device_events', column: 'device_id' },
    onDelete: 'cascade',
  },
  {
    name: 'org_dashboards',
    type: 'one-to-many',
    from: { table: 'organizations', column: 'id' },
    to: { table: 'dashboards', column: 'org_id' },
    onDelete: 'cascade',
  },
  {
    name: 'dashboard_widgets',
    type: 'one-to-many',
    from: { table: 'dashboards', column: 'id' },
    to: { table: 'widgets', column: 'dashboard_id' },
    onDelete: 'cascade',
    embed: true,
  },
]

// ============================================================================
// Size Tiers
// ============================================================================

const sizeTiers: SizeTierConfig[] = [
  {
    size: '1mb',
    seedCount: {
      organizations: 5,
      locations: 25,
      device_types: 10,
      devices: 50,
      readings: 50000,
      readings_hourly: 3000,
      readings_daily: 500,
      alert_rules: 30,
      alerts: 200,
      commands: 100,
      device_events: 2000,
      dashboards: 10,
      widgets: 50,
    },
    estimatedBytes: 1_048_576,
    recommendedMemoryMB: 128,
  },
  {
    size: '10mb',
    seedCount: {
      organizations: 20,
      locations: 150,
      device_types: 25,
      devices: 500,
      readings: 500000,
      readings_hourly: 30000,
      readings_daily: 5000,
      alert_rules: 200,
      alerts: 2000,
      commands: 1000,
      device_events: 20000,
      dashboards: 50,
      widgets: 300,
    },
    estimatedBytes: 10_485_760,
    recommendedMemoryMB: 256,
  },
  {
    size: '100mb',
    seedCount: {
      organizations: 100,
      locations: 1000,
      device_types: 50,
      devices: 5000,
      readings: 5000000,
      readings_hourly: 300000,
      readings_daily: 50000,
      alert_rules: 1500,
      alerts: 20000,
      commands: 10000,
      device_events: 200000,
      dashboards: 300,
      widgets: 2000,
    },
    estimatedBytes: 104_857_600,
    recommendedMemoryMB: 512,
  },
  {
    size: '1gb',
    seedCount: {
      organizations: 500,
      locations: 8000,
      device_types: 100,
      devices: 50000,
      readings: 50000000,
      readings_hourly: 3000000,
      readings_daily: 500000,
      alert_rules: 10000,
      alerts: 200000,
      commands: 100000,
      device_events: 2000000,
      dashboards: 2000,
      widgets: 15000,
    },
    estimatedBytes: 1_073_741_824,
    recommendedMemoryMB: 2048,
    recommendedCores: 2,
  },
  {
    size: '10gb',
    seedCount: {
      organizations: 2000,
      locations: 50000,
      device_types: 200,
      devices: 500000,
      readings: 500000000,
      readings_hourly: 30000000,
      readings_daily: 5000000,
      alert_rules: 50000,
      alerts: 2000000,
      commands: 1000000,
      device_events: 20000000,
      dashboards: 10000,
      widgets: 80000,
    },
    estimatedBytes: 10_737_418_240,
    recommendedMemoryMB: 8192,
    recommendedCores: 4,
  },
  {
    size: '20gb',
    seedCount: {
      organizations: 4000,
      locations: 100000,
      device_types: 300,
      devices: 1000000,
      readings: 1000000000,
      readings_hourly: 60000000,
      readings_daily: 10000000,
      alert_rules: 100000,
      alerts: 4000000,
      commands: 2000000,
      device_events: 40000000,
      dashboards: 20000,
      widgets: 160000,
    },
    estimatedBytes: 21_474_836_480,
    recommendedMemoryMB: 16384,
    recommendedCores: 8,
  },
  {
    size: '30gb',
    seedCount: {
      organizations: 6000,
      locations: 150000,
      device_types: 400,
      devices: 1500000,
      readings: 1500000000,
      readings_hourly: 90000000,
      readings_daily: 15000000,
      alert_rules: 150000,
      alerts: 6000000,
      commands: 3000000,
      device_events: 60000000,
      dashboards: 30000,
      widgets: 240000,
    },
    estimatedBytes: 32_212_254_720,
    recommendedMemoryMB: 24576,
    recommendedCores: 12,
  },
  {
    size: '50gb',
    seedCount: {
      organizations: 10000,
      locations: 250000,
      device_types: 500,
      devices: 2500000,
      readings: 2500000000,
      readings_hourly: 150000000,
      readings_daily: 25000000,
      alert_rules: 250000,
      alerts: 10000000,
      commands: 5000000,
      device_events: 100000000,
      dashboards: 50000,
      widgets: 400000,
    },
    estimatedBytes: 53_687_091_200,
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
    name: 'get_device_by_id',
    description: 'Fetch device by ID',
    category: 'point-lookup',
    sql: 'SELECT * FROM devices WHERE id = $1',
    documentQuery: {
      collection: 'devices',
      operation: 'find',
      filter: { _id: '$1' },
    },
    parameters: [{ type: 'reference', referenceTable: 'devices', referenceColumn: 'id' }],
    expectedComplexity: 'O(1)',
    weight: 10,
  },
  {
    name: 'get_device_by_serial',
    description: 'Fetch device by serial number',
    category: 'point-lookup',
    sql: 'SELECT * FROM devices WHERE serial_number = $1',
    documentQuery: {
      collection: 'devices',
      operation: 'find',
      filter: { serial_number: '$1' },
    },
    parameters: [{ type: 'reference', referenceTable: 'devices', referenceColumn: 'serial_number' }],
    expectedComplexity: 'O(1)',
    weight: 8,
  },
  {
    name: 'get_latest_reading',
    description: 'Get most recent reading for a device and metric',
    category: 'point-lookup',
    sql: `SELECT * FROM readings
          WHERE device_id = $1 AND metric_name = $2
          ORDER BY timestamp DESC
          LIMIT 1`,
    documentQuery: {
      collection: 'readings',
      operation: 'find',
      filter: { device_id: '$1', metric_name: '$2' },
    },
    parameters: [
      { type: 'reference', referenceTable: 'devices', referenceColumn: 'id' },
      { type: 'enum', values: ['temperature', 'humidity', 'pressure', 'co2', 'power'] },
    ],
    expectedComplexity: 'O(log n)',
    weight: 15,
  },

  // Time-range queries (the bread and butter of timeseries)
  {
    name: 'readings_time_range',
    description: 'Get readings for device in time range',
    category: 'range-scan',
    sql: `SELECT * FROM readings
          WHERE device_id = $1 AND metric_name = $2
          AND timestamp >= $3 AND timestamp < $4
          ORDER BY timestamp`,
    documentQuery: {
      collection: 'readings',
      operation: 'find',
      filter: { device_id: '$1', metric_name: '$2', timestamp: { $gte: '$3', $lt: '$4' } },
    },
    parameters: [
      { type: 'reference', referenceTable: 'devices', referenceColumn: 'id' },
      { type: 'enum', values: ['temperature', 'humidity', 'pressure'] },
      { type: 'timestamp-range', start: '2024-01-01', end: '2024-06-01' },
      { type: 'timestamp-range', start: '2024-06-01', end: '2024-12-31' },
    ],
    expectedComplexity: 'O(log n)',
    weight: 15,
  },
  {
    name: 'readings_multiple_devices',
    description: 'Get readings for multiple devices (batch query)',
    category: 'range-scan',
    sql: `SELECT * FROM readings
          WHERE device_id = ANY($1) AND metric_name = $2
          AND timestamp >= $3 AND timestamp < $4
          ORDER BY device_id, timestamp`,
    documentQuery: {
      collection: 'readings',
      operation: 'find',
      filter: { device_id: { $in: '$1' }, metric_name: '$2', timestamp: { $gte: '$3', $lt: '$4' } },
    },
    parameters: [
      { type: 'reference', referenceTable: 'devices', referenceColumn: 'id' },
      { type: 'enum', values: ['temperature', 'humidity'] },
      { type: 'timestamp-range', start: '2024-06-01', end: '2024-09-01' },
      { type: 'timestamp-range', start: '2024-09-01', end: '2024-12-31' },
    ],
    expectedComplexity: 'O(n log n)',
    weight: 8,
  },
  {
    name: 'hourly_aggregates_range',
    description: 'Get hourly aggregates for device in date range',
    category: 'range-scan',
    sql: `SELECT * FROM readings_hourly
          WHERE device_id = $1 AND metric_name = $2
          AND hour >= $3 AND hour < $4
          ORDER BY hour`,
    documentQuery: {
      collection: 'readings_hourly',
      operation: 'find',
      filter: { device_id: '$1', metric_name: '$2', hour: { $gte: '$3', $lt: '$4' } },
    },
    parameters: [
      { type: 'reference', referenceTable: 'devices', referenceColumn: 'id' },
      { type: 'enum', values: ['temperature', 'humidity', 'power'] },
      { type: 'timestamp-range', start: '2024-01-01', end: '2024-03-01' },
      { type: 'timestamp-range', start: '2024-03-01', end: '2024-06-01' },
    ],
    expectedComplexity: 'O(log n)',
    weight: 10,
  },
  {
    name: 'daily_aggregates_range',
    description: 'Get daily aggregates for device in date range',
    category: 'range-scan',
    sql: `SELECT * FROM readings_daily
          WHERE device_id = $1 AND metric_name = $2
          AND date >= $3 AND date < $4
          ORDER BY date`,
    documentQuery: {
      collection: 'readings_daily',
      operation: 'find',
      filter: { device_id: '$1', metric_name: '$2', date: { $gte: '$3', $lt: '$4' } },
    },
    parameters: [
      { type: 'reference', referenceTable: 'devices', referenceColumn: 'id' },
      { type: 'enum', values: ['temperature', 'humidity', 'power'] },
      { type: 'date-range', start: '2024-01-01', end: '2024-06-01' },
      { type: 'date-range', start: '2024-06-01', end: '2024-12-31' },
    ],
    expectedComplexity: 'O(log n)',
    weight: 8,
  },

  // Fleet management queries
  {
    name: 'list_org_devices',
    description: 'List devices for an organization with status',
    category: 'range-scan',
    sql: `SELECT d.*, dt.name as device_type_name, l.name as location_name
          FROM devices d
          JOIN device_types dt ON d.device_type_id = dt.id
          LEFT JOIN locations l ON d.location_id = l.id
          WHERE d.org_id = $1 AND d.is_active = true
          ORDER BY d.last_seen_at DESC
          LIMIT 100`,
    documentQuery: {
      collection: 'devices',
      operation: 'find',
      filter: { org_id: '$1', is_active: true },
    },
    parameters: [{ type: 'reference', referenceTable: 'organizations', referenceColumn: 'id' }],
    expectedComplexity: 'O(log n)',
    weight: 8,
  },
  {
    name: 'list_devices_by_status',
    description: 'List devices by status (find offline devices)',
    category: 'range-scan',
    sql: `SELECT * FROM devices
          WHERE org_id = $1 AND status = $2 AND is_active = true
          ORDER BY last_seen_at
          LIMIT 50`,
    documentQuery: {
      collection: 'devices',
      operation: 'find',
      filter: { org_id: '$1', status: '$2', is_active: true },
    },
    parameters: [
      { type: 'reference', referenceTable: 'organizations', referenceColumn: 'id' },
      { type: 'enum', values: ['offline', 'error', 'maintenance'] },
    ],
    expectedComplexity: 'O(log n)',
    weight: 5,
  },
  {
    name: 'list_devices_by_location',
    description: 'List devices at a location',
    category: 'range-scan',
    sql: `SELECT d.*, dt.name as device_type_name
          FROM devices d
          JOIN device_types dt ON d.device_type_id = dt.id
          WHERE d.location_id = $1 AND d.is_active = true
          ORDER BY d.name`,
    documentQuery: {
      collection: 'devices',
      operation: 'find',
      filter: { location_id: '$1', is_active: true },
    },
    parameters: [{ type: 'reference', referenceTable: 'locations', referenceColumn: 'id' }],
    expectedComplexity: 'O(log n)',
    weight: 5,
  },
  {
    name: 'stale_devices',
    description: 'Find devices that have not reported recently',
    category: 'range-scan',
    sql: `SELECT * FROM devices
          WHERE org_id = $1 AND is_active = true
          AND last_seen_at < $2
          ORDER BY last_seen_at
          LIMIT 100`,
    documentQuery: {
      collection: 'devices',
      operation: 'find',
      filter: { org_id: '$1', is_active: true, last_seen_at: { $lt: '$2' } },
    },
    parameters: [
      { type: 'reference', referenceTable: 'organizations', referenceColumn: 'id' },
      { type: 'timestamp-range', start: '2024-01-01', end: '2024-10-01' },
    ],
    expectedComplexity: 'O(log n)',
    weight: 3,
  },

  // Aggregation queries
  {
    name: 'aggregate_readings_hourly',
    description: 'Compute hourly aggregates for a device (for rollup)',
    category: 'aggregate',
    sql: `SELECT
            device_id,
            metric_name,
            date_trunc('hour', timestamp) as hour,
            MIN(value) as min_value,
            MAX(value) as max_value,
            AVG(value) as avg_value,
            SUM(value) as sum_value,
            COUNT(*) as count,
            STDDEV(value) as std_dev
          FROM readings
          WHERE device_id = $1 AND metric_name = $2
          AND timestamp >= $3 AND timestamp < $4
          GROUP BY device_id, metric_name, date_trunc('hour', timestamp)
          ORDER BY hour`,
    documentQuery: {
      collection: 'readings',
      operation: 'aggregate',
      pipeline: [
        { $match: { device_id: '$1', metric_name: '$2', timestamp: { $gte: '$3', $lt: '$4' } } },
        { $group: { _id: { $dateTrunc: { date: '$timestamp', unit: 'hour' } }, min_value: { $min: '$value' }, max_value: { $max: '$value' }, avg_value: { $avg: '$value' }, count: { $sum: 1 } } },
        { $sort: { _id: 1 } },
      ],
    },
    parameters: [
      { type: 'reference', referenceTable: 'devices', referenceColumn: 'id' },
      { type: 'enum', values: ['temperature', 'humidity', 'power'] },
      { type: 'timestamp-range', start: '2024-06-01', end: '2024-07-01' },
      { type: 'timestamp-range', start: '2024-07-01', end: '2024-08-01' },
    ],
    expectedComplexity: 'O(n)',
    weight: 4,
  },
  {
    name: 'aggregate_by_location',
    description: 'Get average readings by location for a time period',
    category: 'aggregate',
    sql: `SELECT
            l.id as location_id,
            l.name as location_name,
            r.metric_name,
            AVG(r.value) as avg_value,
            MIN(r.value) as min_value,
            MAX(r.value) as max_value,
            COUNT(*) as reading_count
          FROM readings r
          JOIN devices d ON r.device_id = d.id
          JOIN locations l ON d.location_id = l.id
          WHERE l.org_id = $1 AND r.metric_name = $2
          AND r.timestamp >= $3 AND r.timestamp < $4
          GROUP BY l.id, l.name, r.metric_name
          ORDER BY avg_value DESC`,
    documentQuery: {
      collection: 'readings',
      operation: 'aggregate',
      pipeline: [
        { $match: { metric_name: '$2', timestamp: { $gte: '$3', $lt: '$4' } } },
        { $lookup: { from: 'devices', localField: 'device_id', foreignField: '_id', as: 'device' } },
        { $group: { _id: '$device.location_id', avg_value: { $avg: '$value' }, count: { $sum: 1 } } },
      ],
    },
    parameters: [
      { type: 'reference', referenceTable: 'organizations', referenceColumn: 'id' },
      { type: 'enum', values: ['temperature', 'humidity'] },
      { type: 'timestamp-range', start: '2024-06-01', end: '2024-07-01' },
      { type: 'timestamp-range', start: '2024-07-01', end: '2024-08-01' },
    ],
    expectedComplexity: 'O(n)',
    weight: 3,
  },
  {
    name: 'device_statistics',
    description: 'Get device reading statistics for a period',
    category: 'aggregate',
    sql: `SELECT
            d.id,
            d.name,
            COUNT(r.id) as reading_count,
            MIN(r.timestamp) as first_reading,
            MAX(r.timestamp) as last_reading,
            COUNT(DISTINCT DATE(r.timestamp)) as active_days
          FROM devices d
          LEFT JOIN readings r ON d.id = r.device_id
          AND r.timestamp >= $2 AND r.timestamp < $3
          WHERE d.org_id = $1
          GROUP BY d.id, d.name
          ORDER BY reading_count DESC
          LIMIT 50`,
    documentQuery: {
      collection: 'devices',
      operation: 'aggregate',
      pipeline: [
        { $match: { org_id: '$1' } },
        { $lookup: { from: 'readings', localField: '_id', foreignField: 'device_id', as: 'readings' } },
        { $addFields: { reading_count: { $size: '$readings' } } },
        { $sort: { reading_count: -1 } },
        { $limit: 50 },
      ],
    },
    parameters: [
      { type: 'reference', referenceTable: 'organizations', referenceColumn: 'id' },
      { type: 'timestamp-range', start: '2024-01-01', end: '2024-06-01' },
      { type: 'timestamp-range', start: '2024-06-01', end: '2024-12-31' },
    ],
    expectedComplexity: 'O(n)',
    weight: 2,
  },

  // Alert queries
  {
    name: 'active_alerts',
    description: 'Get active alerts for an organization',
    category: 'range-scan',
    sql: `SELECT a.*, d.name as device_name, d.serial_number, ar.name as rule_name
          FROM alerts a
          JOIN devices d ON a.device_id = d.id
          JOIN alert_rules ar ON a.rule_id = ar.id
          WHERE a.org_id = $1 AND a.status = 'active'
          ORDER BY a.severity DESC, a.triggered_at DESC
          LIMIT 100`,
    documentQuery: {
      collection: 'alerts',
      operation: 'find',
      filter: { org_id: '$1', status: 'active' },
    },
    parameters: [{ type: 'reference', referenceTable: 'organizations', referenceColumn: 'id' }],
    expectedComplexity: 'O(log n)',
    weight: 6,
  },
  {
    name: 'alert_history',
    description: 'Get alert history for a device',
    category: 'range-scan',
    sql: `SELECT * FROM alerts
          WHERE device_id = $1
          AND triggered_at >= $2 AND triggered_at < $3
          ORDER BY triggered_at DESC`,
    documentQuery: {
      collection: 'alerts',
      operation: 'find',
      filter: { device_id: '$1', triggered_at: { $gte: '$2', $lt: '$3' } },
    },
    parameters: [
      { type: 'reference', referenceTable: 'devices', referenceColumn: 'id' },
      { type: 'timestamp-range', start: '2024-01-01', end: '2024-06-01' },
      { type: 'timestamp-range', start: '2024-06-01', end: '2024-12-31' },
    ],
    expectedComplexity: 'O(log n)',
    weight: 4,
  },
  {
    name: 'alert_count_by_severity',
    description: 'Count alerts by severity for an organization',
    category: 'aggregate',
    sql: `SELECT severity, status, COUNT(*) as count
          FROM alerts
          WHERE org_id = $1 AND triggered_at >= $2
          GROUP BY severity, status`,
    documentQuery: {
      collection: 'alerts',
      operation: 'aggregate',
      pipeline: [
        { $match: { org_id: '$1', triggered_at: { $gte: '$2' } } },
        { $group: { _id: { severity: '$severity', status: '$status' }, count: { $sum: 1 } } },
      ],
    },
    parameters: [
      { type: 'reference', referenceTable: 'organizations', referenceColumn: 'id' },
      { type: 'timestamp-range', start: '2024-01-01', end: '2024-10-01' },
    ],
    expectedComplexity: 'O(n)',
    weight: 3,
  },

  // Device events
  {
    name: 'device_event_history',
    description: 'Get event history for a device',
    category: 'range-scan',
    sql: `SELECT * FROM device_events
          WHERE device_id = $1
          AND timestamp >= $2 AND timestamp < $3
          ORDER BY timestamp DESC
          LIMIT 100`,
    documentQuery: {
      collection: 'device_events',
      operation: 'find',
      filter: { device_id: '$1', timestamp: { $gte: '$2', $lt: '$3' } },
    },
    parameters: [
      { type: 'reference', referenceTable: 'devices', referenceColumn: 'id' },
      { type: 'timestamp-range', start: '2024-06-01', end: '2024-09-01' },
      { type: 'timestamp-range', start: '2024-09-01', end: '2024-12-31' },
    ],
    expectedComplexity: 'O(log n)',
    weight: 4,
  },

  // Write operations (high-volume ingestion)
  {
    name: 'insert_reading',
    description: 'Insert a single reading (high frequency)',
    category: 'write',
    sql: `INSERT INTO readings (device_id, timestamp, metric_name, value, unit, quality)
          VALUES ($1, $2, $3, $4, $5, 'good')`,
    documentQuery: {
      collection: 'readings',
      operation: 'insert',
    },
    parameters: [
      { type: 'reference', referenceTable: 'devices', referenceColumn: 'id' },
      { type: 'timestamp-range', start: '2024-01-01', end: '2024-12-31' },
      { type: 'enum', values: ['temperature', 'humidity', 'pressure', 'power'] },
      { type: 'random-decimal', min: -50, max: 100, precision: 2 },
      { type: 'enum', values: ['celsius', 'percent', 'hPa', 'watts'] },
    ],
    expectedComplexity: 'O(log n)',
    weight: 20,
  },
  {
    name: 'batch_insert_readings',
    description: 'Insert batch of readings (bulk ingestion)',
    category: 'write',
    sql: `INSERT INTO readings (device_id, timestamp, metric_name, value, unit, quality)
          VALUES ($1, $2, $3, $4, $5, 'good'),
                 ($1, $6, $7, $8, $9, 'good'),
                 ($1, $10, $11, $12, $13, 'good')`,
    documentQuery: {
      collection: 'readings',
      operation: 'insert',
    },
    parameters: [
      { type: 'reference', referenceTable: 'devices', referenceColumn: 'id' },
      { type: 'timestamp-range', start: '2024-01-01', end: '2024-12-31' },
      { type: 'enum', values: ['temperature'] },
      { type: 'random-decimal', min: 15, max: 30, precision: 2 },
      { type: 'enum', values: ['celsius'] },
      { type: 'timestamp-range', start: '2024-01-01', end: '2024-12-31' },
      { type: 'enum', values: ['humidity'] },
      { type: 'random-decimal', min: 30, max: 80, precision: 2 },
      { type: 'enum', values: ['percent'] },
      { type: 'timestamp-range', start: '2024-01-01', end: '2024-12-31' },
      { type: 'enum', values: ['pressure'] },
      { type: 'random-decimal', min: 980, max: 1030, precision: 2 },
      { type: 'enum', values: ['hPa'] },
    ],
    expectedComplexity: 'O(log n)',
    weight: 10,
  },
  {
    name: 'update_device_status',
    description: 'Update device status and last seen timestamp',
    category: 'write',
    sql: `UPDATE devices
          SET status = $2, last_seen_at = $3, updated_at = $3
          WHERE id = $1`,
    documentQuery: {
      collection: 'devices',
      operation: 'update',
      filter: { _id: '$1' },
    },
    parameters: [
      { type: 'reference', referenceTable: 'devices', referenceColumn: 'id' },
      { type: 'enum', values: ['online', 'offline'] },
      { type: 'timestamp-range', start: '2024-01-01', end: '2024-12-31' },
    ],
    expectedComplexity: 'O(log n)',
    weight: 5,
  },
  {
    name: 'create_alert',
    description: 'Create a new alert',
    category: 'write',
    sql: `INSERT INTO alerts (id, org_id, rule_id, device_id, metric_name, severity, status, value, threshold, message, triggered_at, created_at)
          VALUES ($1, $2, $3, $4, $5, $6, 'active', $7, $8, $9, $10, $10)`,
    documentQuery: {
      collection: 'alerts',
      operation: 'insert',
    },
    parameters: [
      { type: 'uuid' },
      { type: 'reference', referenceTable: 'organizations', referenceColumn: 'id' },
      { type: 'reference', referenceTable: 'alert_rules', referenceColumn: 'id' },
      { type: 'reference', referenceTable: 'devices', referenceColumn: 'id' },
      { type: 'enum', values: ['temperature', 'humidity', 'power'] },
      { type: 'enum', values: ['critical', 'warning', 'info'] },
      { type: 'random-decimal', min: 50, max: 100, precision: 2 },
      { type: 'random-decimal', min: 30, max: 40, precision: 2 },
      { type: 'faker', fakerMethod: 'lorem.sentence' },
      { type: 'timestamp-range', start: '2024-01-01', end: '2024-12-31' },
    ],
    expectedComplexity: 'O(log n)',
    weight: 2,
  },
  {
    name: 'resolve_alert',
    description: 'Resolve an alert',
    category: 'write',
    sql: `UPDATE alerts
          SET status = 'resolved', resolved_at = $2
          WHERE id = $1 AND status = 'active'`,
    documentQuery: {
      collection: 'alerts',
      operation: 'update',
      filter: { _id: '$1', status: 'active' },
    },
    parameters: [
      { type: 'reference', referenceTable: 'alerts', referenceColumn: 'id' },
      { type: 'timestamp-range', start: '2024-01-01', end: '2024-12-31' },
    ],
    expectedComplexity: 'O(log n)',
    weight: 1,
  },
  {
    name: 'insert_device_event',
    description: 'Log a device event',
    category: 'write',
    sql: `INSERT INTO device_events (device_id, event_type, message, severity, timestamp)
          VALUES ($1, $2, $3, $4, $5)`,
    documentQuery: {
      collection: 'device_events',
      operation: 'insert',
    },
    parameters: [
      { type: 'reference', referenceTable: 'devices', referenceColumn: 'id' },
      { type: 'enum', values: ['connected', 'disconnected', 'error', 'warning'] },
      { type: 'faker', fakerMethod: 'lorem.sentence' },
      { type: 'enum', values: ['info', 'warning', 'error'] },
      { type: 'timestamp-range', start: '2024-01-01', end: '2024-12-31' },
    ],
    expectedComplexity: 'O(log n)',
    weight: 3,
  },
]

// ============================================================================
// Workload Profiles
// ============================================================================

const workloads: WorkloadProfile[] = [
  {
    name: 'ingestion_heavy',
    description: 'High-volume data ingestion (90% writes)',
    readWriteRatio: 0.1,
    queries: benchmarkQueries.filter(q =>
      ['insert_reading', 'batch_insert_readings', 'update_device_status', 'insert_device_event', 'get_latest_reading'].includes(q.name)
    ),
    targetOps: 50000,
    concurrency: 200,
    duration: 300,
  },
  {
    name: 'balanced',
    description: 'Balanced read/write (60% reads, 40% writes)',
    readWriteRatio: 0.6,
    queries: benchmarkQueries,
    targetOps: 20000,
    concurrency: 100,
    duration: 300,
  },
  {
    name: 'dashboard',
    description: 'Dashboard queries (time-range, aggregations)',
    readWriteRatio: 0.99,
    queries: benchmarkQueries.filter(q =>
      ['readings_time_range', 'hourly_aggregates_range', 'daily_aggregates_range', 'get_latest_reading', 'active_alerts', 'device_statistics'].includes(q.name)
    ),
    targetOps: 10000,
    concurrency: 100,
    duration: 300,
  },
  {
    name: 'fleet_management',
    description: 'Device fleet management workload',
    readWriteRatio: 0.85,
    queries: benchmarkQueries.filter(q =>
      ['list_org_devices', 'list_devices_by_status', 'list_devices_by_location', 'stale_devices', 'get_device_by_serial', 'update_device_status', 'device_event_history'].includes(q.name)
    ),
    targetOps: 5000,
    concurrency: 50,
    duration: 300,
  },
  {
    name: 'alerting',
    description: 'Alert processing workload',
    readWriteRatio: 0.7,
    queries: benchmarkQueries.filter(q =>
      ['get_latest_reading', 'active_alerts', 'alert_history', 'create_alert', 'resolve_alert', 'alert_count_by_severity'].includes(q.name)
    ),
    targetOps: 8000,
    concurrency: 80,
    duration: 300,
  },
  {
    name: 'analytics',
    description: 'Heavy aggregation/analytics workload',
    readWriteRatio: 1.0,
    queries: benchmarkQueries.filter(q => q.category === 'aggregate'),
    targetOps: 500,
    concurrency: 20,
    duration: 300,
  },
]

// ============================================================================
// Dataset Configuration
// ============================================================================

export const iotTimeseriesDataset: DatasetConfig = {
  name: 'iot-timeseries',
  description: 'IoT sensor data dataset with devices, readings, alerts, and time-range queries',
  version: '1.0.0',
  tables,
  relationships,
  sizeTiers,
  workloads,
  metadata: {
    domain: 'iot',
    characteristics: [
      'High-volume write ingestion',
      'Time-range scan patterns',
      'Pre-aggregated rollup tables',
      'Alert rule evaluation',
      'Device fleet management',
    ],
    dataRetention: {
      raw_readings: '7-30 days',
      hourly_aggregates: '90 days',
      daily_aggregates: '1 year',
    },
    ingestionPatterns: {
      per_device_per_second: [0.016, 0.2, 1, 10], // 1/min, 12/min, 1/sec, 10/sec
      typical_metrics_per_device: [1, 3, 5, 10],
    },
  },
}

// Register the dataset
registerDataset(iotTimeseriesDataset)
