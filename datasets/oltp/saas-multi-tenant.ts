/**
 * Multi-Tenant SaaS OLTP Dataset
 *
 * A comprehensive multi-tenant SaaS dataset with organizations, users, projects, and tasks.
 * Represents a typical B2B SaaS workload with:
 * - Strong tenant isolation requirements
 * - Role-based access control
 * - Project management workflows
 * - Audit logging and compliance
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

const tenantsTable: TableConfig = {
  name: 'tenants',
  columns: [
    { name: 'id', type: 'uuid', primaryKey: true, generator: { type: 'uuid' } },
    { name: 'name', type: 'string', maxLength: 255, indexed: true, generator: { type: 'faker', fakerMethod: 'company.name' } },
    { name: 'slug', type: 'string', maxLength: 100, unique: true, generator: { type: 'faker', fakerMethod: 'helpers.slugify' } },
    { name: 'domain', type: 'string', maxLength: 255, unique: true, nullable: true, generator: { type: 'faker', fakerMethod: 'internet.domainName' } },
    { name: 'plan', type: 'string', maxLength: 50, indexed: true, generator: { type: 'weighted-enum', values: ['free', 'starter', 'professional', 'enterprise'], weights: [0.4, 0.3, 0.2, 0.1] } },
    { name: 'status', type: 'string', maxLength: 20, indexed: true, generator: { type: 'weighted-enum', values: ['active', 'trial', 'suspended', 'cancelled'], weights: [0.75, 0.15, 0.05, 0.05] } },
    { name: 'trial_ends_at', type: 'timestamp', nullable: true, indexed: true, generator: { type: 'timestamp-range', start: '2024-01-01', end: '2025-12-31' } },
    { name: 'max_users', type: 'integer', default: 5, generator: { type: 'weighted-enum', values: [5, 10, 25, 100, 1000], weights: [0.4, 0.25, 0.2, 0.1, 0.05] } },
    { name: 'max_projects', type: 'integer', default: 10, generator: { type: 'weighted-enum', values: [3, 10, 50, 100, 1000], weights: [0.3, 0.35, 0.2, 0.1, 0.05] } },
    { name: 'storage_limit_mb', type: 'integer', default: 1000, generator: { type: 'weighted-enum', values: [500, 1000, 5000, 50000, 500000], weights: [0.3, 0.35, 0.2, 0.1, 0.05] } },
    { name: 'storage_used_mb', type: 'integer', default: 0, generator: { type: 'random-int', min: 0, max: 5000 } },
    { name: 'billing_email', type: 'string', maxLength: 255, generator: { type: 'faker', fakerMethod: 'internet.email' } },
    { name: 'settings', type: 'json', nullable: true },
    { name: 'features', type: 'array', nullable: true },
    { name: 'created_at', type: 'timestamp', indexed: true, generator: { type: 'timestamp-range', start: '2020-01-01', end: '2024-12-31' } },
    { name: 'updated_at', type: 'timestamp', generator: { type: 'timestamp-range', start: '2023-01-01', end: '2024-12-31' } },
  ],
  indexes: [
    { name: 'idx_tenants_plan', columns: ['plan'] },
    { name: 'idx_tenants_status', columns: ['status'] },
    { name: 'idx_tenants_trial', columns: ['trial_ends_at'], where: "status = 'trial'" },
    { name: 'idx_tenants_created', columns: ['created_at'] },
  ],
}

const usersTable: TableConfig = {
  name: 'users',
  columns: [
    { name: 'id', type: 'uuid', primaryKey: true, generator: { type: 'uuid' } },
    { name: 'tenant_id', type: 'uuid', indexed: true, references: { table: 'tenants', column: 'id' }, generator: { type: 'reference', referenceTable: 'tenants', referenceColumn: 'id' } },
    { name: 'email', type: 'string', maxLength: 255, indexed: true, generator: { type: 'faker', fakerMethod: 'internet.email' } },
    { name: 'password_hash', type: 'string', maxLength: 255, generator: { type: 'random-string', length: 64 } },
    { name: 'first_name', type: 'string', maxLength: 100, generator: { type: 'faker', fakerMethod: 'person.firstName' } },
    { name: 'last_name', type: 'string', maxLength: 100, generator: { type: 'faker', fakerMethod: 'person.lastName' } },
    { name: 'display_name', type: 'string', maxLength: 200, nullable: true, generator: { type: 'faker', fakerMethod: 'person.fullName' } },
    { name: 'avatar_url', type: 'string', maxLength: 500, nullable: true, generator: { type: 'faker', fakerMethod: 'image.avatar' } },
    { name: 'role', type: 'string', maxLength: 50, indexed: true, generator: { type: 'weighted-enum', values: ['owner', 'admin', 'manager', 'member', 'viewer'], weights: [0.05, 0.1, 0.15, 0.6, 0.1] } },
    { name: 'status', type: 'string', maxLength: 20, indexed: true, generator: { type: 'weighted-enum', values: ['active', 'pending', 'suspended', 'deactivated'], weights: [0.85, 0.08, 0.04, 0.03] } },
    { name: 'timezone', type: 'string', maxLength: 50, default: 'UTC', generator: { type: 'enum', values: ['UTC', 'America/New_York', 'America/Los_Angeles', 'Europe/London', 'Asia/Tokyo'] } },
    { name: 'locale', type: 'string', maxLength: 10, default: 'en-US', generator: { type: 'enum', values: ['en-US', 'en-GB', 'es', 'fr', 'de', 'ja'] } },
    { name: 'email_verified_at', type: 'timestamp', nullable: true, generator: { type: 'timestamp-range', start: '2020-01-01', end: '2024-12-31' } },
    { name: 'last_login_at', type: 'timestamp', nullable: true, indexed: true, generator: { type: 'timestamp-range', start: '2024-01-01', end: '2024-12-31' } },
    { name: 'mfa_enabled', type: 'boolean', default: false, generator: { type: 'weighted-enum', values: [true, false], weights: [0.3, 0.7] } },
    { name: 'preferences', type: 'json', nullable: true },
    { name: 'created_at', type: 'timestamp', indexed: true, generator: { type: 'timestamp-range', start: '2020-01-01', end: '2024-12-31' } },
    { name: 'updated_at', type: 'timestamp', generator: { type: 'timestamp-range', start: '2023-01-01', end: '2024-12-31' } },
  ],
  indexes: [
    { name: 'idx_users_tenant', columns: ['tenant_id'] },
    { name: 'idx_users_tenant_email', columns: ['tenant_id', 'email'], unique: true },
    { name: 'idx_users_tenant_role', columns: ['tenant_id', 'role'] },
    { name: 'idx_users_tenant_status', columns: ['tenant_id', 'status'] },
    { name: 'idx_users_last_login', columns: ['last_login_at'] },
  ],
}

const teamsTable: TableConfig = {
  name: 'teams',
  columns: [
    { name: 'id', type: 'uuid', primaryKey: true, generator: { type: 'uuid' } },
    { name: 'tenant_id', type: 'uuid', indexed: true, references: { table: 'tenants', column: 'id' }, generator: { type: 'reference', referenceTable: 'tenants', referenceColumn: 'id' } },
    { name: 'name', type: 'string', maxLength: 100, generator: { type: 'faker', fakerMethod: 'commerce.department' } },
    { name: 'slug', type: 'string', maxLength: 100, indexed: true, generator: { type: 'faker', fakerMethod: 'helpers.slugify' } },
    { name: 'description', type: 'text', nullable: true, generator: { type: 'faker', fakerMethod: 'lorem.paragraph' } },
    { name: 'visibility', type: 'string', maxLength: 20, default: 'private', generator: { type: 'weighted-enum', values: ['private', 'internal', 'public'], weights: [0.5, 0.4, 0.1] } },
    { name: 'created_by', type: 'uuid', references: { table: 'users', column: 'id' }, generator: { type: 'reference', referenceTable: 'users', referenceColumn: 'id' } },
    { name: 'created_at', type: 'timestamp', generator: { type: 'timestamp-range', start: '2021-01-01', end: '2024-12-31' } },
    { name: 'updated_at', type: 'timestamp', generator: { type: 'timestamp-range', start: '2023-01-01', end: '2024-12-31' } },
  ],
  indexes: [
    { name: 'idx_teams_tenant', columns: ['tenant_id'] },
    { name: 'idx_teams_tenant_slug', columns: ['tenant_id', 'slug'], unique: true },
    { name: 'idx_teams_visibility', columns: ['tenant_id', 'visibility'] },
  ],
}

const teamMembersTable: TableConfig = {
  name: 'team_members',
  columns: [
    { name: 'id', type: 'uuid', primaryKey: true, generator: { type: 'uuid' } },
    { name: 'team_id', type: 'uuid', indexed: true, references: { table: 'teams', column: 'id' }, generator: { type: 'reference', referenceTable: 'teams', referenceColumn: 'id' } },
    { name: 'user_id', type: 'uuid', indexed: true, references: { table: 'users', column: 'id' }, generator: { type: 'reference', referenceTable: 'users', referenceColumn: 'id' } },
    { name: 'role', type: 'string', maxLength: 30, indexed: true, generator: { type: 'weighted-enum', values: ['lead', 'member', 'viewer'], weights: [0.1, 0.75, 0.15] } },
    { name: 'joined_at', type: 'timestamp', generator: { type: 'timestamp-range', start: '2021-01-01', end: '2024-12-31' } },
  ],
  indexes: [
    { name: 'idx_team_members_team', columns: ['team_id'] },
    { name: 'idx_team_members_user', columns: ['user_id'] },
    { name: 'idx_team_members_team_user', columns: ['team_id', 'user_id'], unique: true },
  ],
}

const projectsTable: TableConfig = {
  name: 'projects',
  columns: [
    { name: 'id', type: 'uuid', primaryKey: true, generator: { type: 'uuid' } },
    { name: 'tenant_id', type: 'uuid', indexed: true, references: { table: 'tenants', column: 'id' }, generator: { type: 'reference', referenceTable: 'tenants', referenceColumn: 'id' } },
    { name: 'team_id', type: 'uuid', nullable: true, indexed: true, references: { table: 'teams', column: 'id' }, generator: { type: 'reference', referenceTable: 'teams', referenceColumn: 'id' } },
    { name: 'name', type: 'string', maxLength: 200, indexed: true, generator: { type: 'faker', fakerMethod: 'commerce.productName' } },
    { name: 'slug', type: 'string', maxLength: 100, indexed: true, generator: { type: 'faker', fakerMethod: 'helpers.slugify' } },
    { name: 'description', type: 'text', nullable: true, generator: { type: 'faker', fakerMethod: 'lorem.paragraphs' } },
    { name: 'key', type: 'string', maxLength: 10, indexed: true, generator: { type: 'random-string', length: 4, charset: 'ABCDEFGHIJKLMNOPQRSTUVWXYZ' } },
    { name: 'status', type: 'string', maxLength: 20, indexed: true, generator: { type: 'weighted-enum', values: ['active', 'on_hold', 'completed', 'archived'], weights: [0.6, 0.15, 0.15, 0.1] } },
    { name: 'visibility', type: 'string', maxLength: 20, default: 'private', generator: { type: 'weighted-enum', values: ['private', 'team', 'internal', 'public'], weights: [0.4, 0.35, 0.2, 0.05] } },
    { name: 'color', type: 'string', maxLength: 7, nullable: true, generator: { type: 'faker', fakerMethod: 'internet.color' } },
    { name: 'icon', type: 'string', maxLength: 50, nullable: true, generator: { type: 'enum', values: ['folder', 'rocket', 'code', 'book', 'chart', 'gear'] } },
    { name: 'owner_id', type: 'uuid', indexed: true, references: { table: 'users', column: 'id' }, generator: { type: 'reference', referenceTable: 'users', referenceColumn: 'id' } },
    { name: 'start_date', type: 'date', nullable: true, indexed: true, generator: { type: 'date-range', start: '2023-01-01', end: '2024-12-31' } },
    { name: 'target_date', type: 'date', nullable: true, indexed: true, generator: { type: 'date-range', start: '2024-01-01', end: '2025-12-31' } },
    { name: 'budget', type: 'decimal', precision: 12, scale: 2, nullable: true, generator: { type: 'random-decimal', min: 1000, max: 500000, precision: 2 } },
    { name: 'budget_spent', type: 'decimal', precision: 12, scale: 2, default: 0, generator: { type: 'random-decimal', min: 0, max: 250000, precision: 2 } },
    { name: 'task_count', type: 'integer', default: 0, generator: { type: 'random-int', min: 0, max: 500 } },
    { name: 'completed_task_count', type: 'integer', default: 0, generator: { type: 'random-int', min: 0, max: 250 } },
    { name: 'settings', type: 'json', nullable: true },
    { name: 'created_at', type: 'timestamp', indexed: true, generator: { type: 'timestamp-range', start: '2021-01-01', end: '2024-12-31' } },
    { name: 'updated_at', type: 'timestamp', generator: { type: 'timestamp-range', start: '2023-01-01', end: '2024-12-31' } },
  ],
  indexes: [
    { name: 'idx_projects_tenant', columns: ['tenant_id'] },
    { name: 'idx_projects_tenant_slug', columns: ['tenant_id', 'slug'], unique: true },
    { name: 'idx_projects_tenant_key', columns: ['tenant_id', 'key'], unique: true },
    { name: 'idx_projects_team', columns: ['team_id'] },
    { name: 'idx_projects_owner', columns: ['owner_id'] },
    { name: 'idx_projects_status', columns: ['tenant_id', 'status'] },
    { name: 'idx_projects_dates', columns: ['tenant_id', 'start_date', 'target_date'] },
  ],
}

const projectMembersTable: TableConfig = {
  name: 'project_members',
  columns: [
    { name: 'id', type: 'uuid', primaryKey: true, generator: { type: 'uuid' } },
    { name: 'project_id', type: 'uuid', indexed: true, references: { table: 'projects', column: 'id' }, generator: { type: 'reference', referenceTable: 'projects', referenceColumn: 'id' } },
    { name: 'user_id', type: 'uuid', indexed: true, references: { table: 'users', column: 'id' }, generator: { type: 'reference', referenceTable: 'users', referenceColumn: 'id' } },
    { name: 'role', type: 'string', maxLength: 30, indexed: true, generator: { type: 'weighted-enum', values: ['admin', 'editor', 'member', 'viewer'], weights: [0.1, 0.2, 0.55, 0.15] } },
    { name: 'joined_at', type: 'timestamp', generator: { type: 'timestamp-range', start: '2021-01-01', end: '2024-12-31' } },
    { name: 'last_accessed_at', type: 'timestamp', nullable: true, generator: { type: 'timestamp-range', start: '2024-01-01', end: '2024-12-31' } },
  ],
  indexes: [
    { name: 'idx_project_members_project', columns: ['project_id'] },
    { name: 'idx_project_members_user', columns: ['user_id'] },
    { name: 'idx_project_members_project_user', columns: ['project_id', 'user_id'], unique: true },
  ],
}

const tasksTable: TableConfig = {
  name: 'tasks',
  columns: [
    { name: 'id', type: 'uuid', primaryKey: true, generator: { type: 'uuid' } },
    { name: 'tenant_id', type: 'uuid', indexed: true, references: { table: 'tenants', column: 'id' }, generator: { type: 'reference', referenceTable: 'tenants', referenceColumn: 'id' } },
    { name: 'project_id', type: 'uuid', indexed: true, references: { table: 'projects', column: 'id' }, generator: { type: 'reference', referenceTable: 'projects', referenceColumn: 'id' } },
    { name: 'parent_id', type: 'uuid', nullable: true, indexed: true, references: { table: 'tasks', column: 'id' }, generator: { type: 'reference', referenceTable: 'tasks', referenceColumn: 'id', distribution: 'zipf' } },
    { name: 'key', type: 'string', maxLength: 20, indexed: true, generator: { type: 'sequence' } },
    { name: 'title', type: 'string', maxLength: 500, indexed: true, generator: { type: 'faker', fakerMethod: 'lorem.sentence' } },
    { name: 'description', type: 'text', nullable: true, generator: { type: 'faker', fakerMethod: 'lorem.paragraphs' } },
    { name: 'type', type: 'string', maxLength: 30, indexed: true, generator: { type: 'weighted-enum', values: ['task', 'bug', 'story', 'epic', 'subtask'], weights: [0.5, 0.2, 0.15, 0.05, 0.1] } },
    { name: 'status', type: 'string', maxLength: 30, indexed: true, generator: { type: 'weighted-enum', values: ['backlog', 'todo', 'in_progress', 'in_review', 'done', 'cancelled'], weights: [0.2, 0.15, 0.2, 0.1, 0.3, 0.05] } },
    { name: 'priority', type: 'string', maxLength: 20, indexed: true, generator: { type: 'weighted-enum', values: ['critical', 'high', 'medium', 'low'], weights: [0.05, 0.2, 0.5, 0.25] } },
    { name: 'assignee_id', type: 'uuid', nullable: true, indexed: true, references: { table: 'users', column: 'id' }, generator: { type: 'reference', referenceTable: 'users', referenceColumn: 'id', distribution: 'zipf' } },
    { name: 'reporter_id', type: 'uuid', indexed: true, references: { table: 'users', column: 'id' }, generator: { type: 'reference', referenceTable: 'users', referenceColumn: 'id' } },
    { name: 'estimate_hours', type: 'decimal', precision: 6, scale: 2, nullable: true, generator: { type: 'random-decimal', min: 0.5, max: 40, precision: 2 } },
    { name: 'logged_hours', type: 'decimal', precision: 8, scale: 2, default: 0, generator: { type: 'random-decimal', min: 0, max: 80, precision: 2 } },
    { name: 'story_points', type: 'integer', nullable: true, generator: { type: 'weighted-enum', values: [1, 2, 3, 5, 8, 13], weights: [0.15, 0.2, 0.25, 0.2, 0.12, 0.08] } },
    { name: 'due_date', type: 'date', nullable: true, indexed: true, generator: { type: 'date-range', start: '2024-01-01', end: '2025-12-31' } },
    { name: 'started_at', type: 'timestamp', nullable: true, generator: { type: 'timestamp-range', start: '2024-01-01', end: '2024-12-31' } },
    { name: 'completed_at', type: 'timestamp', nullable: true, indexed: true, generator: { type: 'timestamp-range', start: '2024-01-01', end: '2024-12-31' } },
    { name: 'position', type: 'integer', default: 0, generator: { type: 'sequence' } },
    { name: 'labels', type: 'array', nullable: true, generator: { type: 'faker', fakerMethod: 'helpers.arrayElements' } },
    { name: 'created_at', type: 'timestamp', indexed: true, generator: { type: 'timestamp-range', start: '2022-01-01', end: '2024-12-31' } },
    { name: 'updated_at', type: 'timestamp', indexed: true, generator: { type: 'timestamp-range', start: '2023-01-01', end: '2024-12-31' } },
  ],
  indexes: [
    { name: 'idx_tasks_tenant', columns: ['tenant_id'] },
    { name: 'idx_tasks_project', columns: ['project_id'] },
    { name: 'idx_tasks_project_key', columns: ['project_id', 'key'], unique: true },
    { name: 'idx_tasks_parent', columns: ['parent_id'] },
    { name: 'idx_tasks_assignee', columns: ['assignee_id'] },
    { name: 'idx_tasks_tenant_assignee', columns: ['tenant_id', 'assignee_id'] },
    { name: 'idx_tasks_status', columns: ['project_id', 'status'] },
    { name: 'idx_tasks_priority', columns: ['project_id', 'priority'] },
    { name: 'idx_tasks_type', columns: ['project_id', 'type'] },
    { name: 'idx_tasks_due_date', columns: ['due_date'], where: 'due_date IS NOT NULL' },
    { name: 'idx_tasks_updated', columns: ['updated_at'] },
    { name: 'idx_tasks_position', columns: ['project_id', 'status', 'position'] },
  ],
  partitionBy: {
    type: 'hash',
    column: 'tenant_id',
    partitions: 16,
  },
}

const commentsTable: TableConfig = {
  name: 'comments',
  columns: [
    { name: 'id', type: 'uuid', primaryKey: true, generator: { type: 'uuid' } },
    { name: 'tenant_id', type: 'uuid', indexed: true, references: { table: 'tenants', column: 'id' }, generator: { type: 'reference', referenceTable: 'tenants', referenceColumn: 'id' } },
    { name: 'task_id', type: 'uuid', indexed: true, references: { table: 'tasks', column: 'id' }, generator: { type: 'reference', referenceTable: 'tasks', referenceColumn: 'id' } },
    { name: 'user_id', type: 'uuid', indexed: true, references: { table: 'users', column: 'id' }, generator: { type: 'reference', referenceTable: 'users', referenceColumn: 'id' } },
    { name: 'parent_id', type: 'uuid', nullable: true, indexed: true, references: { table: 'comments', column: 'id' }, generator: { type: 'reference', referenceTable: 'comments', referenceColumn: 'id', distribution: 'zipf' } },
    { name: 'body', type: 'text', generator: { type: 'faker', fakerMethod: 'lorem.paragraphs' } },
    { name: 'is_edited', type: 'boolean', default: false, generator: { type: 'weighted-enum', values: [true, false], weights: [0.15, 0.85] } },
    { name: 'reaction_count', type: 'integer', default: 0, generator: { type: 'random-int', min: 0, max: 20 } },
    { name: 'created_at', type: 'timestamp', indexed: true, generator: { type: 'timestamp-range', start: '2022-01-01', end: '2024-12-31' } },
    { name: 'updated_at', type: 'timestamp', generator: { type: 'timestamp-range', start: '2023-01-01', end: '2024-12-31' } },
  ],
  indexes: [
    { name: 'idx_comments_tenant', columns: ['tenant_id'] },
    { name: 'idx_comments_task', columns: ['task_id'] },
    { name: 'idx_comments_user', columns: ['user_id'] },
    { name: 'idx_comments_parent', columns: ['parent_id'] },
    { name: 'idx_comments_task_created', columns: ['task_id', 'created_at'] },
  ],
}

const attachmentsTable: TableConfig = {
  name: 'attachments',
  columns: [
    { name: 'id', type: 'uuid', primaryKey: true, generator: { type: 'uuid' } },
    { name: 'tenant_id', type: 'uuid', indexed: true, references: { table: 'tenants', column: 'id' }, generator: { type: 'reference', referenceTable: 'tenants', referenceColumn: 'id' } },
    { name: 'task_id', type: 'uuid', nullable: true, indexed: true, references: { table: 'tasks', column: 'id' }, generator: { type: 'reference', referenceTable: 'tasks', referenceColumn: 'id' } },
    { name: 'comment_id', type: 'uuid', nullable: true, indexed: true, references: { table: 'comments', column: 'id' }, generator: { type: 'reference', referenceTable: 'comments', referenceColumn: 'id' } },
    { name: 'uploaded_by', type: 'uuid', indexed: true, references: { table: 'users', column: 'id' }, generator: { type: 'reference', referenceTable: 'users', referenceColumn: 'id' } },
    { name: 'filename', type: 'string', maxLength: 255, generator: { type: 'faker', fakerMethod: 'system.fileName' } },
    { name: 'content_type', type: 'string', maxLength: 100, generator: { type: 'enum', values: ['image/png', 'image/jpeg', 'application/pdf', 'text/plain', 'application/zip'] } },
    { name: 'size_bytes', type: 'bigint', generator: { type: 'random-int', min: 1000, max: 50000000 } },
    { name: 'storage_path', type: 'string', maxLength: 500, generator: { type: 'faker', fakerMethod: 'system.filePath' } },
    { name: 'thumbnail_path', type: 'string', maxLength: 500, nullable: true, generator: { type: 'faker', fakerMethod: 'system.filePath' } },
    { name: 'created_at', type: 'timestamp', generator: { type: 'timestamp-range', start: '2022-01-01', end: '2024-12-31' } },
  ],
  indexes: [
    { name: 'idx_attachments_tenant', columns: ['tenant_id'] },
    { name: 'idx_attachments_task', columns: ['task_id'] },
    { name: 'idx_attachments_comment', columns: ['comment_id'] },
    { name: 'idx_attachments_uploader', columns: ['uploaded_by'] },
  ],
}

const labelsTable: TableConfig = {
  name: 'labels',
  columns: [
    { name: 'id', type: 'uuid', primaryKey: true, generator: { type: 'uuid' } },
    { name: 'tenant_id', type: 'uuid', indexed: true, references: { table: 'tenants', column: 'id' }, generator: { type: 'reference', referenceTable: 'tenants', referenceColumn: 'id' } },
    { name: 'project_id', type: 'uuid', nullable: true, indexed: true, references: { table: 'projects', column: 'id' }, generator: { type: 'reference', referenceTable: 'projects', referenceColumn: 'id' } },
    { name: 'name', type: 'string', maxLength: 50, indexed: true, generator: { type: 'faker', fakerMethod: 'word.noun' } },
    { name: 'color', type: 'string', maxLength: 7, generator: { type: 'faker', fakerMethod: 'internet.color' } },
    { name: 'description', type: 'string', maxLength: 255, nullable: true, generator: { type: 'faker', fakerMethod: 'lorem.sentence' } },
    { name: 'is_global', type: 'boolean', default: false, generator: { type: 'weighted-enum', values: [true, false], weights: [0.2, 0.8] } },
    { name: 'usage_count', type: 'integer', default: 0, generator: { type: 'random-int', min: 0, max: 1000 } },
    { name: 'created_at', type: 'timestamp', generator: { type: 'timestamp-range', start: '2021-01-01', end: '2024-12-31' } },
  ],
  indexes: [
    { name: 'idx_labels_tenant', columns: ['tenant_id'] },
    { name: 'idx_labels_project', columns: ['project_id'] },
    { name: 'idx_labels_tenant_name', columns: ['tenant_id', 'name'] },
    { name: 'idx_labels_global', columns: ['tenant_id', 'is_global'] },
  ],
}

const taskLabelsTable: TableConfig = {
  name: 'task_labels',
  columns: [
    { name: 'task_id', type: 'uuid', indexed: true, references: { table: 'tasks', column: 'id' }, generator: { type: 'reference', referenceTable: 'tasks', referenceColumn: 'id' } },
    { name: 'label_id', type: 'uuid', indexed: true, references: { table: 'labels', column: 'id' }, generator: { type: 'reference', referenceTable: 'labels', referenceColumn: 'id' } },
    { name: 'created_at', type: 'timestamp', generator: { type: 'timestamp-range', start: '2022-01-01', end: '2024-12-31' } },
  ],
  indexes: [
    { name: 'idx_task_labels_task', columns: ['task_id'] },
    { name: 'idx_task_labels_label', columns: ['label_id'] },
    { name: 'idx_task_labels_pk', columns: ['task_id', 'label_id'], unique: true },
  ],
}

const timeEntriesTable: TableConfig = {
  name: 'time_entries',
  columns: [
    { name: 'id', type: 'uuid', primaryKey: true, generator: { type: 'uuid' } },
    { name: 'tenant_id', type: 'uuid', indexed: true, references: { table: 'tenants', column: 'id' }, generator: { type: 'reference', referenceTable: 'tenants', referenceColumn: 'id' } },
    { name: 'task_id', type: 'uuid', indexed: true, references: { table: 'tasks', column: 'id' }, generator: { type: 'reference', referenceTable: 'tasks', referenceColumn: 'id' } },
    { name: 'user_id', type: 'uuid', indexed: true, references: { table: 'users', column: 'id' }, generator: { type: 'reference', referenceTable: 'users', referenceColumn: 'id' } },
    { name: 'description', type: 'string', maxLength: 500, nullable: true, generator: { type: 'faker', fakerMethod: 'lorem.sentence' } },
    { name: 'hours', type: 'decimal', precision: 6, scale: 2, generator: { type: 'random-decimal', min: 0.25, max: 8, precision: 2 } },
    { name: 'billable', type: 'boolean', default: true, generator: { type: 'weighted-enum', values: [true, false], weights: [0.8, 0.2] } },
    { name: 'hourly_rate', type: 'decimal', precision: 8, scale: 2, nullable: true, generator: { type: 'random-decimal', min: 50, max: 300, precision: 2 } },
    { name: 'date', type: 'date', indexed: true, generator: { type: 'date-range', start: '2024-01-01', end: '2024-12-31' } },
    { name: 'started_at', type: 'timestamp', nullable: true, generator: { type: 'timestamp-range', start: '2024-01-01', end: '2024-12-31' } },
    { name: 'ended_at', type: 'timestamp', nullable: true, generator: { type: 'timestamp-range', start: '2024-01-01', end: '2024-12-31' } },
    { name: 'created_at', type: 'timestamp', generator: { type: 'timestamp-range', start: '2024-01-01', end: '2024-12-31' } },
  ],
  indexes: [
    { name: 'idx_time_entries_tenant', columns: ['tenant_id'] },
    { name: 'idx_time_entries_task', columns: ['task_id'] },
    { name: 'idx_time_entries_user', columns: ['user_id'] },
    { name: 'idx_time_entries_tenant_user_date', columns: ['tenant_id', 'user_id', 'date'] },
    { name: 'idx_time_entries_date', columns: ['date'] },
    { name: 'idx_time_entries_billable', columns: ['tenant_id', 'billable', 'date'] },
  ],
}

const auditLogsTable: TableConfig = {
  name: 'audit_logs',
  columns: [
    { name: 'id', type: 'uuid', primaryKey: true, generator: { type: 'uuid' } },
    { name: 'tenant_id', type: 'uuid', indexed: true, references: { table: 'tenants', column: 'id' }, generator: { type: 'reference', referenceTable: 'tenants', referenceColumn: 'id' } },
    { name: 'user_id', type: 'uuid', nullable: true, indexed: true, references: { table: 'users', column: 'id' }, generator: { type: 'reference', referenceTable: 'users', referenceColumn: 'id' } },
    { name: 'action', type: 'string', maxLength: 50, indexed: true, generator: { type: 'enum', values: ['create', 'update', 'delete', 'login', 'logout', 'export', 'invite', 'permission_change'] } },
    { name: 'resource_type', type: 'string', maxLength: 50, indexed: true, generator: { type: 'enum', values: ['user', 'project', 'task', 'comment', 'team', 'settings'] } },
    { name: 'resource_id', type: 'uuid', indexed: true, generator: { type: 'uuid' } },
    { name: 'changes', type: 'json', nullable: true },
    { name: 'ip_address', type: 'string', maxLength: 45, nullable: true, generator: { type: 'faker', fakerMethod: 'internet.ip' } },
    { name: 'user_agent', type: 'string', maxLength: 500, nullable: true, generator: { type: 'faker', fakerMethod: 'internet.userAgent' } },
    { name: 'created_at', type: 'timestamp', indexed: true, generator: { type: 'timestamp-range', start: '2023-01-01', end: '2024-12-31' } },
  ],
  indexes: [
    { name: 'idx_audit_logs_tenant', columns: ['tenant_id'] },
    { name: 'idx_audit_logs_user', columns: ['user_id'] },
    { name: 'idx_audit_logs_action', columns: ['tenant_id', 'action'] },
    { name: 'idx_audit_logs_resource', columns: ['tenant_id', 'resource_type', 'resource_id'] },
    { name: 'idx_audit_logs_created', columns: ['tenant_id', 'created_at'] },
  ],
  partitionBy: {
    type: 'range',
    column: 'created_at',
  },
}

const apiKeysTable: TableConfig = {
  name: 'api_keys',
  columns: [
    { name: 'id', type: 'uuid', primaryKey: true, generator: { type: 'uuid' } },
    { name: 'tenant_id', type: 'uuid', indexed: true, references: { table: 'tenants', column: 'id' }, generator: { type: 'reference', referenceTable: 'tenants', referenceColumn: 'id' } },
    { name: 'user_id', type: 'uuid', indexed: true, references: { table: 'users', column: 'id' }, generator: { type: 'reference', referenceTable: 'users', referenceColumn: 'id' } },
    { name: 'name', type: 'string', maxLength: 100, generator: { type: 'faker', fakerMethod: 'word.noun' } },
    { name: 'key_hash', type: 'string', maxLength: 255, unique: true, generator: { type: 'random-string', length: 64 } },
    { name: 'key_prefix', type: 'string', maxLength: 20, indexed: true, generator: { type: 'random-string', length: 8 } },
    { name: 'scopes', type: 'array', generator: { type: 'faker', fakerMethod: 'helpers.arrayElements' } },
    { name: 'last_used_at', type: 'timestamp', nullable: true, indexed: true, generator: { type: 'timestamp-range', start: '2024-01-01', end: '2024-12-31' } },
    { name: 'expires_at', type: 'timestamp', nullable: true, indexed: true, generator: { type: 'timestamp-range', start: '2024-06-01', end: '2026-12-31' } },
    { name: 'is_active', type: 'boolean', default: true, indexed: true, generator: { type: 'weighted-enum', values: [true, false], weights: [0.9, 0.1] } },
    { name: 'created_at', type: 'timestamp', generator: { type: 'timestamp-range', start: '2022-01-01', end: '2024-12-31' } },
  ],
  indexes: [
    { name: 'idx_api_keys_tenant', columns: ['tenant_id'] },
    { name: 'idx_api_keys_user', columns: ['user_id'] },
    { name: 'idx_api_keys_prefix', columns: ['key_prefix'] },
    { name: 'idx_api_keys_active', columns: ['tenant_id', 'is_active'] },
  ],
}

// ============================================================================
// Tables Array
// ============================================================================

const tables: TableConfig[] = [
  tenantsTable,
  usersTable,
  teamsTable,
  teamMembersTable,
  projectsTable,
  projectMembersTable,
  tasksTable,
  commentsTable,
  attachmentsTable,
  labelsTable,
  taskLabelsTable,
  timeEntriesTable,
  auditLogsTable,
  apiKeysTable,
]

// ============================================================================
// Relationships
// ============================================================================

const relationships: RelationshipConfig[] = [
  {
    name: 'tenant_users',
    type: 'one-to-many',
    from: { table: 'tenants', column: 'id' },
    to: { table: 'users', column: 'tenant_id' },
    onDelete: 'cascade',
  },
  {
    name: 'tenant_teams',
    type: 'one-to-many',
    from: { table: 'tenants', column: 'id' },
    to: { table: 'teams', column: 'tenant_id' },
    onDelete: 'cascade',
  },
  {
    name: 'tenant_projects',
    type: 'one-to-many',
    from: { table: 'tenants', column: 'id' },
    to: { table: 'projects', column: 'tenant_id' },
    onDelete: 'cascade',
  },
  {
    name: 'tenant_tasks',
    type: 'one-to-many',
    from: { table: 'tenants', column: 'id' },
    to: { table: 'tasks', column: 'tenant_id' },
    onDelete: 'cascade',
  },
  {
    name: 'team_members',
    type: 'many-to-many',
    from: { table: 'teams', column: 'id' },
    to: { table: 'users', column: 'id' },
    through: {
      table: 'team_members',
      fromColumn: 'team_id',
      toColumn: 'user_id',
    },
    onDelete: 'cascade',
  },
  {
    name: 'project_team',
    type: 'one-to-many',
    from: { table: 'teams', column: 'id' },
    to: { table: 'projects', column: 'team_id' },
    onDelete: 'set-null',
  },
  {
    name: 'project_members',
    type: 'many-to-many',
    from: { table: 'projects', column: 'id' },
    to: { table: 'users', column: 'id' },
    through: {
      table: 'project_members',
      fromColumn: 'project_id',
      toColumn: 'user_id',
    },
    onDelete: 'cascade',
  },
  {
    name: 'project_tasks',
    type: 'one-to-many',
    from: { table: 'projects', column: 'id' },
    to: { table: 'tasks', column: 'project_id' },
    onDelete: 'cascade',
  },
  {
    name: 'task_subtasks',
    type: 'one-to-many',
    from: { table: 'tasks', column: 'id' },
    to: { table: 'tasks', column: 'parent_id' },
    onDelete: 'set-null',
  },
  {
    name: 'task_assignee',
    type: 'one-to-many',
    from: { table: 'users', column: 'id' },
    to: { table: 'tasks', column: 'assignee_id' },
    onDelete: 'set-null',
  },
  {
    name: 'task_comments',
    type: 'one-to-many',
    from: { table: 'tasks', column: 'id' },
    to: { table: 'comments', column: 'task_id' },
    onDelete: 'cascade',
  },
  {
    name: 'task_labels',
    type: 'many-to-many',
    from: { table: 'tasks', column: 'id' },
    to: { table: 'labels', column: 'id' },
    through: {
      table: 'task_labels',
      fromColumn: 'task_id',
      toColumn: 'label_id',
    },
    onDelete: 'cascade',
  },
  {
    name: 'task_time_entries',
    type: 'one-to-many',
    from: { table: 'tasks', column: 'id' },
    to: { table: 'time_entries', column: 'task_id' },
    onDelete: 'cascade',
  },
  {
    name: 'task_attachments',
    type: 'one-to-many',
    from: { table: 'tasks', column: 'id' },
    to: { table: 'attachments', column: 'task_id' },
    onDelete: 'cascade',
  },
  {
    name: 'user_api_keys',
    type: 'one-to-many',
    from: { table: 'users', column: 'id' },
    to: { table: 'api_keys', column: 'user_id' },
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
      tenants: 10,
      users: 100,
      teams: 30,
      team_members: 150,
      projects: 50,
      project_members: 200,
      tasks: 500,
      comments: 1500,
      attachments: 300,
      labels: 100,
      task_labels: 800,
      time_entries: 1000,
      audit_logs: 2000,
      api_keys: 50,
    },
    estimatedBytes: 1_048_576,
    recommendedMemoryMB: 128,
  },
  {
    size: '10mb',
    seedCount: {
      tenants: 50,
      users: 1000,
      teams: 200,
      team_members: 1500,
      projects: 400,
      project_members: 2000,
      tasks: 5000,
      comments: 15000,
      attachments: 3000,
      labels: 500,
      task_labels: 8000,
      time_entries: 10000,
      audit_logs: 20000,
      api_keys: 500,
    },
    estimatedBytes: 10_485_760,
    recommendedMemoryMB: 256,
  },
  {
    size: '100mb',
    seedCount: {
      tenants: 200,
      users: 10000,
      teams: 1500,
      team_members: 15000,
      projects: 3000,
      project_members: 20000,
      tasks: 50000,
      comments: 150000,
      attachments: 30000,
      labels: 3000,
      task_labels: 80000,
      time_entries: 100000,
      audit_logs: 200000,
      api_keys: 5000,
    },
    estimatedBytes: 104_857_600,
    recommendedMemoryMB: 512,
  },
  {
    size: '1gb',
    seedCount: {
      tenants: 1000,
      users: 100000,
      teams: 10000,
      team_members: 150000,
      projects: 25000,
      project_members: 200000,
      tasks: 500000,
      comments: 1500000,
      attachments: 300000,
      labels: 20000,
      task_labels: 800000,
      time_entries: 1000000,
      audit_logs: 2000000,
      api_keys: 50000,
    },
    estimatedBytes: 1_073_741_824,
    recommendedMemoryMB: 2048,
    recommendedCores: 2,
  },
  {
    size: '10gb',
    seedCount: {
      tenants: 5000,
      users: 1000000,
      teams: 75000,
      team_members: 1500000,
      projects: 200000,
      project_members: 2000000,
      tasks: 5000000,
      comments: 15000000,
      attachments: 3000000,
      labels: 150000,
      task_labels: 8000000,
      time_entries: 10000000,
      audit_logs: 20000000,
      api_keys: 500000,
    },
    estimatedBytes: 10_737_418_240,
    recommendedMemoryMB: 8192,
    recommendedCores: 4,
  },
  {
    size: '20gb',
    seedCount: {
      tenants: 10000,
      users: 2000000,
      teams: 150000,
      team_members: 3000000,
      projects: 400000,
      project_members: 4000000,
      tasks: 10000000,
      comments: 30000000,
      attachments: 6000000,
      labels: 300000,
      task_labels: 16000000,
      time_entries: 20000000,
      audit_logs: 40000000,
      api_keys: 1000000,
    },
    estimatedBytes: 21_474_836_480,
    recommendedMemoryMB: 16384,
    recommendedCores: 8,
  },
  {
    size: '30gb',
    seedCount: {
      tenants: 15000,
      users: 3000000,
      teams: 225000,
      team_members: 4500000,
      projects: 600000,
      project_members: 6000000,
      tasks: 15000000,
      comments: 45000000,
      attachments: 9000000,
      labels: 450000,
      task_labels: 24000000,
      time_entries: 30000000,
      audit_logs: 60000000,
      api_keys: 1500000,
    },
    estimatedBytes: 32_212_254_720,
    recommendedMemoryMB: 24576,
    recommendedCores: 12,
  },
  {
    size: '50gb',
    seedCount: {
      tenants: 25000,
      users: 5000000,
      teams: 375000,
      team_members: 7500000,
      projects: 1000000,
      project_members: 10000000,
      tasks: 25000000,
      comments: 75000000,
      attachments: 15000000,
      labels: 750000,
      task_labels: 40000000,
      time_entries: 50000000,
      audit_logs: 100000000,
      api_keys: 2500000,
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
  // Tenant-scoped point lookups
  {
    name: 'get_user_by_id_tenant',
    description: 'Fetch user by ID within tenant scope',
    category: 'point-lookup',
    sql: `SELECT * FROM users WHERE id = $1 AND tenant_id = $2`,
    documentQuery: {
      collection: 'users',
      operation: 'find',
      filter: { _id: '$1', tenant_id: '$2' },
    },
    parameters: [
      { type: 'reference', referenceTable: 'users', referenceColumn: 'id' },
      { type: 'reference', referenceTable: 'tenants', referenceColumn: 'id' },
    ],
    expectedComplexity: 'O(1)',
    weight: 12,
  },
  {
    name: 'get_user_by_email_tenant',
    description: 'Fetch user by email within tenant (unique constraint)',
    category: 'point-lookup',
    sql: `SELECT * FROM users WHERE email = $1 AND tenant_id = $2`,
    documentQuery: {
      collection: 'users',
      operation: 'find',
      filter: { email: '$1', tenant_id: '$2' },
    },
    parameters: [
      { type: 'faker', fakerMethod: 'internet.email' },
      { type: 'reference', referenceTable: 'tenants', referenceColumn: 'id' },
    ],
    expectedComplexity: 'O(1)',
    weight: 8,
  },
  {
    name: 'get_task_by_key',
    description: 'Fetch task by project key (e.g., PROJ-123)',
    category: 'point-lookup',
    sql: `SELECT * FROM tasks WHERE project_id = $1 AND key = $2`,
    documentQuery: {
      collection: 'tasks',
      operation: 'find',
      filter: { project_id: '$1', key: '$2' },
    },
    parameters: [
      { type: 'reference', referenceTable: 'projects', referenceColumn: 'id' },
      { type: 'sequence' },
    ],
    expectedComplexity: 'O(1)',
    weight: 10,
  },
  {
    name: 'get_project_by_slug',
    description: 'Fetch project by tenant and slug',
    category: 'point-lookup',
    sql: `SELECT * FROM projects WHERE tenant_id = $1 AND slug = $2`,
    documentQuery: {
      collection: 'projects',
      operation: 'find',
      filter: { tenant_id: '$1', slug: '$2' },
    },
    parameters: [
      { type: 'reference', referenceTable: 'tenants', referenceColumn: 'id' },
      { type: 'faker', fakerMethod: 'helpers.slugify' },
    ],
    expectedComplexity: 'O(1)',
    weight: 6,
  },

  // Tenant-scoped range scans
  {
    name: 'list_tenant_users',
    description: 'List all users in a tenant with pagination',
    category: 'range-scan',
    sql: `SELECT * FROM users
          WHERE tenant_id = $1
          ORDER BY created_at DESC
          LIMIT 50 OFFSET $2`,
    documentQuery: {
      collection: 'users',
      operation: 'find',
      filter: { tenant_id: '$1' },
    },
    parameters: [
      { type: 'reference', referenceTable: 'tenants', referenceColumn: 'id' },
      { type: 'random-int', min: 0, max: 100 },
    ],
    expectedComplexity: 'O(log n)',
    weight: 8,
  },
  {
    name: 'list_project_tasks',
    description: 'List tasks in a project with status filter',
    category: 'range-scan',
    sql: `SELECT * FROM tasks
          WHERE project_id = $1 AND status = $2
          ORDER BY position
          LIMIT 100`,
    documentQuery: {
      collection: 'tasks',
      operation: 'find',
      filter: { project_id: '$1', status: '$2' },
    },
    parameters: [
      { type: 'reference', referenceTable: 'projects', referenceColumn: 'id' },
      { type: 'enum', values: ['backlog', 'todo', 'in_progress', 'in_review', 'done'] },
    ],
    expectedComplexity: 'O(log n)',
    weight: 12,
  },
  {
    name: 'list_user_assigned_tasks',
    description: 'List tasks assigned to a user across all projects in tenant',
    category: 'range-scan',
    sql: `SELECT t.*, p.name as project_name
          FROM tasks t
          JOIN projects p ON t.project_id = p.id
          WHERE t.tenant_id = $1 AND t.assignee_id = $2 AND t.status NOT IN ('done', 'cancelled')
          ORDER BY t.due_date NULLS LAST, t.priority
          LIMIT 50`,
    documentQuery: {
      collection: 'tasks',
      operation: 'find',
      filter: { tenant_id: '$1', assignee_id: '$2', status: { $nin: ['done', 'cancelled'] } },
    },
    parameters: [
      { type: 'reference', referenceTable: 'tenants', referenceColumn: 'id' },
      { type: 'reference', referenceTable: 'users', referenceColumn: 'id', distribution: 'zipf' },
    ],
    expectedComplexity: 'O(log n)',
    weight: 10,
  },
  {
    name: 'list_overdue_tasks',
    description: 'List overdue tasks in tenant',
    category: 'range-scan',
    sql: `SELECT t.*, p.name as project_name, u.email as assignee_email
          FROM tasks t
          JOIN projects p ON t.project_id = p.id
          LEFT JOIN users u ON t.assignee_id = u.id
          WHERE t.tenant_id = $1 AND t.due_date < CURRENT_DATE AND t.status NOT IN ('done', 'cancelled')
          ORDER BY t.due_date ASC
          LIMIT 100`,
    documentQuery: {
      collection: 'tasks',
      operation: 'find',
      filter: { tenant_id: '$1', due_date: { $lt: new Date() }, status: { $nin: ['done', 'cancelled'] } },
    },
    parameters: [
      { type: 'reference', referenceTable: 'tenants', referenceColumn: 'id' },
    ],
    expectedComplexity: 'O(log n)',
    weight: 4,
  },
  {
    name: 'list_recent_activity',
    description: 'List recent audit log entries for tenant',
    category: 'range-scan',
    sql: `SELECT * FROM audit_logs
          WHERE tenant_id = $1 AND created_at >= $2
          ORDER BY created_at DESC
          LIMIT 100`,
    documentQuery: {
      collection: 'audit_logs',
      operation: 'find',
      filter: { tenant_id: '$1', created_at: { $gte: '$2' } },
    },
    parameters: [
      { type: 'reference', referenceTable: 'tenants', referenceColumn: 'id' },
      { type: 'timestamp-range', start: '2024-10-01', end: '2024-12-01' },
    ],
    expectedComplexity: 'O(log n)',
    weight: 5,
  },

  // Cross-tenant queries (admin/platform queries)
  {
    name: 'list_all_tenants',
    description: 'List all tenants with user counts (platform admin)',
    category: 'aggregate',
    sql: `SELECT t.*, COUNT(u.id) as user_count
          FROM tenants t
          LEFT JOIN users u ON t.id = u.tenant_id
          WHERE t.status = $1
          GROUP BY t.id
          ORDER BY t.created_at DESC
          LIMIT 50`,
    documentQuery: {
      collection: 'tenants',
      operation: 'aggregate',
      pipeline: [
        { $match: { status: '$1' } },
        { $lookup: { from: 'users', localField: '_id', foreignField: 'tenant_id', as: 'users' } },
        { $addFields: { user_count: { $size: '$users' } } },
        { $sort: { created_at: -1 } },
        { $limit: 50 },
      ],
    },
    parameters: [
      { type: 'enum', values: ['active', 'trial'] },
    ],
    expectedComplexity: 'O(n)',
    weight: 1,
  },
  {
    name: 'get_tenant_usage_metrics',
    description: 'Get storage and usage metrics for a tenant',
    category: 'aggregate',
    sql: `SELECT
            t.id, t.name, t.storage_used_mb, t.storage_limit_mb,
            COUNT(DISTINCT u.id) as user_count,
            COUNT(DISTINCT p.id) as project_count,
            COUNT(DISTINCT tk.id) as task_count
          FROM tenants t
          LEFT JOIN users u ON t.id = u.tenant_id
          LEFT JOIN projects p ON t.id = p.tenant_id
          LEFT JOIN tasks tk ON t.id = tk.tenant_id
          WHERE t.id = $1
          GROUP BY t.id, t.name, t.storage_used_mb, t.storage_limit_mb`,
    documentQuery: {
      collection: 'tenants',
      operation: 'aggregate',
      pipeline: [
        { $match: { _id: '$1' } },
        { $lookup: { from: 'users', localField: '_id', foreignField: 'tenant_id', as: 'users' } },
        { $lookup: { from: 'projects', localField: '_id', foreignField: 'tenant_id', as: 'projects' } },
        { $lookup: { from: 'tasks', localField: '_id', foreignField: 'tenant_id', as: 'tasks' } },
      ],
    },
    parameters: [
      { type: 'reference', referenceTable: 'tenants', referenceColumn: 'id' },
    ],
    expectedComplexity: 'O(n)',
    weight: 2,
  },

  // Joins
  {
    name: 'get_task_with_comments',
    description: 'Fetch task with all comments and authors',
    category: 'join',
    sql: `SELECT t.*, c.id as comment_id, c.body, c.created_at as comment_created,
                 u.first_name, u.last_name, u.email
          FROM tasks t
          LEFT JOIN comments c ON t.id = c.task_id
          LEFT JOIN users u ON c.user_id = u.id
          WHERE t.id = $1
          ORDER BY c.created_at DESC`,
    documentQuery: {
      collection: 'tasks',
      operation: 'aggregate',
      pipeline: [
        { $match: { _id: '$1' } },
        { $lookup: { from: 'comments', localField: '_id', foreignField: 'task_id', as: 'comments' } },
      ],
    },
    parameters: [
      { type: 'reference', referenceTable: 'tasks', referenceColumn: 'id' },
    ],
    expectedComplexity: 'O(log n)',
    weight: 6,
  },
  {
    name: 'get_project_with_members',
    description: 'Fetch project with all team members',
    category: 'join',
    sql: `SELECT p.*, pm.role, u.id as user_id, u.first_name, u.last_name, u.email, u.avatar_url
          FROM projects p
          JOIN project_members pm ON p.id = pm.project_id
          JOIN users u ON pm.user_id = u.id
          WHERE p.id = $1
          ORDER BY pm.role, u.first_name`,
    documentQuery: {
      collection: 'projects',
      operation: 'aggregate',
      pipeline: [
        { $match: { _id: '$1' } },
        { $lookup: { from: 'project_members', localField: '_id', foreignField: 'project_id', as: 'members' } },
      ],
    },
    parameters: [
      { type: 'reference', referenceTable: 'projects', referenceColumn: 'id' },
    ],
    expectedComplexity: 'O(log n)',
    weight: 5,
  },
  {
    name: 'get_user_with_teams',
    description: 'Fetch user with all team memberships',
    category: 'join',
    sql: `SELECT u.*, tm.role as team_role, t.id as team_id, t.name as team_name
          FROM users u
          LEFT JOIN team_members tm ON u.id = tm.user_id
          LEFT JOIN teams t ON tm.team_id = t.id
          WHERE u.id = $1`,
    documentQuery: {
      collection: 'users',
      operation: 'aggregate',
      pipeline: [
        { $match: { _id: '$1' } },
        { $lookup: { from: 'team_members', localField: '_id', foreignField: 'user_id', as: 'memberships' } },
      ],
    },
    parameters: [
      { type: 'reference', referenceTable: 'users', referenceColumn: 'id' },
    ],
    expectedComplexity: 'O(log n)',
    weight: 4,
  },

  // Aggregates
  {
    name: 'project_task_summary',
    description: 'Get task counts by status for a project',
    category: 'aggregate',
    sql: `SELECT status, COUNT(*) as count
          FROM tasks
          WHERE project_id = $1
          GROUP BY status`,
    documentQuery: {
      collection: 'tasks',
      operation: 'aggregate',
      pipeline: [
        { $match: { project_id: '$1' } },
        { $group: { _id: '$status', count: { $sum: 1 } } },
      ],
    },
    parameters: [
      { type: 'reference', referenceTable: 'projects', referenceColumn: 'id' },
    ],
    expectedComplexity: 'O(n)',
    weight: 4,
  },
  {
    name: 'user_time_summary',
    description: 'Get time logged summary for a user in date range',
    category: 'aggregate',
    sql: `SELECT
            SUM(hours) as total_hours,
            SUM(CASE WHEN billable THEN hours ELSE 0 END) as billable_hours,
            COUNT(DISTINCT task_id) as tasks_worked,
            COUNT(DISTINCT date) as days_worked
          FROM time_entries
          WHERE tenant_id = $1 AND user_id = $2 AND date BETWEEN $3 AND $4`,
    documentQuery: {
      collection: 'time_entries',
      operation: 'aggregate',
      pipeline: [
        { $match: { tenant_id: '$1', user_id: '$2', date: { $gte: '$3', $lte: '$4' } } },
        { $group: { _id: null, total_hours: { $sum: '$hours' }, billable_hours: { $sum: { $cond: ['$billable', '$hours', 0] } } } },
      ],
    },
    parameters: [
      { type: 'reference', referenceTable: 'tenants', referenceColumn: 'id' },
      { type: 'reference', referenceTable: 'users', referenceColumn: 'id' },
      { type: 'date-range', start: '2024-01-01', end: '2024-03-01' },
      { type: 'date-range', start: '2024-03-01', end: '2024-06-01' },
    ],
    expectedComplexity: 'O(n)',
    weight: 3,
  },
  {
    name: 'team_productivity',
    description: 'Calculate team productivity metrics',
    category: 'aggregate',
    sql: `SELECT
            u.id, u.first_name, u.last_name,
            COUNT(CASE WHEN t.status = 'done' THEN 1 END) as completed_tasks,
            SUM(t.story_points) FILTER (WHERE t.status = 'done') as completed_points,
            AVG(EXTRACT(EPOCH FROM (t.completed_at - t.started_at))/3600) as avg_hours_to_complete
          FROM users u
          JOIN team_members tm ON u.id = tm.user_id
          LEFT JOIN tasks t ON u.id = t.assignee_id AND t.completed_at >= $2
          WHERE tm.team_id = $1
          GROUP BY u.id, u.first_name, u.last_name
          ORDER BY completed_tasks DESC`,
    documentQuery: {
      collection: 'tasks',
      operation: 'aggregate',
      pipeline: [
        { $match: { assignee_id: { $in: [] }, completed_at: { $gte: '$2' } } },
        { $group: { _id: '$assignee_id', completed_tasks: { $sum: 1 }, completed_points: { $sum: '$story_points' } } },
      ],
    },
    parameters: [
      { type: 'reference', referenceTable: 'teams', referenceColumn: 'id' },
      { type: 'timestamp-range', start: '2024-01-01', end: '2024-06-01' },
    ],
    expectedComplexity: 'O(n)',
    weight: 2,
  },

  // Write operations
  {
    name: 'create_task',
    description: 'Create a new task',
    category: 'write',
    sql: `INSERT INTO tasks (id, tenant_id, project_id, key, title, type, status, priority, reporter_id, created_at, updated_at)
          VALUES ($1, $2, $3, $4, $5, $6, 'backlog', $7, $8, NOW(), NOW())
          RETURNING *`,
    documentQuery: {
      collection: 'tasks',
      operation: 'insert',
    },
    parameters: [
      { type: 'uuid' },
      { type: 'reference', referenceTable: 'tenants', referenceColumn: 'id' },
      { type: 'reference', referenceTable: 'projects', referenceColumn: 'id' },
      { type: 'sequence' },
      { type: 'faker', fakerMethod: 'lorem.sentence' },
      { type: 'enum', values: ['task', 'bug', 'story'] },
      { type: 'enum', values: ['low', 'medium', 'high'] },
      { type: 'reference', referenceTable: 'users', referenceColumn: 'id' },
    ],
    expectedComplexity: 'O(log n)',
    weight: 5,
  },
  {
    name: 'update_task_status',
    description: 'Update task status (Kanban move)',
    category: 'write',
    sql: `UPDATE tasks
          SET status = $2, updated_at = NOW(),
              started_at = CASE WHEN $2 = 'in_progress' AND started_at IS NULL THEN NOW() ELSE started_at END,
              completed_at = CASE WHEN $2 = 'done' THEN NOW() ELSE completed_at END
          WHERE id = $1 AND tenant_id = $3
          RETURNING *`,
    documentQuery: {
      collection: 'tasks',
      operation: 'update',
      filter: { _id: '$1', tenant_id: '$3' },
    },
    parameters: [
      { type: 'reference', referenceTable: 'tasks', referenceColumn: 'id' },
      { type: 'enum', values: ['todo', 'in_progress', 'in_review', 'done'] },
      { type: 'reference', referenceTable: 'tenants', referenceColumn: 'id' },
    ],
    expectedComplexity: 'O(log n)',
    weight: 4,
  },
  {
    name: 'assign_task',
    description: 'Assign or reassign a task',
    category: 'write',
    sql: `UPDATE tasks
          SET assignee_id = $2, updated_at = NOW()
          WHERE id = $1 AND tenant_id = $3
          RETURNING *`,
    documentQuery: {
      collection: 'tasks',
      operation: 'update',
      filter: { _id: '$1', tenant_id: '$3' },
    },
    parameters: [
      { type: 'reference', referenceTable: 'tasks', referenceColumn: 'id' },
      { type: 'reference', referenceTable: 'users', referenceColumn: 'id' },
      { type: 'reference', referenceTable: 'tenants', referenceColumn: 'id' },
    ],
    expectedComplexity: 'O(log n)',
    weight: 3,
  },
  {
    name: 'add_comment',
    description: 'Add a comment to a task',
    category: 'write',
    sql: `INSERT INTO comments (id, tenant_id, task_id, user_id, body, created_at, updated_at)
          VALUES ($1, $2, $3, $4, $5, NOW(), NOW())
          RETURNING *`,
    documentQuery: {
      collection: 'comments',
      operation: 'insert',
    },
    parameters: [
      { type: 'uuid' },
      { type: 'reference', referenceTable: 'tenants', referenceColumn: 'id' },
      { type: 'reference', referenceTable: 'tasks', referenceColumn: 'id' },
      { type: 'reference', referenceTable: 'users', referenceColumn: 'id' },
      { type: 'faker', fakerMethod: 'lorem.paragraphs' },
    ],
    expectedComplexity: 'O(log n)',
    weight: 3,
  },
  {
    name: 'log_time',
    description: 'Log time entry for a task',
    category: 'write',
    sql: `INSERT INTO time_entries (id, tenant_id, task_id, user_id, hours, billable, date, description, created_at)
          VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW())
          RETURNING *`,
    documentQuery: {
      collection: 'time_entries',
      operation: 'insert',
    },
    parameters: [
      { type: 'uuid' },
      { type: 'reference', referenceTable: 'tenants', referenceColumn: 'id' },
      { type: 'reference', referenceTable: 'tasks', referenceColumn: 'id' },
      { type: 'reference', referenceTable: 'users', referenceColumn: 'id' },
      { type: 'random-decimal', min: 0.25, max: 8, precision: 2 },
      { type: 'random-boolean' },
      { type: 'date-range', start: '2024-01-01', end: '2024-12-31' },
      { type: 'faker', fakerMethod: 'lorem.sentence' },
    ],
    expectedComplexity: 'O(log n)',
    weight: 2,
  },
  {
    name: 'create_audit_log',
    description: 'Create audit log entry',
    category: 'write',
    sql: `INSERT INTO audit_logs (id, tenant_id, user_id, action, resource_type, resource_id, changes, ip_address, created_at)
          VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW())
          RETURNING *`,
    documentQuery: {
      collection: 'audit_logs',
      operation: 'insert',
    },
    parameters: [
      { type: 'uuid' },
      { type: 'reference', referenceTable: 'tenants', referenceColumn: 'id' },
      { type: 'reference', referenceTable: 'users', referenceColumn: 'id' },
      { type: 'enum', values: ['create', 'update', 'delete'] },
      { type: 'enum', values: ['task', 'project', 'comment'] },
      { type: 'uuid' },
      { type: 'faker', fakerMethod: 'datatype.json' },
      { type: 'faker', fakerMethod: 'internet.ip' },
    ],
    expectedComplexity: 'O(log n)',
    weight: 2,
  },
]

// ============================================================================
// Workload Profiles
// ============================================================================

const workloads: WorkloadProfile[] = [
  {
    name: 'read_heavy',
    description: 'Typical SaaS read-heavy workload (90% reads, 10% writes)',
    readWriteRatio: 0.9,
    queries: benchmarkQueries.filter(q => q.category !== 'write').map(q => ({ ...q })),
    targetOps: 10000,
    concurrency: 100,
    duration: 300,
  },
  {
    name: 'balanced',
    description: 'Balanced workload with active development (70% reads, 30% writes)',
    readWriteRatio: 0.7,
    queries: benchmarkQueries,
    targetOps: 5000,
    concurrency: 50,
    duration: 300,
  },
  {
    name: 'task_board',
    description: 'Kanban/sprint board focused workload',
    readWriteRatio: 0.8,
    queries: benchmarkQueries.filter(q =>
      ['list_project_tasks', 'get_task_by_key', 'update_task_status', 'assign_task', 'project_task_summary'].includes(q.name)
    ),
    targetOps: 8000,
    concurrency: 80,
    duration: 300,
  },
  {
    name: 'collaboration',
    description: 'Heavy collaboration workload (comments, time logging)',
    readWriteRatio: 0.6,
    queries: benchmarkQueries.filter(q =>
      ['get_task_with_comments', 'add_comment', 'log_time', 'user_time_summary', 'list_recent_activity'].includes(q.name)
    ),
    targetOps: 3000,
    concurrency: 30,
    duration: 300,
  },
  {
    name: 'multi_tenant_isolation',
    description: 'Test tenant isolation with cross-tenant queries',
    readWriteRatio: 0.95,
    queries: benchmarkQueries.filter(q =>
      ['get_user_by_id_tenant', 'list_tenant_users', 'list_user_assigned_tasks', 'get_tenant_usage_metrics'].includes(q.name)
    ),
    targetOps: 15000,
    concurrency: 150,
    duration: 300,
  },
  {
    name: 'admin_analytics',
    description: 'Platform admin analytics workload',
    readWriteRatio: 1.0,
    queries: benchmarkQueries.filter(q => q.category === 'aggregate'),
    targetOps: 200,
    concurrency: 20,
    duration: 300,
  },
]

// ============================================================================
// Dataset Configuration
// ============================================================================

export const saasMultiTenantDataset: DatasetConfig = {
  name: 'saas-multi-tenant',
  description: 'Multi-tenant SaaS dataset with organizations, users, projects, and tasks',
  version: '1.0.0',
  tables,
  relationships,
  sizeTiers,
  workloads,
  metadata: {
    domain: 'project-management',
    characteristics: [
      'Strong tenant isolation requirements',
      'Role-based access control patterns',
      'Hierarchical task structure (epic/story/task/subtask)',
      'Time tracking and billing support',
      'Comprehensive audit logging',
    ],
    tenantDistribution: {
      small: 0.6,   // < 10 users
      medium: 0.3,  // 10-100 users
      large: 0.08,  // 100-1000 users
      enterprise: 0.02, // 1000+ users
    },
  },
}

// Register the dataset
registerDataset(saasMultiTenantDataset)
