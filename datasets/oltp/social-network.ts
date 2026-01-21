/**
 * Social Network OLTP Dataset
 *
 * A comprehensive social network dataset with users, posts, comments, follows, and likes.
 * Represents a typical social media workload with:
 * - Graph traversal patterns (friends-of-friends, mutual connections)
 * - High-volume feed generation
 * - Notification fanout patterns
 * - Engagement metrics and trending content
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

const usersTable: TableConfig = {
  name: 'users',
  columns: [
    { name: 'id', type: 'uuid', primaryKey: true, generator: { type: 'uuid' } },
    { name: 'username', type: 'string', maxLength: 50, unique: true, indexed: true, generator: { type: 'faker', fakerMethod: 'internet.userName' } },
    { name: 'email', type: 'string', maxLength: 255, unique: true, indexed: true, generator: { type: 'faker', fakerMethod: 'internet.email' } },
    { name: 'password_hash', type: 'string', maxLength: 255, generator: { type: 'random-string', length: 64 } },
    { name: 'display_name', type: 'string', maxLength: 100, generator: { type: 'faker', fakerMethod: 'person.fullName' } },
    { name: 'bio', type: 'text', nullable: true, generator: { type: 'faker', fakerMethod: 'lorem.paragraph' } },
    { name: 'avatar_url', type: 'string', maxLength: 500, nullable: true, generator: { type: 'faker', fakerMethod: 'image.avatar' } },
    { name: 'cover_url', type: 'string', maxLength: 500, nullable: true, generator: { type: 'faker', fakerMethod: 'image.url' } },
    { name: 'location', type: 'string', maxLength: 100, nullable: true, generator: { type: 'faker', fakerMethod: 'location.city' } },
    { name: 'website', type: 'string', maxLength: 255, nullable: true, generator: { type: 'faker', fakerMethod: 'internet.url' } },
    { name: 'birth_date', type: 'date', nullable: true, generator: { type: 'date-range', start: '1960-01-01', end: '2005-12-31' } },
    { name: 'is_verified', type: 'boolean', default: false, indexed: true, generator: { type: 'weighted-enum', values: [true, false], weights: [0.05, 0.95] } },
    { name: 'is_private', type: 'boolean', default: false, indexed: true, generator: { type: 'weighted-enum', values: [true, false], weights: [0.2, 0.8] } },
    { name: 'status', type: 'string', maxLength: 20, indexed: true, generator: { type: 'weighted-enum', values: ['active', 'suspended', 'deactivated'], weights: [0.95, 0.03, 0.02] } },
    { name: 'follower_count', type: 'integer', default: 0, indexed: true, generator: { type: 'random-int', min: 0, max: 100000 } },
    { name: 'following_count', type: 'integer', default: 0, indexed: true, generator: { type: 'random-int', min: 0, max: 5000 } },
    { name: 'post_count', type: 'integer', default: 0, generator: { type: 'random-int', min: 0, max: 10000 } },
    { name: 'settings', type: 'json', nullable: true },
    { name: 'created_at', type: 'timestamp', indexed: true, generator: { type: 'timestamp-range', start: '2015-01-01', end: '2024-12-31' } },
    { name: 'updated_at', type: 'timestamp', generator: { type: 'timestamp-range', start: '2023-01-01', end: '2024-12-31' } },
    { name: 'last_active_at', type: 'timestamp', nullable: true, indexed: true, generator: { type: 'timestamp-range', start: '2024-01-01', end: '2024-12-31' } },
  ],
  indexes: [
    { name: 'idx_users_username', columns: ['username'] },
    { name: 'idx_users_email', columns: ['email'] },
    { name: 'idx_users_verified', columns: ['is_verified'] },
    { name: 'idx_users_status', columns: ['status'] },
    { name: 'idx_users_follower_count', columns: ['follower_count'] },
    { name: 'idx_users_created', columns: ['created_at'] },
    { name: 'idx_users_active', columns: ['last_active_at'] },
  ],
}

const followsTable: TableConfig = {
  name: 'follows',
  columns: [
    { name: 'id', type: 'uuid', primaryKey: true, generator: { type: 'uuid' } },
    { name: 'follower_id', type: 'uuid', indexed: true, references: { table: 'users', column: 'id' }, generator: { type: 'reference', referenceTable: 'users', referenceColumn: 'id' } },
    { name: 'following_id', type: 'uuid', indexed: true, references: { table: 'users', column: 'id' }, generator: { type: 'reference', referenceTable: 'users', referenceColumn: 'id', distribution: 'zipf' } },
    { name: 'status', type: 'string', maxLength: 20, indexed: true, generator: { type: 'weighted-enum', values: ['accepted', 'pending', 'blocked'], weights: [0.9, 0.08, 0.02] } },
    { name: 'notifications_enabled', type: 'boolean', default: true, generator: { type: 'weighted-enum', values: [true, false], weights: [0.7, 0.3] } },
    { name: 'created_at', type: 'timestamp', indexed: true, generator: { type: 'timestamp-range', start: '2015-01-01', end: '2024-12-31' } },
  ],
  indexes: [
    { name: 'idx_follows_follower', columns: ['follower_id'] },
    { name: 'idx_follows_following', columns: ['following_id'] },
    { name: 'idx_follows_pair', columns: ['follower_id', 'following_id'], unique: true },
    { name: 'idx_follows_status', columns: ['following_id', 'status'] },
    { name: 'idx_follows_created', columns: ['follower_id', 'created_at'] },
  ],
}

const postsTable: TableConfig = {
  name: 'posts',
  columns: [
    { name: 'id', type: 'uuid', primaryKey: true, generator: { type: 'uuid' } },
    { name: 'user_id', type: 'uuid', indexed: true, references: { table: 'users', column: 'id' }, generator: { type: 'reference', referenceTable: 'users', referenceColumn: 'id', distribution: 'zipf' } },
    { name: 'content', type: 'text', generator: { type: 'faker', fakerMethod: 'lorem.paragraphs' } },
    { name: 'type', type: 'string', maxLength: 20, indexed: true, generator: { type: 'weighted-enum', values: ['text', 'image', 'video', 'link', 'poll'], weights: [0.4, 0.35, 0.1, 0.1, 0.05] } },
    { name: 'visibility', type: 'string', maxLength: 20, indexed: true, generator: { type: 'weighted-enum', values: ['public', 'followers', 'private', 'mentioned'], weights: [0.6, 0.3, 0.08, 0.02] } },
    { name: 'reply_to_id', type: 'uuid', nullable: true, indexed: true, references: { table: 'posts', column: 'id' }, generator: { type: 'reference', referenceTable: 'posts', referenceColumn: 'id', distribution: 'zipf' } },
    { name: 'repost_of_id', type: 'uuid', nullable: true, indexed: true, references: { table: 'posts', column: 'id' }, generator: { type: 'reference', referenceTable: 'posts', referenceColumn: 'id', distribution: 'zipf' } },
    { name: 'quote_of_id', type: 'uuid', nullable: true, indexed: true, references: { table: 'posts', column: 'id' }, generator: { type: 'reference', referenceTable: 'posts', referenceColumn: 'id', distribution: 'zipf' } },
    { name: 'thread_id', type: 'uuid', nullable: true, indexed: true, generator: { type: 'uuid' } },
    { name: 'like_count', type: 'integer', default: 0, indexed: true, generator: { type: 'random-int', min: 0, max: 50000 } },
    { name: 'reply_count', type: 'integer', default: 0, indexed: true, generator: { type: 'random-int', min: 0, max: 5000 } },
    { name: 'repost_count', type: 'integer', default: 0, indexed: true, generator: { type: 'random-int', min: 0, max: 10000 } },
    { name: 'quote_count', type: 'integer', default: 0, generator: { type: 'random-int', min: 0, max: 1000 } },
    { name: 'view_count', type: 'integer', default: 0, indexed: true, generator: { type: 'random-int', min: 0, max: 1000000 } },
    { name: 'bookmark_count', type: 'integer', default: 0, generator: { type: 'random-int', min: 0, max: 5000 } },
    { name: 'is_pinned', type: 'boolean', default: false, generator: { type: 'weighted-enum', values: [true, false], weights: [0.02, 0.98] } },
    { name: 'is_sensitive', type: 'boolean', default: false, generator: { type: 'weighted-enum', values: [true, false], weights: [0.05, 0.95] } },
    { name: 'language', type: 'string', maxLength: 5, nullable: true, generator: { type: 'enum', values: ['en', 'es', 'fr', 'de', 'ja', 'pt', 'zh'] } },
    { name: 'location', type: 'string', maxLength: 100, nullable: true, generator: { type: 'faker', fakerMethod: 'location.city' } },
    { name: 'metadata', type: 'json', nullable: true },
    { name: 'created_at', type: 'timestamp', indexed: true, generator: { type: 'timestamp-range', start: '2020-01-01', end: '2024-12-31' } },
    { name: 'updated_at', type: 'timestamp', nullable: true, generator: { type: 'timestamp-range', start: '2023-01-01', end: '2024-12-31' } },
    { name: 'deleted_at', type: 'timestamp', nullable: true, indexed: true },
  ],
  indexes: [
    { name: 'idx_posts_user', columns: ['user_id'] },
    { name: 'idx_posts_user_created', columns: ['user_id', 'created_at'] },
    { name: 'idx_posts_reply_to', columns: ['reply_to_id'] },
    { name: 'idx_posts_repost_of', columns: ['repost_of_id'] },
    { name: 'idx_posts_thread', columns: ['thread_id'] },
    { name: 'idx_posts_visibility', columns: ['visibility'] },
    { name: 'idx_posts_type', columns: ['type'] },
    { name: 'idx_posts_created', columns: ['created_at'] },
    { name: 'idx_posts_engagement', columns: ['like_count', 'reply_count', 'repost_count'] },
    { name: 'idx_posts_trending', columns: ['created_at', 'like_count'], where: 'deleted_at IS NULL' },
  ],
  partitionBy: {
    type: 'range',
    column: 'created_at',
  },
}

const mediaTable: TableConfig = {
  name: 'media',
  columns: [
    { name: 'id', type: 'uuid', primaryKey: true, generator: { type: 'uuid' } },
    { name: 'post_id', type: 'uuid', indexed: true, references: { table: 'posts', column: 'id' }, generator: { type: 'reference', referenceTable: 'posts', referenceColumn: 'id' } },
    { name: 'user_id', type: 'uuid', indexed: true, references: { table: 'users', column: 'id' }, generator: { type: 'reference', referenceTable: 'users', referenceColumn: 'id' } },
    { name: 'type', type: 'string', maxLength: 20, indexed: true, generator: { type: 'weighted-enum', values: ['image', 'video', 'gif', 'audio'], weights: [0.7, 0.15, 0.1, 0.05] } },
    { name: 'url', type: 'string', maxLength: 500, generator: { type: 'faker', fakerMethod: 'image.url' } },
    { name: 'thumbnail_url', type: 'string', maxLength: 500, nullable: true, generator: { type: 'faker', fakerMethod: 'image.url' } },
    { name: 'width', type: 'integer', nullable: true, generator: { type: 'random-int', min: 100, max: 4096 } },
    { name: 'height', type: 'integer', nullable: true, generator: { type: 'random-int', min: 100, max: 4096 } },
    { name: 'duration_seconds', type: 'integer', nullable: true, generator: { type: 'random-int', min: 1, max: 600 } },
    { name: 'size_bytes', type: 'bigint', generator: { type: 'random-int', min: 10000, max: 100000000 } },
    { name: 'alt_text', type: 'string', maxLength: 500, nullable: true, generator: { type: 'faker', fakerMethod: 'lorem.sentence' } },
    { name: 'blurhash', type: 'string', maxLength: 100, nullable: true, generator: { type: 'random-string', length: 50 } },
    { name: 'is_sensitive', type: 'boolean', default: false, generator: { type: 'weighted-enum', values: [true, false], weights: [0.05, 0.95] } },
    { name: 'sort_order', type: 'integer', default: 0, generator: { type: 'sequence' } },
    { name: 'created_at', type: 'timestamp', generator: { type: 'timestamp-range', start: '2020-01-01', end: '2024-12-31' } },
  ],
  indexes: [
    { name: 'idx_media_post', columns: ['post_id', 'sort_order'] },
    { name: 'idx_media_user', columns: ['user_id'] },
    { name: 'idx_media_type', columns: ['type'] },
  ],
  embedded: ['url', 'thumbnail_url', 'width', 'height', 'alt_text'],
}

const likesTable: TableConfig = {
  name: 'likes',
  columns: [
    { name: 'id', type: 'uuid', primaryKey: true, generator: { type: 'uuid' } },
    { name: 'user_id', type: 'uuid', indexed: true, references: { table: 'users', column: 'id' }, generator: { type: 'reference', referenceTable: 'users', referenceColumn: 'id' } },
    { name: 'post_id', type: 'uuid', indexed: true, references: { table: 'posts', column: 'id' }, generator: { type: 'reference', referenceTable: 'posts', referenceColumn: 'id', distribution: 'zipf' } },
    { name: 'created_at', type: 'timestamp', indexed: true, generator: { type: 'timestamp-range', start: '2020-01-01', end: '2024-12-31' } },
  ],
  indexes: [
    { name: 'idx_likes_user', columns: ['user_id'] },
    { name: 'idx_likes_post', columns: ['post_id'] },
    { name: 'idx_likes_user_post', columns: ['user_id', 'post_id'], unique: true },
    { name: 'idx_likes_user_created', columns: ['user_id', 'created_at'] },
    { name: 'idx_likes_post_created', columns: ['post_id', 'created_at'] },
  ],
}

const bookmarksTable: TableConfig = {
  name: 'bookmarks',
  columns: [
    { name: 'id', type: 'uuid', primaryKey: true, generator: { type: 'uuid' } },
    { name: 'user_id', type: 'uuid', indexed: true, references: { table: 'users', column: 'id' }, generator: { type: 'reference', referenceTable: 'users', referenceColumn: 'id' } },
    { name: 'post_id', type: 'uuid', indexed: true, references: { table: 'posts', column: 'id' }, generator: { type: 'reference', referenceTable: 'posts', referenceColumn: 'id', distribution: 'zipf' } },
    { name: 'folder', type: 'string', maxLength: 100, nullable: true, indexed: true, generator: { type: 'enum', values: ['read-later', 'favorites', 'inspiration', null] } },
    { name: 'created_at', type: 'timestamp', indexed: true, generator: { type: 'timestamp-range', start: '2020-01-01', end: '2024-12-31' } },
  ],
  indexes: [
    { name: 'idx_bookmarks_user', columns: ['user_id'] },
    { name: 'idx_bookmarks_post', columns: ['post_id'] },
    { name: 'idx_bookmarks_user_post', columns: ['user_id', 'post_id'], unique: true },
    { name: 'idx_bookmarks_user_folder', columns: ['user_id', 'folder'] },
    { name: 'idx_bookmarks_user_created', columns: ['user_id', 'created_at'] },
  ],
}

const mentionsTable: TableConfig = {
  name: 'mentions',
  columns: [
    { name: 'id', type: 'uuid', primaryKey: true, generator: { type: 'uuid' } },
    { name: 'post_id', type: 'uuid', indexed: true, references: { table: 'posts', column: 'id' }, generator: { type: 'reference', referenceTable: 'posts', referenceColumn: 'id' } },
    { name: 'user_id', type: 'uuid', indexed: true, references: { table: 'users', column: 'id' }, generator: { type: 'reference', referenceTable: 'users', referenceColumn: 'id', distribution: 'zipf' } },
    { name: 'position_start', type: 'integer', generator: { type: 'random-int', min: 0, max: 500 } },
    { name: 'position_end', type: 'integer', generator: { type: 'random-int', min: 5, max: 550 } },
    { name: 'created_at', type: 'timestamp', generator: { type: 'timestamp-range', start: '2020-01-01', end: '2024-12-31' } },
  ],
  indexes: [
    { name: 'idx_mentions_post', columns: ['post_id'] },
    { name: 'idx_mentions_user', columns: ['user_id'] },
    { name: 'idx_mentions_user_created', columns: ['user_id', 'created_at'] },
  ],
}

const hashtagsTable: TableConfig = {
  name: 'hashtags',
  columns: [
    { name: 'id', type: 'uuid', primaryKey: true, generator: { type: 'uuid' } },
    { name: 'name', type: 'string', maxLength: 100, unique: true, indexed: true, generator: { type: 'faker', fakerMethod: 'word.noun' } },
    { name: 'usage_count', type: 'integer', default: 0, indexed: true, generator: { type: 'random-int', min: 0, max: 1000000 } },
    { name: 'daily_usage', type: 'integer', default: 0, indexed: true, generator: { type: 'random-int', min: 0, max: 10000 } },
    { name: 'weekly_usage', type: 'integer', default: 0, indexed: true, generator: { type: 'random-int', min: 0, max: 50000 } },
    { name: 'is_trending', type: 'boolean', default: false, indexed: true, generator: { type: 'weighted-enum', values: [true, false], weights: [0.05, 0.95] } },
    { name: 'category', type: 'string', maxLength: 50, nullable: true, indexed: true, generator: { type: 'enum', values: ['entertainment', 'news', 'sports', 'technology', 'politics', 'lifestyle', null] } },
    { name: 'created_at', type: 'timestamp', indexed: true, generator: { type: 'timestamp-range', start: '2015-01-01', end: '2024-12-31' } },
    { name: 'last_used_at', type: 'timestamp', indexed: true, generator: { type: 'timestamp-range', start: '2024-01-01', end: '2024-12-31' } },
  ],
  indexes: [
    { name: 'idx_hashtags_name', columns: ['name'] },
    { name: 'idx_hashtags_usage', columns: ['usage_count'] },
    { name: 'idx_hashtags_trending', columns: ['is_trending', 'daily_usage'] },
    { name: 'idx_hashtags_category', columns: ['category'] },
  ],
}

const postHashtagsTable: TableConfig = {
  name: 'post_hashtags',
  columns: [
    { name: 'post_id', type: 'uuid', indexed: true, references: { table: 'posts', column: 'id' }, generator: { type: 'reference', referenceTable: 'posts', referenceColumn: 'id' } },
    { name: 'hashtag_id', type: 'uuid', indexed: true, references: { table: 'hashtags', column: 'id' }, generator: { type: 'reference', referenceTable: 'hashtags', referenceColumn: 'id', distribution: 'zipf' } },
    { name: 'created_at', type: 'timestamp', generator: { type: 'timestamp-range', start: '2020-01-01', end: '2024-12-31' } },
  ],
  indexes: [
    { name: 'idx_post_hashtags_post', columns: ['post_id'] },
    { name: 'idx_post_hashtags_hashtag', columns: ['hashtag_id'] },
    { name: 'idx_post_hashtags_pk', columns: ['post_id', 'hashtag_id'], unique: true },
    { name: 'idx_post_hashtags_hashtag_created', columns: ['hashtag_id', 'created_at'] },
  ],
}

const notificationsTable: TableConfig = {
  name: 'notifications',
  columns: [
    { name: 'id', type: 'uuid', primaryKey: true, generator: { type: 'uuid' } },
    { name: 'user_id', type: 'uuid', indexed: true, references: { table: 'users', column: 'id' }, generator: { type: 'reference', referenceTable: 'users', referenceColumn: 'id' } },
    { name: 'actor_id', type: 'uuid', indexed: true, references: { table: 'users', column: 'id' }, generator: { type: 'reference', referenceTable: 'users', referenceColumn: 'id' } },
    { name: 'type', type: 'string', maxLength: 30, indexed: true, generator: { type: 'weighted-enum', values: ['like', 'reply', 'mention', 'follow', 'repost', 'quote'], weights: [0.35, 0.25, 0.15, 0.1, 0.1, 0.05] } },
    { name: 'post_id', type: 'uuid', nullable: true, indexed: true, references: { table: 'posts', column: 'id' }, generator: { type: 'reference', referenceTable: 'posts', referenceColumn: 'id' } },
    { name: 'is_read', type: 'boolean', default: false, indexed: true, generator: { type: 'weighted-enum', values: [true, false], weights: [0.6, 0.4] } },
    { name: 'is_seen', type: 'boolean', default: false, indexed: true, generator: { type: 'weighted-enum', values: [true, false], weights: [0.7, 0.3] } },
    { name: 'group_key', type: 'string', maxLength: 100, nullable: true, indexed: true, generator: { type: 'uuid' } },
    { name: 'data', type: 'json', nullable: true },
    { name: 'created_at', type: 'timestamp', indexed: true, generator: { type: 'timestamp-range', start: '2024-01-01', end: '2024-12-31' } },
  ],
  indexes: [
    { name: 'idx_notifications_user', columns: ['user_id'] },
    { name: 'idx_notifications_user_created', columns: ['user_id', 'created_at'] },
    { name: 'idx_notifications_user_unread', columns: ['user_id', 'is_read'], where: 'is_read = false' },
    { name: 'idx_notifications_user_unseen', columns: ['user_id', 'is_seen'], where: 'is_seen = false' },
    { name: 'idx_notifications_type', columns: ['type'] },
    { name: 'idx_notifications_group', columns: ['group_key'] },
    { name: 'idx_notifications_post', columns: ['post_id'] },
  ],
  partitionBy: {
    type: 'range',
    column: 'created_at',
  },
}

const directMessagesTable: TableConfig = {
  name: 'direct_messages',
  columns: [
    { name: 'id', type: 'uuid', primaryKey: true, generator: { type: 'uuid' } },
    { name: 'conversation_id', type: 'uuid', indexed: true, generator: { type: 'uuid' } },
    { name: 'sender_id', type: 'uuid', indexed: true, references: { table: 'users', column: 'id' }, generator: { type: 'reference', referenceTable: 'users', referenceColumn: 'id' } },
    { name: 'content', type: 'text', generator: { type: 'faker', fakerMethod: 'lorem.paragraphs' } },
    { name: 'type', type: 'string', maxLength: 20, generator: { type: 'weighted-enum', values: ['text', 'image', 'video', 'post_share', 'profile_share'], weights: [0.8, 0.1, 0.03, 0.05, 0.02] } },
    { name: 'shared_post_id', type: 'uuid', nullable: true, references: { table: 'posts', column: 'id' }, generator: { type: 'reference', referenceTable: 'posts', referenceColumn: 'id' } },
    { name: 'is_read', type: 'boolean', default: false, indexed: true, generator: { type: 'weighted-enum', values: [true, false], weights: [0.7, 0.3] } },
    { name: 'created_at', type: 'timestamp', indexed: true, generator: { type: 'timestamp-range', start: '2023-01-01', end: '2024-12-31' } },
    { name: 'edited_at', type: 'timestamp', nullable: true },
    { name: 'deleted_at', type: 'timestamp', nullable: true },
  ],
  indexes: [
    { name: 'idx_dm_conversation', columns: ['conversation_id'] },
    { name: 'idx_dm_conversation_created', columns: ['conversation_id', 'created_at'] },
    { name: 'idx_dm_sender', columns: ['sender_id'] },
  ],
  partitionBy: {
    type: 'range',
    column: 'created_at',
  },
}

const conversationsTable: TableConfig = {
  name: 'conversations',
  columns: [
    { name: 'id', type: 'uuid', primaryKey: true, generator: { type: 'uuid' } },
    { name: 'type', type: 'string', maxLength: 20, indexed: true, generator: { type: 'weighted-enum', values: ['direct', 'group'], weights: [0.9, 0.1] } },
    { name: 'name', type: 'string', maxLength: 100, nullable: true, generator: { type: 'faker', fakerMethod: 'lorem.words' } },
    { name: 'last_message_id', type: 'uuid', nullable: true, references: { table: 'direct_messages', column: 'id' }, generator: { type: 'reference', referenceTable: 'direct_messages', referenceColumn: 'id' } },
    { name: 'last_message_at', type: 'timestamp', indexed: true, generator: { type: 'timestamp-range', start: '2024-01-01', end: '2024-12-31' } },
    { name: 'message_count', type: 'integer', default: 0, generator: { type: 'random-int', min: 1, max: 10000 } },
    { name: 'created_at', type: 'timestamp', generator: { type: 'timestamp-range', start: '2020-01-01', end: '2024-12-31' } },
    { name: 'updated_at', type: 'timestamp', generator: { type: 'timestamp-range', start: '2024-01-01', end: '2024-12-31' } },
  ],
  indexes: [
    { name: 'idx_conversations_type', columns: ['type'] },
    { name: 'idx_conversations_last_message', columns: ['last_message_at'] },
  ],
}

const conversationParticipantsTable: TableConfig = {
  name: 'conversation_participants',
  columns: [
    { name: 'id', type: 'uuid', primaryKey: true, generator: { type: 'uuid' } },
    { name: 'conversation_id', type: 'uuid', indexed: true, references: { table: 'conversations', column: 'id' }, generator: { type: 'reference', referenceTable: 'conversations', referenceColumn: 'id' } },
    { name: 'user_id', type: 'uuid', indexed: true, references: { table: 'users', column: 'id' }, generator: { type: 'reference', referenceTable: 'users', referenceColumn: 'id' } },
    { name: 'role', type: 'string', maxLength: 20, default: 'member', generator: { type: 'weighted-enum', values: ['admin', 'member'], weights: [0.1, 0.9] } },
    { name: 'last_read_at', type: 'timestamp', nullable: true, indexed: true, generator: { type: 'timestamp-range', start: '2024-01-01', end: '2024-12-31' } },
    { name: 'is_muted', type: 'boolean', default: false, generator: { type: 'weighted-enum', values: [true, false], weights: [0.1, 0.9] } },
    { name: 'joined_at', type: 'timestamp', generator: { type: 'timestamp-range', start: '2020-01-01', end: '2024-12-31' } },
    { name: 'left_at', type: 'timestamp', nullable: true },
  ],
  indexes: [
    { name: 'idx_conv_participants_conv', columns: ['conversation_id'] },
    { name: 'idx_conv_participants_user', columns: ['user_id'] },
    { name: 'idx_conv_participants_conv_user', columns: ['conversation_id', 'user_id'], unique: true },
    { name: 'idx_conv_participants_user_active', columns: ['user_id', 'left_at'], where: 'left_at IS NULL' },
  ],
}

const blocksTable: TableConfig = {
  name: 'blocks',
  columns: [
    { name: 'id', type: 'uuid', primaryKey: true, generator: { type: 'uuid' } },
    { name: 'blocker_id', type: 'uuid', indexed: true, references: { table: 'users', column: 'id' }, generator: { type: 'reference', referenceTable: 'users', referenceColumn: 'id' } },
    { name: 'blocked_id', type: 'uuid', indexed: true, references: { table: 'users', column: 'id' }, generator: { type: 'reference', referenceTable: 'users', referenceColumn: 'id' } },
    { name: 'created_at', type: 'timestamp', generator: { type: 'timestamp-range', start: '2020-01-01', end: '2024-12-31' } },
  ],
  indexes: [
    { name: 'idx_blocks_blocker', columns: ['blocker_id'] },
    { name: 'idx_blocks_blocked', columns: ['blocked_id'] },
    { name: 'idx_blocks_pair', columns: ['blocker_id', 'blocked_id'], unique: true },
  ],
}

const reportsTable: TableConfig = {
  name: 'reports',
  columns: [
    { name: 'id', type: 'uuid', primaryKey: true, generator: { type: 'uuid' } },
    { name: 'reporter_id', type: 'uuid', indexed: true, references: { table: 'users', column: 'id' }, generator: { type: 'reference', referenceTable: 'users', referenceColumn: 'id' } },
    { name: 'reported_user_id', type: 'uuid', nullable: true, indexed: true, references: { table: 'users', column: 'id' }, generator: { type: 'reference', referenceTable: 'users', referenceColumn: 'id' } },
    { name: 'reported_post_id', type: 'uuid', nullable: true, indexed: true, references: { table: 'posts', column: 'id' }, generator: { type: 'reference', referenceTable: 'posts', referenceColumn: 'id' } },
    { name: 'reason', type: 'string', maxLength: 50, indexed: true, generator: { type: 'enum', values: ['spam', 'harassment', 'hate_speech', 'misinformation', 'violence', 'nudity', 'copyright', 'other'] } },
    { name: 'details', type: 'text', nullable: true, generator: { type: 'faker', fakerMethod: 'lorem.paragraph' } },
    { name: 'status', type: 'string', maxLength: 20, indexed: true, generator: { type: 'weighted-enum', values: ['pending', 'reviewing', 'resolved', 'dismissed'], weights: [0.3, 0.2, 0.3, 0.2] } },
    { name: 'resolution', type: 'text', nullable: true, generator: { type: 'faker', fakerMethod: 'lorem.sentence' } },
    { name: 'resolved_by', type: 'uuid', nullable: true, references: { table: 'users', column: 'id' }, generator: { type: 'reference', referenceTable: 'users', referenceColumn: 'id' } },
    { name: 'created_at', type: 'timestamp', indexed: true, generator: { type: 'timestamp-range', start: '2023-01-01', end: '2024-12-31' } },
    { name: 'resolved_at', type: 'timestamp', nullable: true, generator: { type: 'timestamp-range', start: '2024-01-01', end: '2024-12-31' } },
  ],
  indexes: [
    { name: 'idx_reports_reporter', columns: ['reporter_id'] },
    { name: 'idx_reports_user', columns: ['reported_user_id'] },
    { name: 'idx_reports_post', columns: ['reported_post_id'] },
    { name: 'idx_reports_status', columns: ['status'] },
    { name: 'idx_reports_reason', columns: ['reason'] },
    { name: 'idx_reports_pending', columns: ['created_at'], where: "status = 'pending'" },
  ],
}

// ============================================================================
// Tables Array
// ============================================================================

const tables: TableConfig[] = [
  usersTable,
  followsTable,
  postsTable,
  mediaTable,
  likesTable,
  bookmarksTable,
  mentionsTable,
  hashtagsTable,
  postHashtagsTable,
  notificationsTable,
  directMessagesTable,
  conversationsTable,
  conversationParticipantsTable,
  blocksTable,
  reportsTable,
]

// ============================================================================
// Relationships
// ============================================================================

const relationships: RelationshipConfig[] = [
  {
    name: 'user_followers',
    type: 'many-to-many',
    from: { table: 'users', column: 'id' },
    to: { table: 'users', column: 'id' },
    through: {
      table: 'follows',
      fromColumn: 'following_id',
      toColumn: 'follower_id',
    },
    onDelete: 'cascade',
  },
  {
    name: 'user_following',
    type: 'many-to-many',
    from: { table: 'users', column: 'id' },
    to: { table: 'users', column: 'id' },
    through: {
      table: 'follows',
      fromColumn: 'follower_id',
      toColumn: 'following_id',
    },
    onDelete: 'cascade',
  },
  {
    name: 'user_posts',
    type: 'one-to-many',
    from: { table: 'users', column: 'id' },
    to: { table: 'posts', column: 'user_id' },
    onDelete: 'cascade',
  },
  {
    name: 'post_replies',
    type: 'one-to-many',
    from: { table: 'posts', column: 'id' },
    to: { table: 'posts', column: 'reply_to_id' },
    onDelete: 'set-null',
  },
  {
    name: 'post_reposts',
    type: 'one-to-many',
    from: { table: 'posts', column: 'id' },
    to: { table: 'posts', column: 'repost_of_id' },
    onDelete: 'set-null',
  },
  {
    name: 'post_media',
    type: 'one-to-many',
    from: { table: 'posts', column: 'id' },
    to: { table: 'media', column: 'post_id' },
    onDelete: 'cascade',
    embed: true,
  },
  {
    name: 'post_likes',
    type: 'many-to-many',
    from: { table: 'posts', column: 'id' },
    to: { table: 'users', column: 'id' },
    through: {
      table: 'likes',
      fromColumn: 'post_id',
      toColumn: 'user_id',
    },
    onDelete: 'cascade',
  },
  {
    name: 'post_bookmarks',
    type: 'many-to-many',
    from: { table: 'posts', column: 'id' },
    to: { table: 'users', column: 'id' },
    through: {
      table: 'bookmarks',
      fromColumn: 'post_id',
      toColumn: 'user_id',
    },
    onDelete: 'cascade',
  },
  {
    name: 'post_mentions',
    type: 'one-to-many',
    from: { table: 'posts', column: 'id' },
    to: { table: 'mentions', column: 'post_id' },
    onDelete: 'cascade',
  },
  {
    name: 'post_hashtags',
    type: 'many-to-many',
    from: { table: 'posts', column: 'id' },
    to: { table: 'hashtags', column: 'id' },
    through: {
      table: 'post_hashtags',
      fromColumn: 'post_id',
      toColumn: 'hashtag_id',
    },
    onDelete: 'cascade',
  },
  {
    name: 'user_notifications',
    type: 'one-to-many',
    from: { table: 'users', column: 'id' },
    to: { table: 'notifications', column: 'user_id' },
    onDelete: 'cascade',
  },
  {
    name: 'conversation_messages',
    type: 'one-to-many',
    from: { table: 'conversations', column: 'id' },
    to: { table: 'direct_messages', column: 'conversation_id' },
    onDelete: 'cascade',
  },
  {
    name: 'conversation_participants',
    type: 'many-to-many',
    from: { table: 'conversations', column: 'id' },
    to: { table: 'users', column: 'id' },
    through: {
      table: 'conversation_participants',
      fromColumn: 'conversation_id',
      toColumn: 'user_id',
    },
    onDelete: 'cascade',
  },
  {
    name: 'user_blocks',
    type: 'many-to-many',
    from: { table: 'users', column: 'id' },
    to: { table: 'users', column: 'id' },
    through: {
      table: 'blocks',
      fromColumn: 'blocker_id',
      toColumn: 'blocked_id',
    },
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
      users: 500,
      follows: 5000,
      posts: 2000,
      media: 3000,
      likes: 15000,
      bookmarks: 2000,
      mentions: 1500,
      hashtags: 200,
      post_hashtags: 4000,
      notifications: 10000,
      direct_messages: 5000,
      conversations: 500,
      conversation_participants: 1000,
      blocks: 100,
      reports: 50,
    },
    estimatedBytes: 1_048_576,
    recommendedMemoryMB: 128,
  },
  {
    size: '10mb',
    seedCount: {
      users: 5000,
      follows: 50000,
      posts: 20000,
      media: 30000,
      likes: 150000,
      bookmarks: 20000,
      mentions: 15000,
      hashtags: 1000,
      post_hashtags: 40000,
      notifications: 100000,
      direct_messages: 50000,
      conversations: 5000,
      conversation_participants: 10000,
      blocks: 1000,
      reports: 500,
    },
    estimatedBytes: 10_485_760,
    recommendedMemoryMB: 256,
  },
  {
    size: '100mb',
    seedCount: {
      users: 50000,
      follows: 500000,
      posts: 200000,
      media: 300000,
      likes: 1500000,
      bookmarks: 200000,
      mentions: 150000,
      hashtags: 5000,
      post_hashtags: 400000,
      notifications: 1000000,
      direct_messages: 500000,
      conversations: 50000,
      conversation_participants: 100000,
      blocks: 10000,
      reports: 5000,
    },
    estimatedBytes: 104_857_600,
    recommendedMemoryMB: 512,
  },
  {
    size: '1gb',
    seedCount: {
      users: 500000,
      follows: 5000000,
      posts: 2000000,
      media: 3000000,
      likes: 15000000,
      bookmarks: 2000000,
      mentions: 1500000,
      hashtags: 25000,
      post_hashtags: 4000000,
      notifications: 10000000,
      direct_messages: 5000000,
      conversations: 500000,
      conversation_participants: 1000000,
      blocks: 100000,
      reports: 50000,
    },
    estimatedBytes: 1_073_741_824,
    recommendedMemoryMB: 2048,
    recommendedCores: 2,
  },
  {
    size: '10gb',
    seedCount: {
      users: 5000000,
      follows: 50000000,
      posts: 20000000,
      media: 30000000,
      likes: 150000000,
      bookmarks: 20000000,
      mentions: 15000000,
      hashtags: 100000,
      post_hashtags: 40000000,
      notifications: 100000000,
      direct_messages: 50000000,
      conversations: 5000000,
      conversation_participants: 10000000,
      blocks: 1000000,
      reports: 500000,
    },
    estimatedBytes: 10_737_418_240,
    recommendedMemoryMB: 8192,
    recommendedCores: 4,
  },
  {
    size: '20gb',
    seedCount: {
      users: 10000000,
      follows: 100000000,
      posts: 40000000,
      media: 60000000,
      likes: 300000000,
      bookmarks: 40000000,
      mentions: 30000000,
      hashtags: 150000,
      post_hashtags: 80000000,
      notifications: 200000000,
      direct_messages: 100000000,
      conversations: 10000000,
      conversation_participants: 20000000,
      blocks: 2000000,
      reports: 1000000,
    },
    estimatedBytes: 21_474_836_480,
    recommendedMemoryMB: 16384,
    recommendedCores: 8,
  },
  {
    size: '30gb',
    seedCount: {
      users: 15000000,
      follows: 150000000,
      posts: 60000000,
      media: 90000000,
      likes: 450000000,
      bookmarks: 60000000,
      mentions: 45000000,
      hashtags: 200000,
      post_hashtags: 120000000,
      notifications: 300000000,
      direct_messages: 150000000,
      conversations: 15000000,
      conversation_participants: 30000000,
      blocks: 3000000,
      reports: 1500000,
    },
    estimatedBytes: 32_212_254_720,
    recommendedMemoryMB: 24576,
    recommendedCores: 12,
  },
  {
    size: '50gb',
    seedCount: {
      users: 25000000,
      follows: 250000000,
      posts: 100000000,
      media: 150000000,
      likes: 750000000,
      bookmarks: 100000000,
      mentions: 75000000,
      hashtags: 300000,
      post_hashtags: 200000000,
      notifications: 500000000,
      direct_messages: 250000000,
      conversations: 25000000,
      conversation_participants: 50000000,
      blocks: 5000000,
      reports: 2500000,
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
    name: 'get_user_by_id',
    description: 'Fetch user profile by ID',
    category: 'point-lookup',
    sql: 'SELECT * FROM users WHERE id = $1',
    documentQuery: {
      collection: 'users',
      operation: 'find',
      filter: { _id: '$1' },
    },
    parameters: [{ type: 'reference', referenceTable: 'users', referenceColumn: 'id' }],
    expectedComplexity: 'O(1)',
    weight: 15,
  },
  {
    name: 'get_user_by_username',
    description: 'Fetch user by username',
    category: 'point-lookup',
    sql: 'SELECT * FROM users WHERE username = $1',
    documentQuery: {
      collection: 'users',
      operation: 'find',
      filter: { username: '$1' },
    },
    parameters: [{ type: 'faker', fakerMethod: 'internet.userName' }],
    expectedComplexity: 'O(1)',
    weight: 10,
  },
  {
    name: 'get_post_by_id',
    description: 'Fetch single post by ID',
    category: 'point-lookup',
    sql: 'SELECT * FROM posts WHERE id = $1 AND deleted_at IS NULL',
    documentQuery: {
      collection: 'posts',
      operation: 'find',
      filter: { _id: '$1', deleted_at: null },
    },
    parameters: [{ type: 'reference', referenceTable: 'posts', referenceColumn: 'id' }],
    expectedComplexity: 'O(1)',
    weight: 12,
  },
  {
    name: 'check_follow_status',
    description: 'Check if user A follows user B',
    category: 'point-lookup',
    sql: 'SELECT * FROM follows WHERE follower_id = $1 AND following_id = $2',
    documentQuery: {
      collection: 'follows',
      operation: 'find',
      filter: { follower_id: '$1', following_id: '$2' },
    },
    parameters: [
      { type: 'reference', referenceTable: 'users', referenceColumn: 'id' },
      { type: 'reference', referenceTable: 'users', referenceColumn: 'id' },
    ],
    expectedComplexity: 'O(1)',
    weight: 8,
  },
  {
    name: 'check_like_status',
    description: 'Check if user liked a post',
    category: 'point-lookup',
    sql: 'SELECT * FROM likes WHERE user_id = $1 AND post_id = $2',
    documentQuery: {
      collection: 'likes',
      operation: 'find',
      filter: { user_id: '$1', post_id: '$2' },
    },
    parameters: [
      { type: 'reference', referenceTable: 'users', referenceColumn: 'id' },
      { type: 'reference', referenceTable: 'posts', referenceColumn: 'id' },
    ],
    expectedComplexity: 'O(1)',
    weight: 8,
  },

  // Feed generation (critical for social networks)
  {
    name: 'home_feed',
    description: 'Generate home feed from followed users',
    category: 'join',
    sql: `SELECT p.*, u.username, u.display_name, u.avatar_url, u.is_verified
          FROM posts p
          JOIN users u ON p.user_id = u.id
          WHERE p.user_id IN (
            SELECT following_id FROM follows WHERE follower_id = $1 AND status = 'accepted'
          )
          AND p.deleted_at IS NULL
          AND p.visibility IN ('public', 'followers')
          ORDER BY p.created_at DESC
          LIMIT 50`,
    documentQuery: {
      collection: 'posts',
      operation: 'aggregate',
      pipeline: [
        { $match: { user_id: { $in: [] }, deleted_at: null, visibility: { $in: ['public', 'followers'] } } },
        { $sort: { created_at: -1 } },
        { $limit: 50 },
      ],
    },
    parameters: [{ type: 'reference', referenceTable: 'users', referenceColumn: 'id' }],
    expectedComplexity: 'O(n log n)',
    weight: 15,
  },
  {
    name: 'user_profile_posts',
    description: 'Get posts for user profile page',
    category: 'range-scan',
    sql: `SELECT * FROM posts
          WHERE user_id = $1 AND deleted_at IS NULL AND reply_to_id IS NULL
          ORDER BY created_at DESC
          LIMIT 20 OFFSET $2`,
    documentQuery: {
      collection: 'posts',
      operation: 'find',
      filter: { user_id: '$1', deleted_at: null, reply_to_id: null },
    },
    parameters: [
      { type: 'reference', referenceTable: 'users', referenceColumn: 'id', distribution: 'zipf' },
      { type: 'random-int', min: 0, max: 100 },
    ],
    expectedComplexity: 'O(log n)',
    weight: 10,
  },
  {
    name: 'user_media_posts',
    description: 'Get posts with media for user profile',
    category: 'join',
    sql: `SELECT DISTINCT p.*
          FROM posts p
          JOIN media m ON p.id = m.post_id
          WHERE p.user_id = $1 AND p.deleted_at IS NULL
          ORDER BY p.created_at DESC
          LIMIT 30`,
    documentQuery: {
      collection: 'posts',
      operation: 'find',
      filter: { user_id: '$1', deleted_at: null, 'media.0': { $exists: true } },
    },
    parameters: [{ type: 'reference', referenceTable: 'users', referenceColumn: 'id', distribution: 'zipf' }],
    expectedComplexity: 'O(log n)',
    weight: 5,
  },

  // Graph traversals
  {
    name: 'get_followers',
    description: 'Get paginated list of followers',
    category: 'join',
    sql: `SELECT u.*
          FROM users u
          JOIN follows f ON u.id = f.follower_id
          WHERE f.following_id = $1 AND f.status = 'accepted'
          ORDER BY f.created_at DESC
          LIMIT 50 OFFSET $2`,
    documentQuery: {
      collection: 'follows',
      operation: 'aggregate',
      pipeline: [
        { $match: { following_id: '$1', status: 'accepted' } },
        { $lookup: { from: 'users', localField: 'follower_id', foreignField: '_id', as: 'user' } },
        { $sort: { created_at: -1 } },
        { $skip: { $toInt: '$2' } },
        { $limit: 50 },
      ],
    },
    parameters: [
      { type: 'reference', referenceTable: 'users', referenceColumn: 'id', distribution: 'zipf' },
      { type: 'random-int', min: 0, max: 200 },
    ],
    expectedComplexity: 'O(log n)',
    weight: 6,
  },
  {
    name: 'get_following',
    description: 'Get paginated list of following',
    category: 'join',
    sql: `SELECT u.*
          FROM users u
          JOIN follows f ON u.id = f.following_id
          WHERE f.follower_id = $1 AND f.status = 'accepted'
          ORDER BY f.created_at DESC
          LIMIT 50 OFFSET $2`,
    documentQuery: {
      collection: 'follows',
      operation: 'aggregate',
      pipeline: [
        { $match: { follower_id: '$1', status: 'accepted' } },
        { $lookup: { from: 'users', localField: 'following_id', foreignField: '_id', as: 'user' } },
        { $sort: { created_at: -1 } },
        { $limit: 50 },
      ],
    },
    parameters: [
      { type: 'reference', referenceTable: 'users', referenceColumn: 'id' },
      { type: 'random-int', min: 0, max: 100 },
    ],
    expectedComplexity: 'O(log n)',
    weight: 5,
  },
  {
    name: 'mutual_followers',
    description: 'Find mutual followers between two users',
    category: 'join',
    sql: `SELECT u.*
          FROM users u
          WHERE u.id IN (
            SELECT f1.follower_id
            FROM follows f1
            JOIN follows f2 ON f1.follower_id = f2.follower_id
            WHERE f1.following_id = $1 AND f2.following_id = $2
            AND f1.status = 'accepted' AND f2.status = 'accepted'
          )
          LIMIT 50`,
    documentQuery: {
      collection: 'follows',
      operation: 'aggregate',
      pipeline: [
        { $match: { following_id: '$1', status: 'accepted' } },
        { $lookup: { from: 'follows', localField: 'follower_id', foreignField: 'follower_id', as: 'mutual' } },
        { $match: { 'mutual.following_id': '$2', 'mutual.status': 'accepted' } },
      ],
    },
    parameters: [
      { type: 'reference', referenceTable: 'users', referenceColumn: 'id' },
      { type: 'reference', referenceTable: 'users', referenceColumn: 'id' },
    ],
    expectedComplexity: 'O(n)',
    weight: 3,
  },
  {
    name: 'suggested_follows',
    description: 'Get follow suggestions (friends of friends)',
    category: 'join',
    sql: `SELECT u.*, COUNT(*) as mutual_count
          FROM users u
          JOIN follows f1 ON u.id = f1.following_id
          JOIN follows f2 ON f1.follower_id = f2.following_id
          WHERE f2.follower_id = $1
          AND f1.status = 'accepted' AND f2.status = 'accepted'
          AND u.id != $1
          AND u.id NOT IN (SELECT following_id FROM follows WHERE follower_id = $1)
          GROUP BY u.id
          ORDER BY mutual_count DESC
          LIMIT 20`,
    documentQuery: {
      collection: 'follows',
      operation: 'aggregate',
      pipeline: [
        { $match: { follower_id: '$1', status: 'accepted' } },
        { $lookup: { from: 'follows', localField: 'following_id', foreignField: 'follower_id', as: 'fof' } },
        { $unwind: '$fof' },
        { $group: { _id: '$fof.following_id', mutual_count: { $sum: 1 } } },
        { $sort: { mutual_count: -1 } },
        { $limit: 20 },
      ],
    },
    parameters: [{ type: 'reference', referenceTable: 'users', referenceColumn: 'id' }],
    expectedComplexity: 'O(n)',
    weight: 2,
  },

  // Thread and conversation views
  {
    name: 'get_thread',
    description: 'Get a post with all replies (threaded view)',
    category: 'join',
    sql: `WITH RECURSIVE thread AS (
            SELECT p.*, 0 as depth
            FROM posts p WHERE p.id = $1
            UNION ALL
            SELECT p.*, t.depth + 1
            FROM posts p
            JOIN thread t ON p.reply_to_id = t.id
            WHERE p.deleted_at IS NULL AND t.depth < 10
          )
          SELECT t.*, u.username, u.display_name, u.avatar_url
          FROM thread t
          JOIN users u ON t.user_id = u.id
          ORDER BY depth, created_at
          LIMIT 100`,
    documentQuery: {
      collection: 'posts',
      operation: 'aggregate',
      pipeline: [
        { $match: { $or: [{ _id: '$1' }, { thread_id: '$1' }], deleted_at: null } },
        { $sort: { created_at: 1 } },
      ],
    },
    parameters: [{ type: 'reference', referenceTable: 'posts', referenceColumn: 'id' }],
    expectedComplexity: 'O(n)',
    weight: 6,
  },
  {
    name: 'post_replies',
    description: 'Get direct replies to a post',
    category: 'join',
    sql: `SELECT p.*, u.username, u.display_name, u.avatar_url, u.is_verified
          FROM posts p
          JOIN users u ON p.user_id = u.id
          WHERE p.reply_to_id = $1 AND p.deleted_at IS NULL
          ORDER BY p.like_count DESC, p.created_at DESC
          LIMIT 50`,
    documentQuery: {
      collection: 'posts',
      operation: 'find',
      filter: { reply_to_id: '$1', deleted_at: null },
    },
    parameters: [{ type: 'reference', referenceTable: 'posts', referenceColumn: 'id' }],
    expectedComplexity: 'O(log n)',
    weight: 8,
  },

  // Notifications
  {
    name: 'get_notifications',
    description: 'Get user notifications with actor info',
    category: 'join',
    sql: `SELECT n.*, u.username, u.display_name, u.avatar_url, p.content as post_preview
          FROM notifications n
          JOIN users u ON n.actor_id = u.id
          LEFT JOIN posts p ON n.post_id = p.id
          WHERE n.user_id = $1
          ORDER BY n.created_at DESC
          LIMIT 50`,
    documentQuery: {
      collection: 'notifications',
      operation: 'aggregate',
      pipeline: [
        { $match: { user_id: '$1' } },
        { $sort: { created_at: -1 } },
        { $limit: 50 },
        { $lookup: { from: 'users', localField: 'actor_id', foreignField: '_id', as: 'actor' } },
      ],
    },
    parameters: [{ type: 'reference', referenceTable: 'users', referenceColumn: 'id' }],
    expectedComplexity: 'O(log n)',
    weight: 8,
  },
  {
    name: 'unread_notification_count',
    description: 'Get count of unread notifications',
    category: 'aggregate',
    sql: `SELECT COUNT(*) as count FROM notifications WHERE user_id = $1 AND is_read = false`,
    documentQuery: {
      collection: 'notifications',
      operation: 'aggregate',
      pipeline: [
        { $match: { user_id: '$1', is_read: false } },
        { $count: 'count' },
      ],
    },
    parameters: [{ type: 'reference', referenceTable: 'users', referenceColumn: 'id' }],
    expectedComplexity: 'O(log n)',
    weight: 6,
  },

  // Trending and discovery
  {
    name: 'trending_hashtags',
    description: 'Get trending hashtags',
    category: 'range-scan',
    sql: `SELECT * FROM hashtags
          WHERE is_trending = true
          ORDER BY daily_usage DESC
          LIMIT 10`,
    documentQuery: {
      collection: 'hashtags',
      operation: 'find',
      filter: { is_trending: true },
    },
    parameters: [],
    expectedComplexity: 'O(log n)',
    weight: 4,
  },
  {
    name: 'hashtag_posts',
    description: 'Get posts for a hashtag',
    category: 'join',
    sql: `SELECT p.*, u.username, u.display_name, u.avatar_url
          FROM posts p
          JOIN post_hashtags ph ON p.id = ph.post_id
          JOIN users u ON p.user_id = u.id
          WHERE ph.hashtag_id = $1 AND p.deleted_at IS NULL
          ORDER BY p.created_at DESC
          LIMIT 50`,
    documentQuery: {
      collection: 'post_hashtags',
      operation: 'aggregate',
      pipeline: [
        { $match: { hashtag_id: '$1' } },
        { $lookup: { from: 'posts', localField: 'post_id', foreignField: '_id', as: 'post' } },
        { $sort: { created_at: -1 } },
        { $limit: 50 },
      ],
    },
    parameters: [{ type: 'reference', referenceTable: 'hashtags', referenceColumn: 'id', distribution: 'zipf' }],
    expectedComplexity: 'O(log n)',
    weight: 5,
  },
  {
    name: 'search_users',
    description: 'Search users by username or display name',
    category: 'range-scan',
    sql: `SELECT * FROM users
          WHERE (username ILIKE $1 OR display_name ILIKE $1)
          AND status = 'active'
          ORDER BY follower_count DESC
          LIMIT 20`,
    documentQuery: {
      collection: 'users',
      operation: 'find',
      filter: { $text: { $search: '$1' }, status: 'active' },
    },
    parameters: [{ type: 'faker', fakerMethod: 'person.firstName' }],
    expectedComplexity: 'O(n)',
    weight: 4,
  },

  // Direct messages
  {
    name: 'get_conversations',
    description: 'Get user conversations list',
    category: 'join',
    sql: `SELECT c.*, cp.last_read_at,
            (SELECT COUNT(*) FROM direct_messages dm
             WHERE dm.conversation_id = c.id
             AND dm.created_at > COALESCE(cp.last_read_at, '1970-01-01')) as unread_count
          FROM conversations c
          JOIN conversation_participants cp ON c.id = cp.conversation_id
          WHERE cp.user_id = $1 AND cp.left_at IS NULL
          ORDER BY c.last_message_at DESC
          LIMIT 30`,
    documentQuery: {
      collection: 'conversation_participants',
      operation: 'aggregate',
      pipeline: [
        { $match: { user_id: '$1', left_at: null } },
        { $lookup: { from: 'conversations', localField: 'conversation_id', foreignField: '_id', as: 'conversation' } },
        { $sort: { 'conversation.last_message_at': -1 } },
        { $limit: 30 },
      ],
    },
    parameters: [{ type: 'reference', referenceTable: 'users', referenceColumn: 'id' }],
    expectedComplexity: 'O(log n)',
    weight: 5,
  },
  {
    name: 'get_messages',
    description: 'Get messages in a conversation',
    category: 'range-scan',
    sql: `SELECT dm.*, u.username, u.display_name, u.avatar_url
          FROM direct_messages dm
          JOIN users u ON dm.sender_id = u.id
          WHERE dm.conversation_id = $1 AND dm.deleted_at IS NULL
          ORDER BY dm.created_at DESC
          LIMIT 50`,
    documentQuery: {
      collection: 'direct_messages',
      operation: 'find',
      filter: { conversation_id: '$1', deleted_at: null },
    },
    parameters: [{ type: 'reference', referenceTable: 'conversations', referenceColumn: 'id' }],
    expectedComplexity: 'O(log n)',
    weight: 5,
  },

  // Aggregates
  {
    name: 'user_engagement_stats',
    description: 'Get engagement statistics for a user',
    category: 'aggregate',
    sql: `SELECT
            COUNT(DISTINCT p.id) as total_posts,
            SUM(p.like_count) as total_likes_received,
            SUM(p.reply_count) as total_replies_received,
            SUM(p.repost_count) as total_reposts_received
          FROM posts p
          WHERE p.user_id = $1 AND p.deleted_at IS NULL`,
    documentQuery: {
      collection: 'posts',
      operation: 'aggregate',
      pipeline: [
        { $match: { user_id: '$1', deleted_at: null } },
        { $group: { _id: null, total_posts: { $sum: 1 }, total_likes: { $sum: '$like_count' } } },
      ],
    },
    parameters: [{ type: 'reference', referenceTable: 'users', referenceColumn: 'id' }],
    expectedComplexity: 'O(n)',
    weight: 2,
  },
  {
    name: 'post_likers',
    description: 'Get users who liked a post',
    category: 'join',
    sql: `SELECT u.id, u.username, u.display_name, u.avatar_url, u.is_verified
          FROM users u
          JOIN likes l ON u.id = l.user_id
          WHERE l.post_id = $1
          ORDER BY l.created_at DESC
          LIMIT 50`,
    documentQuery: {
      collection: 'likes',
      operation: 'aggregate',
      pipeline: [
        { $match: { post_id: '$1' } },
        { $lookup: { from: 'users', localField: 'user_id', foreignField: '_id', as: 'user' } },
        { $sort: { created_at: -1 } },
        { $limit: 50 },
      ],
    },
    parameters: [{ type: 'reference', referenceTable: 'posts', referenceColumn: 'id', distribution: 'zipf' }],
    expectedComplexity: 'O(log n)',
    weight: 3,
  },

  // Write operations
  {
    name: 'create_post',
    description: 'Create a new post',
    category: 'write',
    sql: `INSERT INTO posts (id, user_id, content, type, visibility, created_at)
          VALUES ($1, $2, $3, $4, $5, NOW())
          RETURNING *`,
    documentQuery: {
      collection: 'posts',
      operation: 'insert',
    },
    parameters: [
      { type: 'uuid' },
      { type: 'reference', referenceTable: 'users', referenceColumn: 'id' },
      { type: 'faker', fakerMethod: 'lorem.paragraphs' },
      { type: 'enum', values: ['text', 'image', 'video'] },
      { type: 'enum', values: ['public', 'followers'] },
    ],
    expectedComplexity: 'O(log n)',
    weight: 4,
  },
  {
    name: 'like_post',
    description: 'Like a post',
    category: 'write',
    sql: `INSERT INTO likes (id, user_id, post_id, created_at)
          VALUES ($1, $2, $3, NOW())
          ON CONFLICT (user_id, post_id) DO NOTHING
          RETURNING *`,
    documentQuery: {
      collection: 'likes',
      operation: 'insert',
    },
    parameters: [
      { type: 'uuid' },
      { type: 'reference', referenceTable: 'users', referenceColumn: 'id' },
      { type: 'reference', referenceTable: 'posts', referenceColumn: 'id', distribution: 'zipf' },
    ],
    expectedComplexity: 'O(log n)',
    weight: 4,
  },
  {
    name: 'follow_user',
    description: 'Follow another user',
    category: 'write',
    sql: `INSERT INTO follows (id, follower_id, following_id, status, created_at)
          VALUES ($1, $2, $3, CASE WHEN (SELECT is_private FROM users WHERE id = $3) THEN 'pending' ELSE 'accepted' END, NOW())
          ON CONFLICT (follower_id, following_id) DO NOTHING
          RETURNING *`,
    documentQuery: {
      collection: 'follows',
      operation: 'insert',
    },
    parameters: [
      { type: 'uuid' },
      { type: 'reference', referenceTable: 'users', referenceColumn: 'id' },
      { type: 'reference', referenceTable: 'users', referenceColumn: 'id', distribution: 'zipf' },
    ],
    expectedComplexity: 'O(log n)',
    weight: 2,
  },
  {
    name: 'create_notification',
    description: 'Create a notification',
    category: 'write',
    sql: `INSERT INTO notifications (id, user_id, actor_id, type, post_id, created_at)
          VALUES ($1, $2, $3, $4, $5, NOW())
          RETURNING *`,
    documentQuery: {
      collection: 'notifications',
      operation: 'insert',
    },
    parameters: [
      { type: 'uuid' },
      { type: 'reference', referenceTable: 'users', referenceColumn: 'id' },
      { type: 'reference', referenceTable: 'users', referenceColumn: 'id' },
      { type: 'enum', values: ['like', 'reply', 'follow', 'mention'] },
      { type: 'reference', referenceTable: 'posts', referenceColumn: 'id' },
    ],
    expectedComplexity: 'O(log n)',
    weight: 3,
  },
  {
    name: 'mark_notifications_read',
    description: 'Mark notifications as read',
    category: 'write',
    sql: `UPDATE notifications SET is_read = true WHERE user_id = $1 AND is_read = false`,
    documentQuery: {
      collection: 'notifications',
      operation: 'update',
      filter: { user_id: '$1', is_read: false },
    },
    parameters: [{ type: 'reference', referenceTable: 'users', referenceColumn: 'id' }],
    expectedComplexity: 'O(n)',
    weight: 2,
  },
  {
    name: 'send_message',
    description: 'Send a direct message',
    category: 'write',
    sql: `INSERT INTO direct_messages (id, conversation_id, sender_id, content, type, created_at)
          VALUES ($1, $2, $3, $4, 'text', NOW())
          RETURNING *`,
    documentQuery: {
      collection: 'direct_messages',
      operation: 'insert',
    },
    parameters: [
      { type: 'uuid' },
      { type: 'reference', referenceTable: 'conversations', referenceColumn: 'id' },
      { type: 'reference', referenceTable: 'users', referenceColumn: 'id' },
      { type: 'faker', fakerMethod: 'lorem.paragraph' },
    ],
    expectedComplexity: 'O(log n)',
    weight: 2,
  },
  {
    name: 'increment_post_counters',
    description: 'Increment post engagement counters',
    category: 'write',
    sql: `UPDATE posts
          SET like_count = like_count + $2,
              reply_count = reply_count + $3,
              repost_count = repost_count + $4,
              view_count = view_count + $5
          WHERE id = $1`,
    documentQuery: {
      collection: 'posts',
      operation: 'update',
      filter: { _id: '$1' },
    },
    parameters: [
      { type: 'reference', referenceTable: 'posts', referenceColumn: 'id' },
      { type: 'random-int', min: 0, max: 1 },
      { type: 'random-int', min: 0, max: 1 },
      { type: 'random-int', min: 0, max: 1 },
      { type: 'random-int', min: 1, max: 100 },
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
    name: 'read_heavy',
    description: 'Typical social media read-heavy workload (95% reads)',
    readWriteRatio: 0.95,
    queries: benchmarkQueries.filter(q => q.category !== 'write').map(q => ({ ...q })),
    targetOps: 20000,
    concurrency: 200,
    duration: 300,
  },
  {
    name: 'feed_generation',
    description: 'Feed-focused workload (home feed, profile, discovery)',
    readWriteRatio: 0.98,
    queries: benchmarkQueries.filter(q =>
      ['home_feed', 'user_profile_posts', 'trending_hashtags', 'hashtag_posts', 'get_user_by_id', 'get_post_by_id'].includes(q.name)
    ),
    targetOps: 15000,
    concurrency: 150,
    duration: 300,
  },
  {
    name: 'engagement',
    description: 'High engagement workload (likes, follows, notifications)',
    readWriteRatio: 0.7,
    queries: benchmarkQueries.filter(q =>
      ['like_post', 'follow_user', 'check_like_status', 'check_follow_status', 'get_notifications', 'unread_notification_count', 'create_notification', 'mark_notifications_read'].includes(q.name)
    ),
    targetOps: 10000,
    concurrency: 100,
    duration: 300,
  },
  {
    name: 'graph_traversal',
    description: 'Social graph focused (followers, following, suggestions)',
    readWriteRatio: 0.99,
    queries: benchmarkQueries.filter(q =>
      ['get_followers', 'get_following', 'mutual_followers', 'suggested_follows', 'check_follow_status'].includes(q.name)
    ),
    targetOps: 8000,
    concurrency: 80,
    duration: 300,
  },
  {
    name: 'messaging',
    description: 'Direct messaging focused workload',
    readWriteRatio: 0.7,
    queries: benchmarkQueries.filter(q =>
      ['get_conversations', 'get_messages', 'send_message'].includes(q.name)
    ),
    targetOps: 5000,
    concurrency: 50,
    duration: 300,
  },
  {
    name: 'viral_content',
    description: 'Simulates viral content spread (high write on popular posts)',
    readWriteRatio: 0.6,
    queries: benchmarkQueries.filter(q =>
      ['get_post_by_id', 'post_replies', 'post_likers', 'like_post', 'create_post', 'increment_post_counters', 'create_notification'].includes(q.name)
    ),
    targetOps: 25000,
    concurrency: 250,
    duration: 300,
  },
]

// ============================================================================
// Dataset Configuration
// ============================================================================

export const socialNetworkDataset: DatasetConfig = {
  name: 'social-network',
  description: 'Social network dataset with users, posts, follows, likes, and messaging',
  version: '1.0.0',
  tables,
  relationships,
  sizeTiers,
  workloads,
  metadata: {
    domain: 'social-media',
    characteristics: [
      'Heavy graph traversal patterns',
      'Zipf distribution on popular users/posts',
      'High fanout for notifications',
      'Time-ordered feed generation',
      'Real-time engagement counters',
    ],
    userDistribution: {
      casual: 0.7,      // < 100 followers
      active: 0.25,     // 100-10K followers
      influencer: 0.045, // 10K-1M followers
      celebrity: 0.005,  // 1M+ followers
    },
  },
}

// Register the dataset
registerDataset(socialNetworkDataset)
