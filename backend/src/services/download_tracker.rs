use std::sync::Arc;

use chrono::{DateTime, Utc};
use serde::Serialize;
use sqlx::PgPool;
use tracing::warn;
use uuid::Uuid;

use crate::services::event_bus::EventBus;
use crate::storage::PresignedUrlSource;

/// Storage source for a download event, mapped to the `source` column
/// values in `download_statistics`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DownloadSource {
    /// Served through proxy cache or local storage
    Proxy,
    /// Served via presigned/redirect URL
    Redirect(PresignedUrlSource),
}

impl DownloadSource {
    pub fn as_str(&self) -> &'static str {
        match self {
            DownloadSource::Proxy => "proxy",
            DownloadSource::Redirect(PresignedUrlSource::S3) => "redirect-s3",
            DownloadSource::Redirect(PresignedUrlSource::CloudFront) => "redirect-cloudfront",
            DownloadSource::Redirect(PresignedUrlSource::Azure) => "redirect-azure",
            DownloadSource::Redirect(PresignedUrlSource::Gcs) => "redirect-gcs",
        }
    }
}

/// Input to [`DownloadTracker::record_download`].
#[derive(Debug, Clone)]
pub struct DownloadRecord {
    pub artifact_id: Option<Uuid>,
    pub proxy_cache_artifact_id: Option<Uuid>,
    pub user_id: Option<Uuid>,
    pub ip_address: String,
    pub user_agent: Option<String>,
    pub source: DownloadSource,
    pub byte_range: Option<String>,
    pub session_id: Option<Uuid>,
    pub repository_id: Option<Uuid>,
    pub actor: Option<String>,
}

impl Default for DownloadRecord {
    fn default() -> Self {
        Self {
            artifact_id: None,
            proxy_cache_artifact_id: None,
            user_id: None,
            ip_address: String::new(),
            user_agent: None,
            source: DownloadSource::Proxy,
            byte_range: None,
            session_id: None,
            repository_id: None,
            actor: None,
        }
    }
}

/// Builder for [`DownloadRecord`].
pub struct DownloadRecordBuilder {
    record: DownloadRecord,
}

impl DownloadRecordBuilder {
    pub fn for_artifact(artifact_id: Uuid) -> Self {
        Self {
            record: DownloadRecord {
                artifact_id: Some(artifact_id),
                ..Default::default()
            },
        }
    }

    pub async fn for_proxy_cache(repo_id: Uuid, cache_key: &str, db: &PgPool) -> Self {
        let pca_id = lookup_proxy_cache_artifact_id_impl(repo_id, cache_key, db).await;
        Self {
            record: DownloadRecord {
                proxy_cache_artifact_id: pca_id,
                repository_id: Some(repo_id),
                ..Default::default()
            },
        }
    }

    pub fn ip(mut self, ip: impl Into<String>) -> Self {
        self.record.ip_address = ip.into();
        self
    }

    pub fn ua(mut self, ua: Option<impl Into<String>>) -> Self {
        self.record.user_agent = ua.map(|s| s.into());
        self
    }

    pub fn source(mut self, source: DownloadSource) -> Self {
        self.record.source = source;
        self
    }

    pub fn byte_range(mut self, byte_range: Option<impl Into<String>>) -> Self {
        self.record.byte_range = byte_range.map(|s| s.into());
        self
    }

    pub fn session_id(mut self, session_id: Option<Uuid>) -> Self {
        self.record.session_id = session_id;
        self
    }

    pub fn user_id(mut self, user_id: Option<Uuid>) -> Self {
        self.record.user_id = user_id;
        self
    }

    pub fn repository_id(mut self, repository_id: Option<Uuid>) -> Self {
        self.record.repository_id = repository_id;
        self
    }

    pub fn actor(mut self, actor: Option<String>) -> Self {
        self.record.actor = actor;
        self
    }

    pub fn build(self) -> DownloadRecord {
        self.record
    }
}

async fn lookup_proxy_cache_artifact_id_impl(
    repo_id: Uuid,
    cache_key: &str,
    db: &PgPool,
) -> Option<Uuid> {
    match sqlx::query_scalar::<_, Uuid>(
        r#"
        SELECT id FROM proxy_cache_artifacts
        WHERE storage_key = $1 AND repository_id = $2
        "#,
    )
    .bind(cache_key)
    .bind(repo_id)
    .fetch_optional(db)
    .await
    {
        Ok(Some(id)) => Some(id),
        Ok(None) => {
            warn!(%repo_id, %cache_key, "proxy_cache_artifact not found for download tracking");
            None
        }
        Err(e) => {
            warn!(%repo_id, %cache_key, error = %e, "failed to lookup proxy_cache_artifact");
            None
        }
    }
}

/// A single download statistics record returned by [`DownloadTracker::list_downloads`].
#[derive(Debug, Clone, Serialize, sqlx::FromRow)]
pub struct DownloadStatisticsItem {
    pub id: Uuid,
    pub artifact_id: Option<Uuid>,
    pub proxy_cache_artifact_id: Option<Uuid>,
    pub user_id: Option<Uuid>,
    pub ip_address: Option<String>,
    pub user_agent: Option<String>,
    pub source: String,
    pub byte_range: Option<String>,
    pub session_id: Option<Uuid>,
    pub repository_id: Option<Uuid>,
    pub downloaded_at: DateTime<Utc>,
}

/// Single entry point for recording artifact download events.
///
/// Every download path (generic, format-handler, virtual, proxy, presigned)
/// routes through [`DownloadTracker::record_download`] so there is exactly
/// one place that writes to `download_statistics`.
#[derive(Clone)]
pub struct DownloadTracker {
    db: PgPool,
    event_bus: Arc<EventBus>,
}

impl DownloadTracker {
    pub fn new(db: PgPool, event_bus: Arc<EventBus>) -> Self {
        Self { db, event_bus }
    }

    /// Look up the `proxy_cache_artifacts.id` for a given cache key (storage_key)
    /// and repository. Returns `None` if the proxy artifact was evicted or
    /// was never cached (e.g. uncached upstream fetch). Best-effort: errors
    /// are logged and return `None`.
    pub async fn lookup_proxy_cache_artifact_id(
        &self,
        repo_id: Uuid,
        cache_key: &str,
    ) -> Option<Uuid> {
        lookup_proxy_cache_artifact_id_impl(repo_id, cache_key, &self.db).await
    }

    /// Record a download event in `download_statistics` and emit a
    /// `artifact.downloaded` domain event.
    ///
    /// This is a **best-effort** write: database errors are logged but not
    /// propagated to the caller. The `artifact.downloaded` event is emitted
    /// after the write for webhooks and notifications.
    pub async fn record_download(&self, record: DownloadRecord) -> Option<Uuid> {
        let source_str = record.source.as_str();

        let result = sqlx::query_scalar::<_, Uuid>(
            r#"
            INSERT INTO download_statistics
                (artifact_id, proxy_cache_artifact_id, user_id, ip_address, user_agent, source, byte_range, session_id, repository_id)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            RETURNING id
            "#,
        )
        .bind(record.artifact_id)
        .bind(record.proxy_cache_artifact_id)
        .bind(record.user_id)
        .bind(&record.ip_address)
        .bind(&record.user_agent)
        .bind(source_str)
        .bind(&record.byte_range)
        .bind(record.session_id)
        .bind(record.repository_id)
        .fetch_optional(&self.db)
        .await;

        match result {
            Ok(Some(id)) => {
                tracing::info!(
                    ?record.artifact_id,
                    ?record.proxy_cache_artifact_id,
                    source = source_str,
                    ?record.byte_range,
                    ?record.session_id,
                    %id,
                    "Download recorded"
                );

                let entity_id = record
                    .artifact_id
                    .or(record.proxy_cache_artifact_id)
                    .map(|id| id.to_string())
                    .unwrap_or_default();
                match record.repository_id {
                    Some(repo_id) => self.event_bus.emit_for_repo(
                        "artifact.downloaded",
                        entity_id,
                        repo_id,
                        record.actor,
                    ),
                    None => self
                        .event_bus
                        .emit("artifact.downloaded", entity_id, record.actor),
                }

                Some(id)
            }
            Ok(None) => {
                warn!("INSERT into download_statistics returned no id");
                None
            }
            Err(e) => {
                warn!(
                    ?record.artifact_id,
                    ?record.proxy_cache_artifact_id,
                    error = %e,
                    "Failed to record download statistics"
                );
                None
            }
        }
    }

    /// Convenience wrapper: looks up the proxy-cache artifact (best-effort),
    /// builds a [`DownloadRecord`] with the given IP / user-agent, and records
    /// the download. Returns the new `download_statistics` row id when the
    /// INSERT succeeds.
    pub async fn record_proxy_download(
        &self,
        repo_id: Uuid,
        cache_key: &str,
        db: &PgPool,
        client_ip: &str,
        user_agent: Option<&str>,
    ) -> Option<Uuid> {
        let record = DownloadRecordBuilder::for_proxy_cache(repo_id, cache_key, db)
            .await
            .ip(client_ip)
            .ua(user_agent)
            .build();
        self.record_download(record).await
    }

    /// List recent download statistics, ordered by newest first.
    pub async fn list_downloads(&self, per_page: i64) -> Vec<DownloadStatisticsItem> {
        sqlx::query_as::<_, DownloadStatisticsItem>(
            r#"
            SELECT id, artifact_id, proxy_cache_artifact_id, user_id,
                   ip_address, user_agent, source, byte_range, session_id,
                   repository_id, downloaded_at
            FROM download_statistics
            ORDER BY downloaded_at DESC
            LIMIT $1
            "#,
        )
        .bind(per_page)
        .fetch_all(&self.db)
        .await
        .unwrap_or_default()
    }
}
