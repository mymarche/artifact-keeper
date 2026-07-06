use std::sync::Arc;

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

    /// Record a download event in `download_statistics` and emit a
    /// `artifact.downloaded` domain event.
    ///
    /// This is a **best-effort** write: database errors are logged but not
    /// propagated to the caller. The `artifact.downloaded` event is emitted
    /// after the write for webhooks and notifications.
    pub async fn record_download(
        &self,
        artifact_id: Option<Uuid>,
        proxy_cache_artifact_id: Option<Uuid>,
        user_id: Option<Uuid>,
        ip_address: &str,
        user_agent: Option<&str>,
        source: DownloadSource,
        byte_range: Option<&str>,
        session_id: Option<Uuid>,
        repository_id: Option<Uuid>,
        actor: Option<String>,
    ) {
        let source_str = source.as_str();

        if let Err(e) = sqlx::query(
            r#"
            INSERT INTO download_statistics
                (artifact_id, proxy_cache_artifact_id, user_id, ip_address, user_agent, source, byte_range, session_id)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            "#,
        )
        .bind(artifact_id)
        .bind(proxy_cache_artifact_id)
        .bind(user_id)
        .bind(ip_address)
        .bind(user_agent)
        .bind(source_str)
        .bind(byte_range)
        .bind(session_id)
        .execute(&self.db)
        .await
        {
            warn!(
                ?artifact_id,
                ?proxy_cache_artifact_id,
                error = %e,
                "Failed to record download statistics"
            );
            return;
        }

        tracing::info!(
            ?artifact_id,
            ?proxy_cache_artifact_id,
            source = source_str,
            ?byte_range,
            ?session_id,
            "Download recorded"
        );

        // Emit domain event for webhooks / notifications.
        let entity_id = artifact_id
            .or(proxy_cache_artifact_id)
            .map(|id| id.to_string())
            .unwrap_or_default();
        match repository_id {
            Some(repo_id) => self.event_bus.emit_for_repo(
                "artifact.downloaded",
                entity_id,
                repo_id,
                actor,
            ),
            None => self.event_bus.emit("artifact.downloaded", entity_id, actor),
        }
    }
}
