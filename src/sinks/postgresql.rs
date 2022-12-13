use vector_config::component::GenerateConfig;
use vector_core::config::{AcknowledgementsConfig, DataType, Input};
use crate::codecs::{Encoder, EncodingConfig};
use crate::config::{SinkConfig, SinkContext};

/// Configuration for the `postgresql` sink.
#[configurable_component(sink("postgresql"))]
#[derive(Clone, Debug)]
#[serde(default, deny_unknown_fields)]
pub struct PostgresqlSinkConfig {
    /// PostgreSQL connection URI.
    url: String,

    #[configurable(derived)]
    encoding: EncodingConfig,

    #[configurable(derived)]
    #[serde(
    default,
    deserialize_with = "crate::serde::bool_or_struct",
    skip_serializing_if = "crate::serde::skip_serializing_if_default"
    )]
    pub acknowledgements: AcknowledgementsConfig,
}

impl GenerateConfig for PostgresqlSinkConfig {
    fn generate_config() -> toml::Value {
        toml::from_str(
            r#"
            url = "postgresql://localhost/mydb"
            encoding.codec = "json"
            "#,
        )
            .unwrap()
    }
}

#[async_trait::async_trait]
impl SinkConfig for PostgresqlSinkConfig {
    async fn build(
        &self,
        _cx: SinkContext,
    ) -> crate::Result<(super::VectorSink, super::Healthcheck)> {
        let conn = self.build_client().await.context(RedisCreateFailedSnafu)?;
        let healthcheck = PostgresqlSinkConfig::healthcheck(conn.clone()).boxed();
        let sink = self.new(conn)?;
        Ok((sink, healthcheck))
    }

    fn input(&self) -> Input {
        Input::new(self.encoding.config().input_type() & DataType::Log)
    }

    fn acknowledgements(&self) -> &AcknowledgementsConfig {
        &self.acknowledgements
    }
}

impl PostgresqlSinkConfig {
    pub fn new(&self, conn: ConnectionManager) -> crate::Result<super::VectorSink> {
        let request = self.request.unwrap_with(&TowerRequestConfig {
            concurrency: Concurrency::Fixed(1),
            ..Default::default()
        });

        let key = self.key.clone();

        let transformer = self.encoding.transformer();
        let serializer = self.encoding.build()?;
        let mut encoder = Encoder::<()>::new(serializer);

        let redis = RedisSink {
            conn,
            data_type,
            bytes_sent: register!(BytesSent::from(Protocol::TCP)),
        };

        let svc = ServiceBuilder::new()
            .settings(request, RedisRetryLogic)
            .service(redis);

        let sink = BatchSink::new(svc, buffer, batch.timeout)
            .with_flat_map(move |event| {
                // Errors are handled by `Encoder`.
                stream::iter(encode_event(event, &key, &transformer, &mut encoder)).map(Ok)
            })
            .sink_map_err(|error| error!(message = "Sink failed to flush.", %error));

        Ok(super::VectorSink::from_event_sink(sink))
    }

    async fn build_client(&self) -> RedisResult<ConnectionManager> {
        trace!("Open Redis client.");
        let client = redis::Client::open(self.url.as_str())?;
        trace!("Open Redis client success.");
        trace!("Get Redis connection.");
        let conn = client.get_tokio_connection_manager().await;
        trace!("Get Redis connection success.");
        conn
    }

    async fn healthcheck(mut conn: ConnectionManager) -> crate::Result<()> {
        redis::cmd("PING")
            .query_async(&mut conn)
            .await
            .map_err(Into::into)
    }
}
