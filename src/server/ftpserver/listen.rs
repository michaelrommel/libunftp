//! Contains the code that listens to control channel connections in a non-proxy protocol mode.

use super::{ServerError, chosen::OptionsHolder};
use crate::server::failed_logins::FailedLoginsCache;
use crate::server::shutdown;
use crate::{auth::UserDetail, server::controlchan, storage::StorageBackend};
// use chrono::{DateTime, Duration, Utc};
use redis::aio::ConnectionManager;
// use reqwest::Error;
// use serde::Deserialize;
// use std::env;
use std::ffi::OsString;
use std::net::SocketAddr;
#[cfg(unix)]
use std::os::fd::AsRawFd;
use std::sync::Arc;
use tokio::net::TcpListener;

// #[derive(Deserialize, Debug)]
// #[serde(rename_all = "camelCase")]
// struct AwsCredentials {
//     role_arn: String,
//     access_key_id: String,
//     secret_access_key: String,
//     token: String,
//     expiration: String,
// }

// async fn fetch_creds(url: &str) -> Result<AwsCredentials, Error> {
//     let response = reqwest::get(url).await?.json::<AwsCredentials>().await?;
//     Ok(response)
// }

// fn check_expiry(name: &str, minutes: i64) -> bool {
//     if let Ok(val) = env::var(name) {
//         // 2. Convert value to a DateTime (from sth. like "2023-10-27T10:00:00Z")
//         if let Ok(expiry) = DateTime::parse_from_rfc3339(&val) {
//             let expiry_utc = expiry.with_timezone(&Utc);
//             let now = Utc::now();
//             return expiry_utc > now && expiry_utc < (now + Duration::minutes(minutes));
//         }
//     }
//     false
// }

// async fn provision_credentials(logger: &slog::Logger) {
//     if let Ok(cred_uri) = env::var("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI") {
//         slog::debug!(logger, "Checking S3 credentials {:?}", env::var("AWS_TOKEN_EXPIRATION"));
//         if !check_expiry("AWS_TOKEN_EXPIRATION", 15) {
//             slog::info!(logger, "AWS Token expired or will be soon, renewing");
//             match fetch_creds(&cred_uri).await {
//                 Ok(credentials) => {
//                     slog::debug!(logger, "Expiration: {:?}", credentials.expiration)
//                 }
//                 Err(e) => {
//                     slog::error!(logger, "Error fetching credentials from ECS URI {:?}", e);
//                     // clean all environment variables regarding AWS credentials
//                     env::remove_var("AWS_TOKEN_EXPIRATION");
//                 }
//             }
//         }
//     } else {
//         slog::error!(logger, "The ECS Credentials Relative URI is not set.")
//     }
// }

// Listener listens for control channel connections on a TCP port and spawns a control channel loop
// in a new task for each incoming connection.
pub struct Listener<Storage, User>
where
    Storage: StorageBackend<User>,
    User: UserDetail,
{
    pub bind_address: SocketAddr,
    pub logger: slog::Logger,
    pub options: OptionsHolder<Storage, User>,
    pub shutdown_topic: Arc<shutdown::Notifier>,
    pub failed_logins: Option<Arc<FailedLoginsCache>>,
    pub connection_helper: Option<OsString>,
    pub connection_helper_args: Vec<OsString>,
    pub metastore: Option<ConnectionManager>,
}

impl<Storage, User> Listener<Storage, User>
where
    Storage: StorageBackend<User> + 'static,
    User: UserDetail + 'static,
{
    // Starts listening, returning an error if the TCP address could not be bound to.
    pub async fn listen(self) -> std::result::Result<(), ServerError> {
        let Listener {
            logger,
            bind_address,
            options,
            shutdown_topic,
            failed_logins,
            connection_helper,
            connection_helper_args,
            metastore,
        } = self;
        let listener = TcpListener::bind(bind_address).await?;
        loop {
            let shutdown_listener = shutdown_topic.subscribe().await;
            match listener.accept().await {
                Ok((tcp_stream, socket_addr)) => {
                    slog::info!(logger, "Incoming control connection from {:?}", socket_addr);
                    // provision AWS credentials to the environment if inside ECS
                    // provision_credentials(&logger).await;
                    if let Some(helper) = connection_helper.as_ref() {
                        slog::info!(logger, "Spawning connection helper: {:?} {:?}", helper, connection_helper_args);
                        #[cfg(unix)]
                        Self::spawn_helper(&logger, helper, &connection_helper_args, &tcp_stream, socket_addr);
                        #[cfg(not(unix))]
                        unimplemented!()
                    } else {
                        let result = controlchan::spawn_loop::<Storage, User>(
                            (&options).into(),
                            tcp_stream,
                            None,
                            None,
                            shutdown_listener,
                            failed_logins.clone(),
                            metastore.clone(),
                        )
                        .await;
                        if let Err(err) = result {
                            slog::error!(logger, "Could not spawn control channel loop for connection from {:?}: {:?}", socket_addr, err);
                        }
                    }
                }
                Err(err) => {
                    slog::error!(logger, "Error accepting incoming control connection {:?}", err);
                }
            }
        }
    }

    #[cfg(unix)]
    fn spawn_helper(
        logger: &slog::Logger,
        helper: &OsString,
        connection_helper_args: &[OsString],
        tcp_stream: &tokio::net::TcpStream,
        socket_addr: SocketAddr,
    ) {
        let fd = tcp_stream.as_raw_fd();
        nix::fcntl::fcntl(tcp_stream, nix::fcntl::FcntlArg::F_SETFD(nix::fcntl::FdFlag::empty())).unwrap();
        let result = tokio::process::Command::new(helper)
            .args(connection_helper_args.iter())
            .arg(fd.to_string())
            .spawn();
        let logger2 = logger.clone();
        match result {
            Ok(mut child) => {
                tokio::spawn(async move {
                    let child_status = child.wait().await;
                    slog::debug!(logger2, "helper process exited {:?}", child_status);
                });
            }
            Err(err) => {
                slog::error!(logger, "Could not spawn helper process for connection from {:?}: {:?}", socket_addr, err);
            }
        }
    }
}
