//! The RFC 959 Store (`STOR`) command
//
// This command causes the server-DTP to accept the data
// transferred via the data connection and to store the data as
// a file at the server site.  If the file specified in the
// pathname exists at the server site, then its contents shall
// be replaced by the data being transferred.  A new file is
// created at the server site if the file specified in the
// pathname does not already exist.

use crate::server::chancomms::DataChanCmd;
use crate::{
    auth::UserDetail,
    server::controlchan::{
        Reply, ReplyCode,
        command::Command,
        error::ControlChanError,
        handler::{CommandContext, CommandHandler},
    },
    storage::{Metadata, StorageBackend},
};
use async_trait::async_trait;

#[derive(Debug)]
pub struct Stor;

#[async_trait]
impl<Storage, User> CommandHandler<Storage, User> for Stor
where
    User: UserDetail + 'static,
    Storage: StorageBackend<User> + 'static,
    Storage::Metadata: Metadata,
{
    #[tracing_attributes::instrument]
    async fn handle(&self, args: CommandContext<Storage, User>) -> Result<Reply, ControlChanError> {
        let mut session = args.session.lock().await;

        let (cmd, path): (DataChanCmd, String) = match args.parsed_command.clone() {
            Command::Stor { path } => {
                let path_clone = path.clone();
                (DataChanCmd::Stor { path }, path_clone)
            }
            _ => panic!("Programmer error, expected command to be STOR"),
        };

        let logger = args.logger;
        slog::info!(logger, "STOR: command received"; "path" => &path);
        match session.data_cmd_tx.take() {
            Some(tx) => {
                slog::info!(logger, "STOR: data channel available, dispatching command and sending 150"; "path" => &path);
                let logger2 = logger.clone();
                tokio::spawn(async move {
                    if let Err(err) = tx.send(cmd).await {
                        slog::warn!(logger2, "STOR: could not notify data channel to respond with STOR. {}", err);
                    }
                });
                Ok(Reply::new(ReplyCode::FileStatusOkay, "Ready to receive data"))
            }
            None => {
                slog::warn!(logger, "STOR: no data connection established"; "path" => &path);
                Ok(Reply::new(ReplyCode::CantOpenDataConnection, "No data connection established"))
            }
        }
    }
}
