use crate::KvsCompactor;

use super::{cp::*, kvsengine::KvsEngine, thread_pool::ThreadPool};
use mio::{
    net::{TcpListener, TcpStream},
    {Events, Interest, Poll, Token},
};
use mio_signals::{Signal, Signals};
use mio_timerfd::{ClockId, TimerFd};
use slog::Logger;
use smallvec::{smallvec, SmallVec};
use std::{
    error::Error,
    fmt,
    io::prelude::*,
    net::SocketAddr,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

const SERVER_TOKEN: Token = Token(0);
const SERVER_TIMER_TOKEN: Token = Token(1);
const SERVER_SIGNALS_TOKEN: Token = Token(2);

const SERVER_TIMER_CHECK_PERIOD: std::time::Duration = std::time::Duration::from_millis(100);
const SERVER_COMPACTION_PERIOD: std::time::Duration = std::time::Duration::from_secs(5);
const POLL_ATTEMPTS: u16 = 10;

/// Macro to unwrap the Ok of a result or if Err, log and returns the control flow to the caller
#[macro_export]
macro_rules! unwrap_or_return_on_err {
    ($res:expr, $logger:expr, $desc:expr) => {
        match $res {
            Ok(val) => val,
            Err(e) => {
                error!($logger, "Could not {}", $desc; "error" => e.to_string());
                return;
            }
        }
    };
}

/// Macro to unwrap the Ok of a result or if Err, log and returns the control flow with the value 1 to the caller
#[macro_export]
macro_rules! unwrap_or_return_code1_on_err {
    ($res:expr, $logger:expr, $desc:expr) => {
        $res.map_err(|e| {
            error!($logger, "Could not {}", $desc; "error" => e.to_string());
            1i32
        })?
    };
}

/// The server that listens for tcp connections,
/// receive commands, directly communicates with the database engine executing each command
/// and send responses to kvs clients
#[derive(Debug)]
pub struct KvServer<Engine, Tp> {
    db: Engine,
    address: SocketAddr,
    thread_pool: Tp,
    logger: Logger,
    shutdown_trigger: KvServerShutdownTrigger,
    signals: Option<Signals>,
}

/// The signal that is sent to the KvServer indicanting that it should stop running
#[derive(Debug, Clone)]
pub struct KvServerShutdownTrigger(Arc<AtomicBool>);

impl KvServerShutdownTrigger {
    /// Creates a new Shutdown signal
    pub fn new() -> Self {
        Self(Arc::new(AtomicBool::new(false)))
    }

    /// Fires a signal to the server indicating that it must shutdown
    pub fn trigger(&self) {
        self.0.store(true, Ordering::Release);
    }

    /// Tells if the signal was fired
    pub fn must_shutdown(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }
}

/// The error type returned by the new function of the KvServer
#[derive(Debug)]
pub enum KvServerCreationError<'a> {
    /// Engine not supported
    EngineParseError {
        /// engine that the user tried to instantiate
        engine_name: &'a str,
    },

    /// Socket is invalid
    InvalidSocketAddress {
        /// Actual address
        addr: &'a str,
        /// Internal cause
        cause: String,
    },

    /// IO operation failed
    IoError {
        /// Internal cause
        cause: std::io::Error,
    },

    /// The listener failed to start
    UnableToStartListener,
}

struct LogConnectionClosedGuard {
    peer_addr: SocketAddr,
    log_server: Logger,
}

impl Drop for LogConnectionClosedGuard {
    fn drop(&mut self) {
        info!(self.log_server, "closed connection"; "peer" => self.peer_addr);
    }
}

impl<'a> fmt::Display for KvServerCreationError<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            KvServerCreationError::EngineParseError { engine_name } => f.write_fmt(format_args!(
                "Engine parse error: engine {} not supported",
                engine_name
            )),
            KvServerCreationError::InvalidSocketAddress { addr, cause } => f.write_fmt(
                format_args!("Invalid socket address: {}. Error: {}", addr, cause),
            ),
            KvServerCreationError::UnableToStartListener => f.write_str("Could not start listener"),
            KvServerCreationError::IoError { cause } => {
                f.write_fmt(format_args!("IO operation failed with error: {}", cause))
            }
        }
    }
}

impl<'a> Error for KvServerCreationError<'a> {}

impl<Engine, Tp> KvServer<Engine, Tp>
where
    Engine: KvsEngine + KvsCompactor,
    Tp: ThreadPool,
{
    /// Creates a new KvServer object given the `engine` type name,
    /// the server `address` and the `config_file` for reading/storing server configuration.
    pub fn new<'a>(
        engine: Engine,
        address: &'a str,
        thread_pool: Tp,
        logger: Logger,
        signals: Option<Signals>,
    ) -> Result<Self, KvServerCreationError<'a>> {
        let address = match address.parse::<SocketAddr>() {
            Ok(addr) => addr,
            Err(err) => {
                return Err(KvServerCreationError::InvalidSocketAddress {
                    addr: address,
                    cause: err.to_string(),
                })
            }
        };
        Ok(Self {
            db: engine,
            address,
            thread_pool,
            logger,
            shutdown_trigger: KvServerShutdownTrigger::new(),
            signals,
        })
    }

    fn poll(&mut self, poll: &mut Poll, events: &mut Events) -> Result<(), i32> {
        let mut poll_attempt = POLL_ATTEMPTS;
        loop {
            if let Err(e) = poll.poll(events, None) {
                match e.kind() {
                    std::io::ErrorKind::Interrupted if poll_attempt > 0 => poll_attempt -= 1,
                    _ => {
                        error!(self.logger, "Could not poll the server for events"; "error" => e.to_string());
                        return Err(1i32);
                    }
                }
            } else {
                break;
            }
        }
        Ok(())
    }

    /// Starts listening for connections and enter the forever loop handling server connections
    pub fn run(&mut self) -> Result<(), i32> {
        let mut listener = unwrap_or_return_code1_on_err!(
            TcpListener::bind(self.address),
            self.logger,
            format!("open listener on address {}", self.address)
        );
        let mut poll = unwrap_or_return_code1_on_err!(Poll::new(), self.logger, "create a poll");
        unwrap_or_return_code1_on_err!(
            poll.registry()
                .register(&mut listener, SERVER_TOKEN, Interest::READABLE),
            self.logger,
            "register event source: listener"
        );
        let mut timer = unwrap_or_return_code1_on_err!(
            TimerFd::new(ClockId::Monotonic),
            self.logger,
            "create a timer"
        );
        unwrap_or_return_code1_on_err!(
            timer.set_timeout_interval(&SERVER_TIMER_CHECK_PERIOD),
            self.logger,
            "setup the timer"
        );
        unwrap_or_return_code1_on_err!(
            poll.registry()
                .register(&mut timer, SERVER_TIMER_TOKEN, Interest::READABLE),
            self.logger,
            "register event source: timer"
        );
        if let Some(signals) = &mut self.signals {
            unwrap_or_return_code1_on_err!(
                poll.registry()
                    .register(signals, SERVER_SIGNALS_TOKEN, Interest::READABLE),
                self.logger,
                "register event source: signal handler"
            );
        }
        let compaction_timer_check_init =
            SERVER_COMPACTION_PERIOD.as_millis() / SERVER_TIMER_CHECK_PERIOD.as_millis();
        let mut compaction_timer_check_count = compaction_timer_check_init;
        let compactor_running = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let mut events = Events::with_capacity(1024);
        loop {
            self.poll(&mut poll, &mut events)?;
            if self.shutdown_trigger.must_shutdown() {
                info!(self.logger, "server is shutting down");
                break;
            }
            for event in events.iter() {
                match event.token() {
                    SERVER_TOKEN => {
                        let log_server = self.logger.clone();
                        let db = self.db.clone();
                        let (mut stream, peer_addr) = match listener.accept() {
                            Ok((stream, address)) => (stream, address),
                            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                                break;
                            }
                            Err(e) => {
                                error!(self.logger, "Server failed while polling for events"; "error" => e);
                                return Err(1);
                            }
                        };
                        self.thread_pool.spawn(move || {
                            info!(log_server, "Acceppted connection"; "peer" => peer_addr);
                            let _log_conn_closed_guard = LogConnectionClosedGuard {
                                peer_addr,
                                log_server: log_server.clone(),
                            };
                            match unwrap_or_return_on_err!(
                                KvServer::<Engine, Tp>::recv_payload(&mut stream),
                                log_server,
                                "get the message payload from peer's message"
                            ) {
                                MessagePayload::Request(Request::Set(req)) => {
                                    info!(log_server, "received message"; "peer" => peer_addr, "payload_type" => "RequestSet", "key" => req.key(), "value" => req.value());
                                    let res = db.set(req.key().to_owned(), req.value().to_owned());
                                    let resp = ResponseSet::new_message(StatusCode::from(&res));
                                    unwrap_or_return_on_err!(
                                        KvServer::<Engine, Tp>::send_response(&resp, &mut stream),
                                        log_server,
                                        "send response to peer"
                                    );
                                    info!(log_server, "sent message"; "peer" => peer_addr, "payload_type" => "ResponseSet", "status" => StatusCode::from(&res).to_string());
                                }
                                MessagePayload::Request(Request::Get(req)) => {
                                    info!(log_server, "received message"; "peer" => peer_addr, "payload_type" => "RequestGet", "key" => req.key());
                                    let res = db.get(req.key().to_owned());
                                    let value = res.as_ref().unwrap_or(&None).clone();
                                    let resp = ResponseGet::new_message(StatusCode::from(&res), value.clone());
                                    unwrap_or_return_on_err!(
                                        KvServer::<Engine, Tp>::send_response(&resp, &mut stream),
                                        log_server,
                                        "send response to peer"
                                    );
                                    info!(log_server, "sent message"; "peer" => peer_addr, "payload_type" => "ResponseGet", "status" => StatusCode::from(&res).to_string(), "value" => value);
                                }
                                MessagePayload::Request(Request::Remove(req)) => {
                                    info!(log_server, "received message"; "peer" => peer_addr, "payload_type" => "RequestRemove", "key" => req.key());
                                    let res = db.remove(req.key().to_owned());
                                    let resp = ResponseRemove::new_message(StatusCode::from(&res));
                                    unwrap_or_return_on_err!(
                                        KvServer::<Engine, Tp>::send_response(&resp, &mut stream),
                                        log_server,
                                        "send response to peer"
                                    );
                                    info!(log_server, "sent message"; "peer" => peer_addr, "payload_type" => "ResponseRemove", "status" => StatusCode::from(&res).to_string());
                                }
                                MessagePayload::Response(_) => {
                                    // Error: client sent a response message
                                    error!(log_server, "received message"; "peer" => peer_addr, "payload_type" => "Response");
                                    let resp = ResponseSet::new_message(StatusCode::FatalError);
                                    unwrap_or_return_on_err!(
                                        KvServer::<Engine, Tp>::send_response(&resp, &mut stream),
                                        log_server,
                                        "send response to peer"
                                    );
                                }
                            }
                        });
                    }
                    SERVER_TIMER_TOKEN => {
                        if !compactor_running.load(std::sync::atomic::Ordering::Acquire) {
                            if compaction_timer_check_count == 0 {
                                let db = self.db.clone();
                                let logger = self.logger.clone();
                                let compactor_running = compactor_running.clone();
                                compactor_running.store(true, std::sync::atomic::Ordering::Release);
                                self.thread_pool.spawn(move || {
                                    KvServer::<Engine, Tp>::run_compactor(db, logger);
                                    compactor_running
                                        .store(false, std::sync::atomic::Ordering::Release);
                                });
                                compaction_timer_check_count = compaction_timer_check_init;
                            } else {
                                compaction_timer_check_count -= 1;
                            }
                        }

                        unwrap_or_return_code1_on_err!(
                            timer.set_timeout_interval(&SERVER_TIMER_CHECK_PERIOD),
                            self.logger,
                            "setup the timer"
                        );
                    }
                    SERVER_SIGNALS_TOKEN => {
                        if let Some(signals) = &mut self.signals {
                            loop {
                                match unwrap_or_return_code1_on_err!(
                                    signals.receive(),
                                    self.logger,
                                    "retrieve received signal"
                                ) {
                                    Some(Signal::Interrupt)
                                    | Some(Signal::Terminate)
                                    | Some(Signal::Quit) => {
                                        info!(self.logger, "server is shutting down");
                                        return Ok(());
                                    }
                                    None => break,
                                    Some(sig) => {
                                        error!(self.logger, "received unexpected signal"; "signal" => format!("{:?}",sig));
                                        return Err(1);
                                    }
                                }
                            }
                        }
                    }
                    _ => unreachable!(),
                }
            }
        }
        Ok(())
    }

    /// Gives the user a trigger that can be used to signal to the server that it must stop running
    pub fn get_shutdown_trigger(&self) -> KvServerShutdownTrigger {
        self.shutdown_trigger.clone()
    }

    fn send_response(msg: &Message, stream: &mut TcpStream) -> Result<(), error::Error> {
        let mut buf = SmallVec::<[u8; 256]>::new();
        buf.resize(ser::calc_len(msg)?, 0u8);
        ser::to_bytes(msg, &mut buf[..])?;
        stream.write_all(&buf[..])?;
        Ok(())
    }

    fn recv_payload(stream: &mut TcpStream) -> Result<MessagePayload, error::Error> {
        let mut header_buf = [0u8; HEADER_SIZE];
        stream.read_exact(&mut header_buf)?;
        let header: Result<Header, _> = de::from_bytes(&header_buf);
        let header = header?;

        let mut payload_buf: SmallVec<[u8; 256]> = smallvec![0; header.payload_length() as usize];
        stream.read_exact(&mut payload_buf)?;
        de::from_bytes(&payload_buf)
    }

    fn run_compactor(db: Engine, logger: Logger) {
        unwrap_or_return_on_err!(db.compact(), logger, "run compaction successfully");
    }
}
