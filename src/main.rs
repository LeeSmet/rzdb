use redis_protocol::{decode::decode, encode::encode, types::Frame};
use tokio::net::{TcpListener, UnixListener};
use tokio::prelude::*;
use tokio::sync::mpsc;

// TODO
const BUFFER_SIZE: usize = 9 * 1024;
const BATCH_SIZE: usize = 1_000;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(run())?;
    Ok(())
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    // let db = sled::open("test.db").unwrap();

    let db = sled::Config::default()
        .path("test.db")
        //    .use_compression(true)
        // .cache_capacity(4_000_000_000)
        .open()
        .unwrap();

    // let mut listener = TcpListener::bind("0.0.0.0:9900").await?;
    let mut unix_listener = UnixListener::bind("/tmp/rzdb.sock")?;

    loop {
        let (socket, _) = unix_listener.accept().await?;

        eprintln!("Accepted connection");

        let db = db.clone();

        let client = Client::new(socket, db);

        tokio::spawn(client.run());
    }
}

#[derive(Debug)]
enum RzdbError {
    EmptyPayload,
    IllegalType,
    UnknownCommand,
    WrongArgumentCount,
}

impl std::fmt::Display for RzdbError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            RzdbError::EmptyPayload => write!(f, "Empty payload"),
            RzdbError::IllegalType => write!(f, "Illegal type"),
            RzdbError::UnknownCommand => write!(f, "Unknown command"),
            RzdbError::WrongArgumentCount => write!(f, "Wrong amount of arguments"),
        }
    }
}

impl std::error::Error for RzdbError {}

#[derive(Debug)]
struct Client<T> {
    transport: T,
    otx: mpsc::Sender<DbOperation>,
    rrx: mpsc::Receiver<DbResult>,
}

impl<T> Client<T>
where
    T: AsyncRead + AsyncWrite + Unpin + Send + Sync,
{
    fn new(transport: T, db: sled::Db) -> Self {
        let (otx, orx) = mpsc::channel(BATCH_SIZE);
        let (rtx, rrx) = mpsc::channel(1);

        // set up db thread
        let dbm = DbManager::new(db, orx, rtx);
        tokio::spawn(dbm.run());

        Client {
            transport,
            otx,
            rrx,
        }
    }

    async fn run(mut self) {
        // set up read and write buffers
        let mut read_buf = [0u8; BUFFER_SIZE];
        let mut write_buf = [0u8; BUFFER_SIZE];

        let mut buf_size = 0;

        loop {
            let n = match self.transport.read(&mut read_buf[buf_size..]).await {
                Ok(n) if n == 0 => {
                    eprintln!("Client disconnect");
                    return;
                } //closed socket
                Ok(n) => n,
                Err(e) => {
                    eprintln!("failed to read form socket, err = {:?}", e);
                    return;
                }
            };

            // update buf_size to point to the end of the data buffer
            buf_size += n;

            // try to parse a frame
            let (frame, consumed) = match decode(&read_buf[0..buf_size]) {
                Ok((f, c)) => (f, c),
                Err(e) => {
                    eprintln!("Protocol errror: {:?}", e);
                    // consider this fatal
                    return;
                }
            };

            let frame = match frame {
                Some(f) => f,
                None => {
                    eprintln!("no frame yet, buffer head at {}", buf_size);
                    continue; // need more data
                }
            };

            // we now have a frame. Shift the buffer to remove decoded bytes
            read_buf.rotate_left(consumed);
            // update buf_size
            buf_size -= consumed;

            let payload = match frame {
                Frame::Array(payload) => payload,
                _ => {
                    // anything else than an array is a protocol error
                    eprintln!("Got frame which is not an array, terminate connection");
                    return;
                }
            };

            // validation
            payload.iter().for_each(|f| match f {
                Frame::BulkString(_) => {}
                _ => {
                    eprintln!("Found message part which is not a bulk string");
                }
            });

            // dispacth command
            let resp_frame = match self.dispatch(payload).await {
                Ok(f) => f,
                Err(e) => {
                    eprintln!("Could not dispatch request: {:?}", e);
                    return;
                }
            };

            // response
            let size = match encode(&mut write_buf, &resp_frame) {
                Ok(n) => n,
                Err(e) => {
                    eprintln!("Could not encode response frame: {:?}", e);
                    return;
                }
            };

            if let Err(e) = self.transport.write_all(&write_buf[..size]).await {
                eprintln!("Could not write response: {:?}", e);
                return;
            };
        }
    }

    async fn dispatch(
        &mut self,
        mut payload: Vec<Frame>,
    ) -> Result<Frame, Box<dyn std::error::Error>> {
        if payload.is_empty() {
            return Err(Box::new(RzdbError::EmptyPayload));
        }

        let cmd = match &payload[0] {
            Frame::BulkString(cmdb) => String::from_utf8_lossy(&cmdb),
            _ => return Err(Box::new(RzdbError::IllegalType)),
        };

        match &*cmd {
            "PING" => {
                if payload.len() != 1 {
                    return Err(Box::new(RzdbError::WrongArgumentCount));
                }
                Ok(Frame::SimpleString("PONG".to_owned()))
            }
            "SET" => {
                if payload.len() != 3 {
                    return Err(Box::new(RzdbError::WrongArgumentCount));
                }
                let key = match payload.remove(1) {
                    Frame::BulkString(key) => key,
                    _ => return Err(Box::new(RzdbError::IllegalType)),
                };
                // since we removed the previous value this is now at index 1
                let data = match payload.remove(1) {
                    Frame::BulkString(data) => data,
                    _ => return Err(Box::new(RzdbError::IllegalType)),
                };
                // PROTOCOL MODIFICATION: return key set
                if let Err(e) = self.otx.send(DbOperation::Write(key.clone(), data)).await {
                    // Channel is full
                    eprintln!("DB insert error {:?}", e);
                    return Ok(Frame::Error(e.to_string()));
                };

                Ok(Frame::BulkString(key))
            }
            "GET" => {
                if payload.len() != 2 {
                    return Err(Box::new(RzdbError::WrongArgumentCount));
                }
                let key = match payload.remove(1) {
                    Frame::BulkString(key) => key,
                    _ => return Err(Box::new(RzdbError::IllegalType)),
                };
                if let Err(e) = self.otx.send(DbOperation::Read(key)).await {
                    eprintln!("DB get error {:?}", e);
                    return Ok(Frame::Error(e.to_string()));
                };

                match self.rrx.recv().await {
                    None => unreachable!(),
                    Some(r) => match r {
                        DbResult::None => Ok(Frame::Null),
                        DbResult::Read(d) => Ok(Frame::BulkString(d)),
                        _ => unreachable!(),
                    },
                }
            }
            _ => {
                eprintln!("Unknown command {}", cmd);
                Err(Box::new(RzdbError::UnknownCommand))
            }
        }
    }
}

struct DbManager {
    db: sled::Db,
    orx: mpsc::Receiver<DbOperation>,
    rtx: mpsc::Sender<DbResult>,

    // debug stoofs
    writes: usize,
    reads: usize,
}

impl DbManager {
    fn new(db: sled::Db, orx: mpsc::Receiver<DbOperation>, rtx: mpsc::Sender<DbResult>) -> Self {
        DbManager {
            db,
            orx,
            rtx,
            writes: 0,
            reads: 0,
        }
    }

    async fn run(mut self) {
        loop {
            // Get value from client
            // TODO: select over timer to flush write buffer in cases of inactivity
            let req = match self.orx.recv().await {
                Some(req) => req,
                // if we got none the sender is dropped, indicating the client disconnected
                None => return,
            };

            // Process request
            let resp = match req {
                DbOperation::Write(key, value) => {
                    self.writes += 1;
                    if let Err(e) = self.db.insert(key, value) {
                        eprintln!("Db write error {:?}", e);
                    };
                    // do not send a response on the channel
                    continue;
                }
                DbOperation::Read(key) => {
                    self.reads += 1;
                    match self.db.get(key) {
                        Err(e) => {
                            eprintln!("Db get error {:?}", e);
                            DbResult::Error(Box::new(e))
                        }
                        Ok(v) => match v {
                            None => DbResult::None,
                            Some(value) => DbResult::Read(value[..].into()),
                        },
                    }
                }
                DbOperation::Flush => unimplemented!(),
            };

            // Send response
            if let Err(e) = self.rtx.send(resp).await {
                eprintln!("Failed to send db response to client {:?}", e);
            };
        }
    }
}

impl Drop for DbManager {
    fn drop(&mut self) {
        eprintln!(
            "Dropping db thread, {} writes, {} reads",
            self.writes, self.reads
        );
        let _ = self.db.flush();
    }
}

/// DbOperation messages send from a client to a db processing thread.
#[derive(Debug)]
enum DbOperation {
    /// Write operation, with a key and value
    Write(Vec<u8>, Vec<u8>),
    /// Read operation, with a key
    Read(Vec<u8>),
    /// Flush operation, flushes the Db write buffer (if any), and explicitly flushes data to disk
    Flush,
}

/// DbResult messages send form a db processing thread back to the controlling client
#[derive(Debug)]
enum DbResult {
    // TODO
    // FIXME: add error??
    Write(Vec<u8>),
    Read(Vec<u8>),
    Error(Box<dyn std::error::Error + Send + Sync>),
    None,
}
