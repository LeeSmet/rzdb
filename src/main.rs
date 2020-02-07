use futures::select;
use redis_protocol::{decode::decode, encode::encode, types::Frame};
use sled::Batch;
use tokio::net::{TcpListener, UnixListener};
use tokio::prelude::*;
use tokio::sync::mpsc;

// TODO
const BUFFER_SIZE: usize = 9 * 1024;
const BATCH_SIZE: usize = 10;

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

    db.set_merge_operator(replace_merge);

    // let mut listener = TcpListener::bind("0.0.0.0:9900").await?;
    let mut unix_listener = UnixListener::bind("/tmp/rzdb.sock")?;

    loop {
        let (mut socket, _) = unix_listener.accept().await?;

        eprintln!("Accepted connection");

        let db = db.clone();

        tokio::spawn(async move {
            let mut read_buf = [0u8; BUFFER_SIZE];
            let mut write_buf = [0u8; BUFFER_SIZE];

            let mut buf_size = 0;

            loop {
                let n = match socket.read(&mut read_buf[buf_size..]).await {
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
                let resp_frame = match dispatch(&db, payload).await {
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

                if let Err(e) = socket.write_all(&write_buf[..size]).await {
                    eprintln!("Could not write response: {:?}", e);
                    return;
                };
            }
        });
    }
}

async fn dispatch(db: &sled::Db, payload: Vec<Frame>) -> Result<Frame, Box<dyn std::error::Error>> {
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
            let key = match &payload[1] {
                Frame::BulkString(key) => key,
                _ => return Err(Box::new(RzdbError::IllegalType)),
            };
            let data = match &payload[2] {
                Frame::BulkString(data) => data,
                _ => return Err(Box::new(RzdbError::IllegalType)),
            };
            // PROTOCOL MODIFICATION: return key set
            //let mut batch = Batch::default();
            //batch.remove(&key[..]);
            //batch.insert(&key[..], &data[..]);
            //if let Err(e) = db.apply_batch(batch) {
            if let Err(e) = db.insert(key, &data[..]) {
                eprintln!("DB insert error {:?}", e);
                return Ok(Frame::Error(e.to_string()));
            };

            Ok(payload[1].clone())
        }
        "GET" => {
            if payload.len() != 2 {
                return Err(Box::new(RzdbError::WrongArgumentCount));
            }
            let key = match &payload[1] {
                Frame::BulkString(key) => key,
                _ => return Err(Box::new(RzdbError::IllegalType)),
            };
            let data = match db.get(key) {
                Ok(d) => d,
                Err(e) => {
                    eprintln!("DB get error {:?}", e);
                    return Ok(Frame::Error(e.to_string()));
                }
            };

            match data {
                None => Ok(Frame::Null),
                Some(d) => Ok(Frame::BulkString(Vec::from(&*d))),
            }
        }
        _ => {
            eprintln!("Unknown command {}", cmd);
            Err(Box::new(RzdbError::UnknownCommand))
        }
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

fn replace_merge(_: &[u8], _: Option<&[u8]>, new: &[u8]) -> Option<Vec<u8>> {
    Some(new.into())
}

struct Client<T> {
    transport: T,
    otx: mpsc::Sender<DbOperation>,
    rrx: mpsc::Receiver<DbResult>,
}

struct DbManager {
    db: sled::Db,
    write_buf: [Option<DbOperation>; BATCH_SIZE],
    buf_size: usize,
    orx: mpsc::Receiver<DbOperation>,
    rtx: mpsc::Sender<DbResult>,
}

impl<T> Client<T>
where
    T: AsyncRead + AsyncWrite + Send + Sync,
{
    fn new(transport: T, db: sled::Db) -> Self {
        let (mut otx, mut orx) = mpsc::channel(1);
        let (mut rtx, mut rrx) = mpsc::channel(1);

        let dbm = DbManager {
            db,
            write_buf: [None, None, None, None, None, None, None, None, None, None], // TODO: ???
            buf_size: BATCH_SIZE,
            orx,
            rtx,
        };
        tokio::spawn(dbm.run());

        Client {
            transport,
            otx,
            rrx,
        }
    }

    async fn run(&self) {
        // set up read and write buffers
        let mut read_buf = [0u8; BUFFER_SIZE];
        let mut write_buf = [0u8; BUFFER_SIZE];

        let mut buf_size = 0;

        loop {}
    }
}

impl DbManager {
    async fn run(&self) {
        // TODO
        let buf = [None, None, None, None, None, None, None, None, None, None];
        let buf_size = 0;

        loop {
            // Get value from client
            // TODO: select over timer to flush write buffer in cases of inactivity
            let req = match self.orx.recv().await {
                Some(req) => req,
                // if we got none the sender is dropped, indicating the client disconnected
                None => return,
            };
            // Process request
            //
            // Send response
        }
    }
}

impl Drop for DbManager {
    fn drop(&mut self) {
        // TODO: flush write buffer
    }
}

/// DbOperation messages send from a client to a db processing thread.
enum DbOperation {
    /// Write operation, with a key and value
    Write(Vec<u8>, Vec<u8>),
    /// Read operation, with a key
    Read(Vec<u8>),
    /// Flush operation, flushes the Db write buffer (if any), and explicitly flushes data to disk
    Flush,
}

/// DbResult messages send form a db processing thread back to the controlling client
enum DbResult {
    // TODO
// FIXME: add error??
}
