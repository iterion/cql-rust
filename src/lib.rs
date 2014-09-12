use std::{io, result, collections};
use message::{WriteMessage,
              ReadMessage,
              Startup,
              Request,
              Response,
              Query
              };
use std::io::{IoResult};

pub mod message;

fn startup_request() -> Request {
  let mut body = collections::HashMap::new();
  body.insert("CQL_VERSION".to_string(), "3.0.0".to_string());

  Startup(body)
}

pub struct Client {
  buf: io::BufferedStream<io::TcpStream>
}

impl Client {
  pub fn query(&mut self, query: String, consistency: message::Consistency) -> IoResult<Response> {
    let query = Query(query, consistency);
    try!(self.buf.write_message(&query));
    try!(self.buf.flush());

    Ok(try!(self.buf.read_message()))
  }
}


pub fn connect(ip: String, port: u16) -> io::IoResult<Client> {

  let stream = try!(io::TcpStream::connect(ip.as_slice(), port));

  let startup_msg = startup_request();
  let mut buf = io::BufferedStream::new(stream);
  try!(buf.write_message(&startup_msg));
  try!(buf.flush());

  let msg = buf.read_message().unwrap();
  match msg {
    message::Ready => {
      println!("No auth required by server - moving on");
      let cli = Client { buf: buf };
      result::Ok(cli)
    },
    message::Authenticate(_) => {
      println!("Auth required - sending credentials - maybe");
      let cli = Client { buf: buf };
      result::Ok(cli)
    },
    _ => {
      println!("Bad response - response was {}", msg);
      Err(io::IoError {
        kind: io::ConnectionFailed,
        desc: "Invalid response after startup",
        detail: None
      })
    }
  }
}


#[test]
fn connect_and_query() {
  let mut client = connect("127.0.0.1".to_string(), 9042).unwrap();

  let result = client.query("DROP KEYSPACE IF EXISTS testing".to_string(), message::Quorum);
  println!("Result of DROP KEYSPACE was {}", result);

  let query = "CREATE KEYSPACE testing
               WITH replication = {
                 'class' : 'SimpleStrategy',
                 'replication_factor' : 1
               }".to_string();
  let result = client.query(query, message::Quorum);
  println!("Result of CREATE KEYSPACE was {}", result);

  let result = client.query("USE testing".to_string(), message::Quorum);
  println!("Result of USE was {}", result);

  let query = "CREATE TABLE users (
    user_id varchar PRIMARY KEY,
    first varchar,
    last varchar,
    age int
    )".to_string();

  let result = client.query(query, message::Quorum);
  println!("Result of CREATE TABLE was {}", result);

  let query = "INSERT INTO users (user_id, first, last, age)
               VALUES ('jsmith', 'John', 'Smith', 42);".to_string();
  let result = client.query(query, message::Quorum);
  println!("Result of INSERT was {}", result);

  let result = client.query("SELECT * FROM users".to_string(), message::Quorum);
  println!("Result of SELECT was {}", result);
}
