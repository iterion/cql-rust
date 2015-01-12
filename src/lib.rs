use std::{io, collections};
use std::result::Result;
use message::{WriteMessage,
              ReadMessage,
              Request,
              Consistency,
              Response};
use std::io::{IoResult};

pub mod message;

fn startup_request() -> Request {
  let mut body = collections::HashMap::new();
  body.insert("CQL_VERSION".to_string(), "3.0.0".to_string());

  Request::Startup(body)
}

pub struct Client {
  buf: io::BufferedStream<io::TcpStream>
}

impl Client {
  pub fn query(&mut self, query: String, consistency: message::Consistency) -> IoResult<Response> {
    let query = Request::Query(query, consistency);
    try!(self.buf.write_message(&query));
    try!(self.buf.flush());

    Ok(try!(self.buf.read_message()))
  }
}


pub fn connect(addr: String) -> io::IoResult<Client> {

  let stream = try!(io::TcpStream::connect(addr.as_slice()));

  let startup_msg = startup_request();
  let mut buf = io::BufferedStream::new(stream);
  try!(buf.write_message(&startup_msg));
  try!(buf.flush());

  let msg = try!(buf.read_message());
  match msg {
    Response::Ready => {
      println!("No auth required by server - moving on");
      let cli = Client { buf: buf };
      Result::Ok(cli)
    },
    Response::Authenticate(_) => {
      println!("Auth required - sending credentials - maybe");
      let cli = Client { buf: buf };
      Result::Ok(cli)
    },
    _ => {
      println!("Bad response - response was {:?}", msg);
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
  let mut client = connect("127.0.0.1:9042".to_string()).unwrap();

  let result = client.query("DROP KEYSPACE IF EXISTS testing".to_string(), Consistency::Quorum);
  println!("Result of DROP KEYSPACE was {}", result);

  let query = "CREATE KEYSPACE testing
               WITH replication = {
                 'class' : 'SimpleStrategy',
                 'replication_factor' : 1
               }".to_string();
  let result = client.query(query, Consistency::Quorum);
  println!("Result of CREATE KEYSPACE was {}", result);

  let result = client.query("USE testing".to_string(), Consistency::Quorum);
  println!("Result of USE was {}", result);

  let query = "CREATE TABLE users (
    user_id varchar PRIMARY KEY,
    first varchar,
    last varchar,
    age int,
    height float
    )".to_string();

  let result = client.query(query, Consistency::Quorum);
  println!("Result of CREATE TABLE was {}", result);

  let query = "INSERT INTO users (user_id, first, last, age, height)
               VALUES ('jsmith', 'John', 'Smith', 42, 12.1);".to_string();
  let result = client.query(query, Consistency::Quorum);
  println!("Result of INSERT was {}", result);

  let result = client.query("SELECT * FROM users".to_string(), Consistency::Quorum);
  println!("Result of SELECT was {}", result);
}
