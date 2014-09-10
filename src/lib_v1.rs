use std::{io, result, collections};

pub static CQL_VERSION:u8 = 0x01;

pub enum CqlError {
  ConnectionError(String)
}

pub enum Consistency {
  Any = 0x0000,
  One = 0x0001,
  Two = 0x0002,
  Three = 0x0003,
  Quorum = 0x0004,
  All = 0x0005,
  LocalQuorum = 0x0006,
  EachQuorum = 0x0007,
  Unknown,
}

trait Serializable {
  fn cql_len(&self) -> uint;
  fn serialize<T: io::Writer>(&self, buf: &mut T) -> io::IoResult<()>;
}

#[deriving(Show)]
enum Opcode {
  ErrorOpcode = 0x00,
  StartupOpcode = 0x01,
  ReadyOpcode = 0x02,
  AuthenticateOpcode = 0x03,
  CredentialsOpcode = 0x04,
  OptionsOpcode = 0x05,
  SupportedOpcode = 0x06,
  QueryOpcode = 0x07,
  ResultOpcode = 0x08,
  PrepareOpcode = 0x09,
  ExecuteOpcode = 0x0A,
  RegisterOpcode = 0x0B,
  EventOpcode = 0x0C,

  UnknownOpcode
}

fn opcode(val: u8) -> Opcode {
  match val {
    0x00 => ErrorOpcode,
    0x01 => StartupOpcode,
    0x02 => ReadyOpcode,
    0x03 => AuthenticateOpcode,
    0x04 => CredentialsOpcode,
    0x05 => OptionsOpcode,
    0x06 => SupportedOpcode,
    0x07 => QueryOpcode,
    0x08 => ResultOpcode,
    0x09 => PrepareOpcode,
    0x0A => ExecuteOpcode,
    0x0B => RegisterOpcode,
    0x0C => EventOpcode,

    _ => UnknownOpcode
  }
}
trait CqlReader {
  // fn read_cql_str(&self) -> String;
  // fn read_cql_long_str(&self) -> Option<String>;
  // fn read_cql_rows(&self) -> CqlRows;
  //
  // fn read_cql_metadata(&self) -> CqlMetadata;
  fn read_cql_response(&mut self) -> Msg;
}

impl<T: io::Reader> CqlReader for T {
  fn read_cql_response(&mut self) -> Msg {
    let version = self.read_u8().unwrap();
    let flags = self.read_u8().unwrap();
    let stream = self.read_i8().unwrap();
    let opcode = opcode(self.read_u8().unwrap());
    let length = self.read_be_u32().unwrap();

    let body_data = self.read_exact(length as uint);
    // let reader = io_util::BufReader::new(body_data);

    Msg {
      version: version,
      flags: flags,
      stream: stream,
      opcode: opcode,
      body: ResponseEmpty
    }
  }
}


pub type CqlHashMap = collections::HashMap<String, String>;

impl Serializable for CqlHashMap {
  fn serialize<T: io::Writer>(&self, buf: &mut T) -> io::IoResult<()>{
    try!(buf.write_be_u16(self.len() as u16));
    for (key, val) in self.iter() {
      try!(buf.write_be_u16(key.len() as u16));
      try!(buf.write_str(key.as_slice()));
      try!(buf.write_be_u16(val.len() as u16));
      try!(buf.write_str(val.as_slice()));
    }
    Ok(())
  }
  fn cql_len(&self) -> uint {
    let mut total = 2u;
    for (key, val) in self.iter() {
      total += key.len() + val.len() + 4;
    }
    total
  }
}

#[test]
fn test_cql_hash_map_len() {
  let mut hm: CqlHashMap = collections::HashMap::new();
  hm.insert("CQL_VERSION".to_string(), "3.0.0".to_string());

  assert_eq!(hm.cql_len(), 22);
}

#[test]
fn test_cql_hash_map_serialize() {
  let mut w = io::MemWriter::new();
  let mut hm: CqlHashMap = collections::HashMap::new();
  hm.insert("CQL_VERSION".to_string(), "3.0.0".to_string());

  let vec_form = vec!(
    0, 1,
    0, 11, 67, 81, 76, 95, 86, 69, 82, 83, 73, 79, 78,
    0, 5, 51, 46, 48, 46, 48
    );
  hm.serialize(&mut w);
  assert_eq!(w.unwrap(), vec_form);
}

pub enum Body {
  //Requests
  StartupRequest(CqlHashMap),
  CredentialRequest(Vec<String>),
  QueryRequest(String, Consistency),
  OptionsRequest,

  //Responses
  ResponseError(u32, String),
  ResponseReady,
  ResponseAuth(String),

  ResultVoid,
  // ResultRows(CqlRows),
  ResultKeyspace(String),
  // ResultPrepared(u8, CqlMetadata),
  ResultSchemaChange(String, String, String),
  ResultUnknown,

  ResponseEmpty,
}

struct Msg {
  version: u8,
  flags: u8,
  stream: i8,
  opcode: Opcode,
  body: Body,
}

impl Serializable for Msg {
  fn serialize<T: io::Writer>(&self, buf: &mut T) -> io::IoResult<()> {
    buf.write_u8(self.version);
    buf.write_u8(self.flags);
    buf.write_i8(self.stream);
    buf.write_u8(self.opcode as u8);
    buf.write_be_u32((self.cql_len()-8) as u32);

    match self.body {
      StartupRequest(ref map) => {
        map.serialize(buf);
      },
      _ => ()
    }
    Ok(())
  }
  fn cql_len(&self) -> uint {
    8 + match self.body {
      StartupRequest(ref map) => {
        map.cql_len()
      }
      _ => {
        0
      }
    }
  }
}

#[test]
fn test_request_len_for_options() {
  let req = Msg {
    version: CQL_VERSION,
    flags: 0x00,
    stream: 0x01,
    opcode: OptionsOpcode,
    body: OptionsRequest
  };
  assert_eq!(req.cql_len(), 8);
}

#[test]
fn test_request_len_for_startup() {
  let mut body = collections::HashMap::new();
  body.insert("CQL_VERSION".to_string(), "3.0.0".to_string());

  let req = Msg {
    version: CQL_VERSION,
    flags: 0x00,
    stream: 0x01,
    opcode: StartupOpcode,
    body: StartupRequest(body)
  };
  assert_eq!(req.cql_len(), 30);
}

#[test]
fn test_request_serialize_for_startup() {
  let mut w = io::MemWriter::new();
  let mut body = collections::HashMap::new();
  body.insert("CQL_VERSION".to_string(), "3.0.0".to_string());

  let req = Msg {
    version: CQL_VERSION,
    flags: 0x00,
    stream: 0x01,
    opcode: StartupOpcode,
    body: StartupRequest(body)
  };

  let vec_form = vec!(
    1, 0, 1, 1, 0, 0, 0, 22, //HEADER PART
    0, 1, //Startup request hash serialized
    0, 11, 67, 81, 76, 95, 86, 69, 82, 83, 73, 79, 78,
    0, 5, 51, 46, 48, 46, 48
    );
  let res = req.serialize(&mut w);
  assert_eq!(w.unwrap(), vec_form);
}

fn startup_request() -> Msg {
  let mut body = collections::HashMap::new();
  body.insert("CQL_VERSION".to_string(), "3.0.0".to_string());

  return Msg {
    version: CQL_VERSION,
    flags: 0x00,
    stream: 0x01,
    opcode: StartupOpcode,
    body: StartupRequest(body)
  }
}

pub struct CqlClient {
  buf: io::BufferedStream<io::TcpStream>
}


pub fn connect(ip: String, port: u16) -> result::Result<CqlClient, CqlError> {

  let res = io::TcpStream::connect(ip.as_slice(), port);

  match res {
    result::Ok(stream) => {
      let startup_msg = startup_request();
      println!("Hello World!");
      let mut buf = io::BufferedStream::new(stream);
      startup_msg.serialize(&mut buf);
      let flushed = buf.flush();

      let msg = buf.read_cql_response();
      println!("opcode was {}", msg.opcode);
      result::Ok(CqlClient { buf: buf })
    },
    result::Err(_) => {
      println!("totally failed");
      result::Err(ConnectionError("Could not connect to IP".to_string()))
    }
  }
}


#[test]
fn run_some_shit() {
  let connection = connect("127.0.0.1".to_string(), 9042);
  match connection {
    result::Ok(client) => {
      assert!(true)
    },
    result::Err(e) => {
      fail!(e)
    }
  }
}
