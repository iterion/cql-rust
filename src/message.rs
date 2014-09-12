use std::collections;
use std::io::{Writer, MemWriter, MemReader, IoResult};
use std::result::{Ok, Err};

pub static CQL_VERSION:u8 = 0x02;

pub enum Consistency {
  Any = 0x0000,
  One = 0x0001,
  Two = 0x0002,
  Three = 0x0003,
  Quorum = 0x0004,
  All = 0x0005,
  LocalQuorum = 0x0006,
  EachQuorum = 0x0007,
  UnknownConsistency,
}



pub type CqlHashMap = collections::HashMap<String, String>;

pub enum Request {
  Startup(CqlHashMap),
  Credential(Vec<String>), // not used in v2
  Options,
  Query(String, Consistency)
}

impl Request {
  pub fn opcode(&self) -> u8 {
    match *self {
      Startup(_) => 0x01,
      Credential(_) => 0x04,
      Options => 0x05,
      Query(_, _) => 0x07
    }
  }
}

#[deriving(Show)]
pub enum Response {
  Error(u32, String),
  Ready,
  Supported,
  Result(ResultBody),
  Authenticate(String),
  Unknown,
  Empty
}

#[deriving(Show)]
pub enum ResultBody {
  Void,
  Rows,
  SetKeyspace(String),
  Prepared,
  SchemaChange(String, String, String)
}

#[doc(hidden)]
pub trait WriteMessage {
  fn write_message(&mut self, &Request) -> IoResult<()>;
}

impl<W: Writer> WriteMessage for W {
  fn write_message(&mut self, message: &Request) -> IoResult<()> {
    let mut header = MemWriter::new();

    try!(header.write_u8(CQL_VERSION));
    try!(header.write_u8(0x00));
    try!(header.write_i8(1));
    try!(header.write_u8(message.opcode()));

    let mut buf = MemWriter::new();

    match *message {
      Startup(ref hash_map) => {
        // try!(body.write(hash_map.as_cql_binary()));
        try!(buf.write_be_u16(hash_map.len() as u16));
        for (key, val) in hash_map.iter() {
          try!(buf.write_be_u16(key.len() as u16));
          try!(buf.write_str(key.as_slice()));
          try!(buf.write_be_u16(val.len() as u16));
          try!(buf.write_str(val.as_slice()));
        }
      }
      Query(ref query, consistency) => {
        try!(buf.write_be_u32(query.len() as u32));
        try!(buf.write_str(query.as_slice()));
        try!(buf.write_be_u16(consistency as u16));
        try!(buf.write_u8(0u as u8));
      }
      _ => ()
    }

    let header = header.unwrap();
    let buf = buf.unwrap();

    try!(self.write(header.as_slice()));
    try!(self.write_be_u32(buf.len() as u32));
    try!(self.write(buf.as_slice()));

    Ok(())
  }
}

#[doc(hidden)]
pub trait ReadMessage {
  fn read_message(&mut self) -> IoResult<Response>;
}

impl<R: Reader> ReadMessage for R {
  fn read_message(&mut self) -> IoResult<Response> {
    let _version = try!(self.read_u8());
    let _flags = try!(self.read_u8());
    let _stream = try!(self.read_i8());
    let opcode = try!(self.read_u8());
    let len = try!(self.read_be_u32());

    let mut buf = MemReader::new(try!(self.read_exact(len as uint)));

    let ret = match opcode {
      0 => try!(read_error_response(&mut buf)),
      2 => Ready,
      3 => Authenticate("test".to_string()),
      6 => Supported,
      8 => try!(read_result(&mut buf)),
      _ => Empty
    };

    Ok(ret)
  }
}
//   PrepareOpcode = 0x09,
//   ExecuteOpcode = 0x0A,
//   RegisterOpcode = 0x0B,
//   EventOpcode = 0x0C,
//   BatchOpcode = 0x0D,
//   AuthChallengeOpcode = 0x0E,
//   AuthResponseOpcode = 0x0F,
//   AuthSuccessOpcode = 0x10,
//
//   UnknownOpcode

fn read_error_response(buf: &mut MemReader) -> IoResult<Response> {
  let code = try!(buf.read_be_u32());
  let len = try!(buf.read_be_u16());
  let string_bytes = try!(buf.read_exact(len as uint));
  let res = String::from_utf8(string_bytes);
  Ok(match res {
    Ok(string) => Error(code, string),
    Err(_) => Error(code, "couldn't parse".to_string())
  })
}

fn read_result(buf: &mut MemReader) -> IoResult<Response> {
  let result_type = try!(buf.read_be_u32());

  let body = match result_type {
    2 => {
      Rows
    },
    3 => {
      let len = try!(buf.read_be_u16());
      let string_bytes = try!(buf.read_exact(len as uint));
      let name = String::from_utf8(string_bytes).unwrap();
      SetKeyspace(name)
    },
    4 => Prepared,
    5 => {
      // dedup this - map over range?
      let len = try!(buf.read_be_u16());
      let string_bytes = try!(buf.read_exact(len as uint));
      let change = String::from_utf8(string_bytes).unwrap();

      let len = try!(buf.read_be_u16());
      let string_bytes = try!(buf.read_exact(len as uint));
      let keyspace = String::from_utf8(string_bytes).unwrap();

      let len = try!(buf.read_be_u16());
      let string_bytes = try!(buf.read_exact(len as uint));
      let table = String::from_utf8(string_bytes).unwrap();
      SchemaChange(change, keyspace, table)
    },
    _ => Void,
  };

  Ok(Result(body))
}
