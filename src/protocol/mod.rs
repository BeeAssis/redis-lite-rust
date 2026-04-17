pub mod parser;
pub mod serializer;

#[derive(Debug)]
pub enum RespType{
    SimpleString(String),
    Error(String),
    Integer(i64),
    BulkString(Option<Vec<u8>>), 
    Array(Vec<RespType>),
}

