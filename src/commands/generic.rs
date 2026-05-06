use crate::protocol::RespType;
use crate::protocol::serializer::serialize_resp;
use crate::storage::Store;

use super::args::expect_bulk_string;

pub fn handle_type(out: &mut Vec<u8>, args: &[RespType], store: &Store) {
    let [arg] = args else {
        serialize_resp(
            out,
            &RespType::Error("ERR wrong number of arguments for 'type' command".to_string()),
        );
        return;
    };

    let Some(key) = expect_bulk_string(arg) else {
        serialize_resp(out, &RespType::Error("ERR invalid key".to_string()));
        return;
    };

    let type_string = store.type_of(key);
    serialize_resp(out, &RespType::SimpleString(type_string.to_string()));
}