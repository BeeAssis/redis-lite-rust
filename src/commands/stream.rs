use crate::protocol::RespType;
use crate::protocol::serializer::serialize_resp;
use crate::storage::{EntryId, Store, XaddId};

use super::args::expect_bulk_string;
use super::serialize_store_error;

pub fn handle_xadd(out: &mut Vec<u8>, args: &[RespType], store: &Store) {
    if args.len() < 4 || args.len() % 2 != 0 {
        serialize_resp(
            out,
            &RespType::Error("ERR wrong number of arguments for 'xadd' command".to_string()),
        );
        return;
    }

    let Some(key) = expect_bulk_string(&args[0]) else {
        serialize_resp(out, &RespType::Error("ERR invalid key".to_string()));
        return;
    };

    let id_bytes = match expect_bulk_string(&args[1]) {
        Some(b) => b,
        None => {
            serialize_resp(out, &RespType::Error("ERR invalid stream ID".to_string()));
            return;
        }
    };

    let id_str = match std::str::from_utf8(id_bytes) {
        Ok(s) => s,
        Err(_) => {
            serialize_resp(out, &RespType::Error("ERR invalid stream ID".to_string()));
            return;
        }
    };

    let xadd_req = match parse_xadd_id(id_str) {
        Ok(req) => req,
        Err(msg) => {
            serialize_resp(out, &RespType::Error(msg));
            return;
        }
    };

    let mut fields = Vec::new();
    for chunk in args[2..].chunks(2) {
        let (Some(k), Some(v)) = (expect_bulk_string(&chunk[0]), expect_bulk_string(&chunk[1])) else {
            serialize_resp(out, &RespType::Error("ERR invalid field/value pair".to_string()));
            return;
        };
        fields.push((k.to_vec(), v.to_vec()));
    }

    match store.xadd(key.to_vec(), xadd_req, fields) {
        Ok(final_id) => {
            let response_str = format!("{}-{}", final_id.ms, final_id.seq);
            serialize_resp(out, &RespType::BulkString(Some(response_str.into_bytes())));
        }
        Err(e) => serialize_store_error(out, e),
    }
}

fn parse_xadd_id(id_str: &str) -> Result<XaddId, String> {
    if id_str == "*" {
        return Ok(XaddId::FullAuto);
    }

    if let Some(ms_part) = id_str.strip_suffix("-*") {
        let ms = ms_part
            .parse::<u64>()
            .map_err(|_| "ERR invalid stream ID".to_string())?;
        return Ok(XaddId::PartialAuto(ms));
    }

    let mut parts = id_str.split('-');
    let ms = parts
        .next()
        .ok_or_else(|| "ERR invalid stream ID".to_string())?
        .parse::<u64>()
        .map_err(|_| "ERR invalid stream ID".to_string())?;
    let seq = parts
        .next()
        .ok_or_else(|| "ERR invalid stream ID".to_string())?
        .parse::<u64>()
        .map_err(|_| "ERR invalid stream ID".to_string())?;

    if parts.next().is_some() {
        return Err("ERR invalid stream ID".to_string());
    }

    Ok(XaddId::Explicit(EntryId { ms, seq }))
}