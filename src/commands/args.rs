use crate::protocol::RespType;

/// Extract a bulk string argument, returning its bytes.
pub(crate) fn expect_bulk_string(arg: &RespType) -> Option<&[u8]> {
    match arg {
        RespType::BulkString(Some(bytes)) => Some(bytes.as_slice()),
        _ => None,
    }
}

/// Parse an integer argument from a bulk string.
pub(crate) fn parse_i64(arg: &RespType) -> Option<i64> {
    let bytes = expect_bulk_string(arg)?;
    let s = std::str::from_utf8(bytes).ok()?;
    s.parse::<i64>().ok()
}

/// Parse an unsigned integer argument from a bulk string.
pub(crate) fn parse_u64(arg: &RespType) -> Option<u64> {
    let bytes = expect_bulk_string(arg)?;
    let s = std::str::from_utf8(bytes).ok()?;
    s.parse::<u64>().ok()
}

/// Parse a float argument from a bulk string.
pub(crate) fn parse_f64(arg: &RespType) -> Option<f64> {
    let bytes = expect_bulk_string(arg)?;
    let s = std::str::from_utf8(bytes).ok()?;
    s.parse::<f64>().ok()
}