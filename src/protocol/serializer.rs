use crate::protocol::RespType;

fn append_len(buf: &mut Vec<u8>, n: usize) {
    buf.extend_from_slice(n.to_string().as_bytes());
}

fn append_crlf(buf: &mut Vec<u8>) {
    buf.extend_from_slice(b"\r\n");
}

fn serialize_bulk_string(out: &mut Vec<u8>, data: Option<&[u8]>) {
    match data {
        Some(bytes) => {
            out.push(b'$');
            append_len(out, bytes.len());
            append_crlf(out);
            out.extend_from_slice(bytes);
            append_crlf(out);
        }
        None => out.extend_from_slice(b"$-1\r\n"),
    }
}

pub fn serialize_resp(out: &mut Vec<u8>, resp: &RespType) {
    match resp {
        RespType::SimpleString(s) => {
            out.push(b'+');
            out.extend_from_slice(s.as_bytes());
            append_crlf(out);
        }
        RespType::Error(e) => {
            out.push(b'-');
            out.extend_from_slice(e.as_bytes());
            append_crlf(out);
        }
        RespType::Integer(i) => {
            out.push(b':');
            out.extend_from_slice(i.to_string().as_bytes());
            append_crlf(out);
        }
        RespType::BulkString(opt) => {
            serialize_bulk_string(out, opt.as_deref());
        }
        RespType::Array(elements) => {
            out.push(b'*');
            append_len(out, elements.len());
            append_crlf(out);
            for element in elements {
                serialize_resp(out, element);
            }
        }
    }
}