use std::str;
use crate::protocol::RespType;


#[derive(Debug)]
pub enum ParserError {
    // Input-level issues
    EmptyInput,
    IncompleteInput,
    UnknownType,           // first byte isn't +, -, :, $, *, etc.

    // Formatting
    InvalidTerminator,     // missing or malformed \r\n
    InvalidUtf8,           // bytes aren't valid UTF-8 where expected

    // Integer reply (:)
    InvalidInteger,        // couldn't parse as i64

    // Bulk string ($)
    InvalidBulkStringLength,   // non-numeric length, or length < -1
    BulkStringLengthMismatch,  // declared length doesn't match actual bytes

    // Array (*)
    InvalidArrayLength,        // non-numeric, or < -1
    NestedParseError,          // optional: wrap errors from recursive calls

    // Simple string (+) and Error (-)
    InvalidSimpleString,       // contains \r or \n in the payload

    // Protocol limits (Redis enforces these)
    BulkStringTooLarge,        // Redis caps at 512 MB
    ArrayTooLarge,             // Redis caps at 1024*1024 elements by default
}

fn find_crlf(input: &[u8], start: usize) -> Result<usize, ParserError> {
    if start >= input.len() {
        return Err(ParserError::IncompleteInput);
    }

    let mut i = start;
    while i + 1 < input.len() {
        if input[i] == b'\r' && input[i + 1] == b'\n' {
            return Ok(i);
        }
        i += 1;
    }

    Err(ParserError::IncompleteInput)
}



fn array_type_parser( resp_as_bytes: &[u8], cursor: usize,) -> Result<(RespType, usize), ParserError> {
    let resp_prefix_type = match resp_as_bytes.get(cursor) {
        Some(b) => *b,
        None => return Err(ParserError::IncompleteInput),
    };

    if resp_prefix_type != b'*' {
        return Err(ParserError::UnknownType);
    }

    let start = cursor + 1;
    let prefix_line_end = find_crlf(resp_as_bytes, start)?;
    let raw_element_count = &resp_as_bytes[start..prefix_line_end];
    let body_start = prefix_line_end + 2;

    let element_count_str =
        std::str::from_utf8(raw_element_count).map_err(|_| ParserError::InvalidUtf8)?;
    let element_count: usize = element_count_str
        .parse()
        .map_err(|_| ParserError::InvalidArrayLength)?;

    let mut resp_elements = Vec::with_capacity(element_count);
    let mut updated_cursor = body_start;

    for _ in 0..element_count {
        let (element, next_cursor) = resp_protocol_parser(resp_as_bytes, updated_cursor)?;
        resp_elements.push(element);
        updated_cursor = next_cursor;
    }

    Ok((RespType::Array(resp_elements), updated_cursor))
}


fn simple_string_type_parser(resp_as_bytes: &[u8], cursor: usize,) -> Result<(RespType, usize), ParserError> {
    let resp_prefix_type = match resp_as_bytes.get(cursor) {
        Some(b) => *b,
        None => return Err(ParserError::IncompleteInput),
    };

    if resp_prefix_type != b'+' {
        return Err(ParserError::UnknownType);
    }

    let start = cursor + 1;
    let prefix_line_end = find_crlf(resp_as_bytes, start)?;
    let simple_string_element_bytes = &resp_as_bytes[start..prefix_line_end];

    let simple_string_element =  str::from_utf8(simple_string_element_bytes).map_err(|_| ParserError::InvalidUtf8)?;

    let simple_string = simple_string_element.to_string();


    let updated_cursor = prefix_line_end+2;


    Ok((RespType::SimpleString(simple_string), updated_cursor))
}


fn error_type_parser(resp_as_bytes: &[u8], cursor: usize,) -> Result<(RespType, usize), ParserError> {
    let resp_prefix_type = match resp_as_bytes.get(cursor) {
        Some(b) => *b,
        None => return Err(ParserError::IncompleteInput),
    };

    if resp_prefix_type != b'-' {
        return Err(ParserError::UnknownType);
    }

    let start = cursor + 1;
    let prefix_line_end = find_crlf(resp_as_bytes, start)?;
    let error_message_bytes = &resp_as_bytes[start..prefix_line_end];

    let error_message_element =  str::from_utf8(error_message_bytes).map_err(|_| ParserError::InvalidUtf8)?;

    let error_message = error_message_element.to_string();


    let updated_cursor = prefix_line_end + 2;


    Ok((RespType::Error(error_message), updated_cursor))
}


fn integer_type_parser(resp_as_bytes: &[u8], cursor: usize,) -> Result<(RespType, usize), ParserError> {
    let resp_prefix_type = match resp_as_bytes.get(cursor) {
        Some(b) => *b,
        None => return Err(ParserError::IncompleteInput),
    };

    if resp_prefix_type != b':' {
        return Err(ParserError::UnknownType);
    }

    let start = cursor + 1;
    let prefix_line_end = find_crlf(resp_as_bytes, start)?;
    let integer_bytes =  &resp_as_bytes[start..prefix_line_end];

    let int_element_str =  str::from_utf8(integer_bytes).map_err(|_| ParserError::InvalidUtf8)?;

    let int_elements : i64 = int_element_str.parse().map_err(|_| ParserError::InvalidInteger)?;


    let updated_cursor = prefix_line_end + 2;



    Ok((RespType::Integer(int_elements), updated_cursor))
}


fn bulk_string_type_parser( resp_as_bytes: &[u8], cursor: usize,) -> Result<(RespType, usize), ParserError> {
    let resp_prefix_type = match resp_as_bytes.get(cursor) {
        Some(b) => *b,
        None => return Err(ParserError::IncompleteInput),
    };

    if resp_prefix_type != b'$' {
        return Err(ParserError::UnknownType);
    }

    let start = cursor + 1;
    let prefix_line_end = find_crlf(resp_as_bytes, start)?;

    let raw_bulk_string_len_count = &resp_as_bytes[start..prefix_line_end];
    let bulk_string_len_count_str = str::from_utf8(raw_bulk_string_len_count).map_err(|_| ParserError::InvalidUtf8)?;
    let bulk_string_len_int : i64 = bulk_string_len_count_str.parse().map_err(|_| ParserError::InvalidBulkStringLength)?;


    let bulk_string_body_start = prefix_line_end + 2;


   if bulk_string_len_int == -1 {
    // null bulk string → no body
    let updated_cursor = prefix_line_end + 2;

    return Ok((RespType::BulkString(None), updated_cursor));
    }

    if bulk_string_len_int < -1 {
        return Err(ParserError::InvalidBulkStringLength);
    }


    let len = bulk_string_len_int as usize;
    let bulkstring_body_end = bulk_string_body_start + len;


    if bulkstring_body_end > resp_as_bytes.len(){

        return Err(ParserError::IncompleteInput);

    }

    if bulkstring_body_end + 2 > resp_as_bytes.len(){

        return Err(ParserError::IncompleteInput);

    }

    if resp_as_bytes[bulkstring_body_end] != b'\r' || resp_as_bytes[bulkstring_body_end + 1] !=b'\n'{
        return Err(ParserError::InvalidTerminator)
    }

    let bulkstring_body = resp_as_bytes[bulk_string_body_start..bulkstring_body_end].to_vec();
    let updated_cursor = bulkstring_body_end + 2;



    Ok((RespType::BulkString(Some(bulkstring_body)), updated_cursor))
}


pub fn resp_protocol_parser(
    resp_as_bytes: &[u8],
    cursor: usize,) -> Result<(RespType, usize), ParserError> {
    let resp_prefix = match resp_as_bytes.get(cursor) {
        Some(b) => *b,
        None => return Err(ParserError::IncompleteInput),
    };

    match resp_prefix {
        b'+' => simple_string_type_parser(resp_as_bytes, cursor),
        b'-' => error_type_parser(resp_as_bytes, cursor),
        b':' => integer_type_parser(resp_as_bytes, cursor),
        b'$' => bulk_string_type_parser(resp_as_bytes, cursor),
        b'*' => array_type_parser(resp_as_bytes, cursor),
        _ => Err(ParserError::UnknownType),
    }
}
