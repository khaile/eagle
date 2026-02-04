use bytes::Bytes;
use eagle_core::resp::{RespValue, encoder::encode, parser::RespParser};
use std::io::Cursor;
use tokio::io::BufReader;

#[tokio::test]
async fn test_full_roundtrip() -> Result<(), Box<dyn std::error::Error>> {
    let test_cases = vec![
        RespValue::SimpleString("OK".into()),
        RespValue::Error("ERR unknown command".into()),
        RespValue::Integer(42),
        RespValue::BulkString(Bytes::from_static(b"Hello\r\nWorld")),
        RespValue::NullBulkString,
        RespValue::Array(
            vec![
                RespValue::Integer(1),
                RespValue::BulkString(Bytes::from_static(b"foo")),
            ]
            .into(),
        ),
        RespValue::NullArray,
    ];

    for case in test_cases {
        let encoded = encode(&case)?;
        let cursor = Cursor::new(encoded);
        let mut parser = RespParser::new(BufReader::new(cursor));
        let parsed = parser.parse().await?;
        assert_eq!(case, parsed);
    }
    Ok(())
}

#[tokio::test]
async fn test_array_parsing() -> Result<(), Box<dyn std::error::Error>> {
    let input = b"*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
    let cursor = Cursor::new(input.as_slice());
    let mut parser = RespParser::new(BufReader::new(cursor));
    assert_eq!(
        parser.parse().await?,
        RespValue::Array(
            vec![
                RespValue::BulkString(Bytes::from_static(b"foo")),
                RespValue::BulkString(Bytes::from_static(b"bar")),
            ]
            .into()
        )
    );
    Ok(())
}
