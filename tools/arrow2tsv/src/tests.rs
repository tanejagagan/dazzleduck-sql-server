use std::sync::Arc;

use arrow_array::builder::{Int32Builder, ListBuilder, MapBuilder, StringBuilder};
use arrow_array::types::{Float64Type, Int32Type};
use arrow_array::{
    ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, FixedSizeListArray,
    Float64Array, Int32Array, ListArray, RecordBatch, StringArray, StructArray,
    Time64MicrosecondArray, TimestampMicrosecondArray,
};
use arrow_buffer::NullBuffer;
use arrow_ipc::CompressionType;
use arrow_ipc::writer::{IpcWriteOptions, StreamWriter};
use arrow_schema::{DataType, Field, Fields, Schema};

use super::*;

/// 2024-01-01 as days / microseconds since the epoch.
const DAY: i32 = 19723;
const MICROS: i64 = DAY as i64 * 86_400_000_000;

fn ipc(batches: &[RecordBatch], compression: Option<CompressionType>) -> Vec<u8> {
    let options = IpcWriteOptions::default()
        .try_with_compression(compression)
        .unwrap();
    let mut buf = Vec::new();
    let mut writer =
        StreamWriter::try_new_with_options(&mut buf, &batches[0].schema(), options).unwrap();
    for b in batches {
        writer.write(b).unwrap();
    }
    writer.finish().unwrap();
    drop(writer);
    buf
}

fn tsv_with(bytes: &[u8], header_row: bool, escape: bool) -> String {
    let mut out = Vec::new();
    convert(bytes, &mut out, header_row, escape).unwrap();
    String::from_utf8(out).unwrap()
}

fn tsv(batch: RecordBatch) -> String {
    tsv_with(&ipc(&[batch], None), true, true)
}

/// Unescaped (`--raw`), for tests about the JSON itself.
fn tsv_raw(batch: RecordBatch) -> String {
    tsv_with(&ipc(&[batch], None), true, false)
}

fn batch(columns: Vec<(&str, ArrayRef)>) -> RecordBatch {
    RecordBatch::try_from_iter(columns).unwrap()
}

#[test]
fn scalars_render_like_the_server() {
    let b = batch(vec![
        (
            "i",
            Arc::new(Int32Array::from(vec![Some(1), None])) as ArrayRef,
        ),
        ("s", Arc::new(StringArray::from(vec![Some("plain"), None]))),
        ("d", Arc::new(Date32Array::from(vec![Some(DAY), None]))),
        (
            "ts",
            Arc::new(TimestampMicrosecondArray::from(vec![Some(MICROS), None])),
        ),
        (
            "tz",
            Arc::new(
                TimestampMicrosecondArray::from(vec![Some(MICROS), None]).with_timezone("Etc/UTC"),
            ),
        ),
        (
            "t",
            Arc::new(Time64MicrosecondArray::from(vec![
                Some(12 * 3_600_000_000),
                None,
            ])),
        ),
        (
            "dc",
            Arc::new(
                Decimal128Array::from(vec![Some(314), None])
                    .with_precision_and_scale(5, 2)
                    .unwrap(),
            ),
        ),
        ("f", Arc::new(Float64Array::from(vec![Some(1.5), None]))),
        ("b", Arc::new(BooleanArray::from(vec![Some(true), None]))),
        (
            "bl",
            Arc::new(BinaryArray::from(vec![Some(b"x".as_ref()), None])),
        ),
    ]);
    assert_eq!(
        tsv(b),
        "i\ts\td\tts\ttz\tt\tdc\tf\tb\tbl\n\
         1\tplain\t2024-01-01\t2024-01-01T00:00:00\t2024-01-01T00:00:00Z\t12:00:00\t3.14\t1.5\ttrue\t78\n\
         \t\t\t\t\t\t\t\t\t\n"
    );
}

#[test]
fn escapes_framing_characters_unless_raw() {
    let b = batch(vec![
        (
            "a\tb",
            Arc::new(StringArray::from(vec!["x\ty\nz\r\\"])) as ArrayRef,
        ),
        ("n", Arc::new(Int32Array::from(vec![7]))),
    ]);
    let bytes = ipc(&[b], None);
    assert_eq!(
        tsv_with(&bytes, true, true),
        "a\\tb\tn\nx\\ty\\nz\\r\\\\\t7\n"
    );
    assert_eq!(tsv_with(&bytes, true, false), "a\tb\tn\nx\ty\nz\r\\\t7\n");
}

#[test]
fn nested_cells_follow_the_same_escaping_rule() {
    // The same value in a scalar column and inside a list, with a backslash and a tab in it.
    let value = "a\\b\tc";
    let mut list = ListBuilder::new(StringBuilder::new());
    list.values().append_value(value);
    list.append(true);
    let b = batch(vec![
        ("s", Arc::new(StringArray::from(vec![value])) as ArrayRef),
        ("l", Arc::new(list.finish())),
    ]);
    let bytes = ipc(&[b], None);

    let escaped = tsv_with(&bytes, false, true);
    assert_eq!(escaped, "a\\\\b\\tc\t[\"a\\\\\\\\b\\\\tc\"]\n");
    // Undoing the TSV escaping on every cell gives the value back, and the JSON the server sends.
    let unescape = |cell: &str| {
        let mut out = String::new();
        let mut chars = cell.chars();
        while let Some(c) = chars.next() {
            if c != '\\' {
                out.push(c);
                continue;
            }
            out.push(match chars.next() {
                Some('t') => '\t',
                Some('n') => '\n',
                Some('r') => '\r',
                Some(c) => c,
                None => '\\',
            });
        }
        out
    };
    let cells: Vec<String> = escaped.trim_end().split('\t').map(unescape).collect();
    assert_eq!(cells, [value, "[\"a\\\\b\\tc\"]"]);
    assert_eq!(
        tsv_with(&bytes, false, false),
        format!("{value}\t{}\n", cells[1])
    );
}

#[test]
fn no_header_omits_column_names() {
    let b = batch(vec![(
        "n",
        Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef,
    )]);
    assert_eq!(tsv_with(&ipc(&[b], None), false, true), "1\n2\n");
}

#[test]
fn lists_render_as_json_arrays() {
    let list = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
        Some(vec![Some(1), None, Some(3)]),
        None,
        Some(vec![]),
    ]);
    let fixed = FixedSizeListArray::from_iter_primitive::<Float64Type, _, _>(
        vec![
            Some(vec![Some(1.5), Some(2.0)]),
            None,
            Some(vec![Some(f64::NAN), None]),
        ],
        2,
    );
    let b = batch(vec![
        ("l", Arc::new(list) as ArrayRef),
        ("fx", Arc::new(fixed)),
    ]);
    assert_eq!(
        tsv(b),
        "l\tfx\n[1,null,3]\t[1.5,2.0]\n\t\n[]\t[\"NaN\",null]\n"
    );
}

#[test]
fn string_and_number_arrays() {
    use arrow_array::builder::{GenericListBuilder, ListBuilder, StringViewBuilder};
    use arrow_array::types::{Int64Type, UInt8Type};
    use arrow_array::{GenericListArray, LargeStringArray};

    let mut strings = ListBuilder::new(StringBuilder::new());
    for v in [Some("a"), None, Some(""), Some("q\"t\tz\\"), Some("é漢")] {
        strings.values().append_option(v);
    }
    strings.append(true);
    strings.append(true); // empty
    strings.append(false); // null

    // DuckDB may send VARCHAR as Utf8View and big lists as LargeList.
    let mut views = ListBuilder::new(StringViewBuilder::new());
    views
        .values()
        .append_value("a string longer than twelve bytes");
    views.values().append_value("b");
    views.append(true);
    views.append(true);
    views.append(true);
    let large_strings = GenericListArray::<i64>::new(
        Arc::new(Field::new("item", DataType::LargeUtf8, true)),
        arrow_buffer::OffsetBuffer::from_lengths([1, 0, 0]),
        Arc::new(LargeStringArray::from(vec!["x"])),
        None,
    );

    let bigints = ListArray::from_iter_primitive::<Int64Type, _, _>(vec![
        Some(vec![Some(i64::MIN), Some(0), Some(i64::MAX)]),
        Some(vec![None]),
        Some(vec![]),
    ]);
    let small = ListArray::from_iter_primitive::<UInt8Type, _, _>(vec![
        Some(vec![Some(255)]),
        None,
        Some(vec![]),
    ]);
    let doubles = ListArray::from_iter_primitive::<Float64Type, _, _>(vec![
        Some(vec![Some(-0.5), Some(1e300), Some(2.0)]),
        Some(vec![Some(f64::INFINITY), Some(f64::NEG_INFINITY)]),
        None,
    ]);
    let decimals = {
        let values = Decimal128Array::from(vec![Some(-314), None, Some(0)])
            .with_precision_and_scale(5, 2)
            .unwrap();
        ListArray::new(
            Arc::new(Field::new("item", values.data_type().clone(), true)),
            arrow_buffer::OffsetBuffer::from_lengths([3, 0, 0]),
            Arc::new(values),
            None,
        )
    };
    let mut large_ints = GenericListBuilder::<i64, _>::new(Int32Builder::new());
    large_ints.values().append_value(7);
    large_ints.append(true);
    large_ints.append(false);
    large_ints.append(true);

    let b = batch(vec![
        ("s", Arc::new(strings.finish()) as ArrayRef),
        ("sv", Arc::new(views.finish())),
        ("ls", Arc::new(large_strings)),
        ("i64", Arc::new(bigints)),
        ("u8", Arc::new(small)),
        ("f64", Arc::new(doubles)),
        ("dec", Arc::new(decimals)),
        ("li", Arc::new(large_ints.finish())),
    ]);
    assert_eq!(
        tsv_raw(b),
        "s\tsv\tls\ti64\tu8\tf64\tdec\tli\n\
         [\"a\",null,\"\",\"q\\\"t\\tz\\\\\",\"é漢\"]\t[\"a string longer than twelve bytes\",\"b\"]\t[\"x\"]\t\
         [-9223372036854775808,0,9223372036854775807]\t[255]\t[-0.5,1e300,2.0]\t[-3.14,null,0.00]\t[7]\n\
         []\t[]\t[]\t[null]\t\t[\"Infinity\",\"-Infinity\"]\t[]\t\n\
         \t[]\t[]\t[]\t[]\t\t[]\t[]\n"
    );
}

#[test]
fn temporal_arrays() {
    use arrow_array::{TimestampMillisecondArray, TimestampNanosecondArray};

    fn list(values: ArrayRef) -> ArrayRef {
        let n = values.len();
        Arc::new(ListArray::new(
            Arc::new(Field::new("item", values.data_type().clone(), true)),
            arrow_buffer::OffsetBuffer::from_lengths([n, 0]),
            values,
            Some(NullBuffer::from(vec![true, false])),
        ))
    }
    let b = batch(vec![
        (
            "d",
            list(Arc::new(Date32Array::from(vec![Some(DAY), None, Some(-1)]))),
        ),
        (
            "ts",
            list(Arc::new(TimestampMicrosecondArray::from(vec![
                MICROS,
                MICROS + 1_500_000,
            ]))),
        ),
        (
            "tz",
            list(Arc::new(
                TimestampMillisecondArray::from(vec![MICROS / 1000]).with_timezone("Etc/UTC"),
            )),
        ),
        (
            "tzo",
            list(Arc::new(
                TimestampNanosecondArray::from(vec![MICROS * 1000]).with_timezone("+05:30"),
            )),
        ),
        (
            "t",
            list(Arc::new(Time64MicrosecondArray::from(vec![
                12 * 3_600_000_000 + 250_000,
            ]))),
        ),
    ]);
    assert_eq!(
        tsv(b),
        "d\tts\ttz\ttzo\tt\n\
         [\"2024-01-01\",null,\"1969-12-31\"]\t[\"2024-01-01T00:00:00\",\"2024-01-01T00:00:01.5\"]\t\
         [\"2024-01-01T00:00:00Z\"]\t[\"2024-01-01T00:00:00Z\"]\t[\"12:00:00.25\"]\n\
         \t\t\t\t\n"
    );
}

fn sample_struct() -> StructArray {
    let fields = Fields::from(vec![
        Field::new("x", DataType::Int32, true),
        Field::new("s", DataType::Utf8, true),
        Field::new("d", DataType::Date32, true),
        Field::new("b", DataType::Boolean, true),
    ]);
    StructArray::try_new(
        fields,
        vec![
            Arc::new(Int32Array::from(vec![Some(1), Some(2), None])),
            Arc::new(StringArray::from(vec![
                Some("q\"t\t\u{1}"),
                None,
                Some("z"),
            ])),
            Arc::new(Date32Array::from(vec![Some(DAY), None, None])),
            Arc::new(BooleanArray::from(vec![Some(false), None, None])),
        ],
        Some(NullBuffer::from(vec![true, false, true])),
    )
    .unwrap()
}

#[test]
fn structs_render_as_json_objects() {
    let b = batch(vec![("st", Arc::new(sample_struct()) as ArrayRef)]);
    assert_eq!(
        tsv_raw(b),
        "st\n\
         {\"x\":1,\"s\":\"q\\\"t\\t\\u0001\",\"d\":\"2024-01-01\",\"b\":false}\n\
         \n\
         {\"x\":null,\"s\":\"z\",\"d\":null,\"b\":null}\n"
    );
}

fn sample_map() -> ArrayRef {
    let mut m = MapBuilder::new(None, StringBuilder::new(), Int32Builder::new());
    m.keys().append_value("k");
    m.values().append_value(1);
    m.keys().append_value("n");
    m.values().append_null();
    m.append(true).unwrap();
    m.append(false).unwrap();
    m.keys().append_value("z");
    m.values().append_value(5);
    m.append(true).unwrap();
    Arc::new(m.finish())
}

#[test]
fn maps_render_as_key_value_entries() {
    let b = batch(vec![("m", sample_map())]);
    assert_eq!(
        tsv(b),
        "m\n[{\"key\":\"k\",\"value\":1},{\"key\":\"n\",\"value\":null}]\n\n[{\"key\":\"z\",\"value\":5}]\n"
    );
}

#[test]
fn nesting_composes() {
    // list<struct<i: list<int>>>
    let inner =
        ListArray::from_iter_primitive::<Int32Type, _, _>(vec![Some(vec![Some(1)]), Some(vec![])]);
    let st = StructArray::from(vec![(
        Arc::new(Field::new("i", inner.data_type().clone(), true)),
        Arc::new(inner) as ArrayRef,
    )]);
    let field = Arc::new(Field::new("item", st.data_type().clone(), true));
    let outer = ListArray::new(
        field,
        arrow_buffer::OffsetBuffer::from_lengths([2]),
        Arc::new(st),
        None,
    );
    let b = batch(vec![("n", Arc::new(outer) as ArrayRef)]);
    assert_eq!(tsv(b), "n\n[{\"i\":[1]},{\"i\":[]}]\n");
}

#[test]
fn encoder_honours_sliced_arrays() {
    // IPC rebases sliced arrays on write, so this goes to the encoder directly.
    let list: ArrayRef = Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
        Some(vec![Some(1)]),
        Some(vec![Some(2), Some(3)]),
        Some(vec![Some(4)]),
    ]));
    let fixed: ArrayRef = Arc::new(FixedSizeListArray::from_iter_primitive::<Int32Type, _, _>(
        vec![
            Some(vec![Some(1), Some(2)]),
            Some(vec![Some(3), Some(4)]),
            Some(vec![Some(5), Some(6)]),
        ],
        2,
    ));
    let st: ArrayRef = Arc::new(sample_struct());
    let options = FormatOptions::default();
    let render = |array: &ArrayRef| {
        let sliced = array.slice(1, 2);
        let enc = json::Encoder::try_new(sliced.as_ref(), &options).unwrap();
        (0..2)
            .map(|i| {
                let mut s = String::new();
                enc.encode(i, &mut s).unwrap();
                s
            })
            .collect::<Vec<_>>()
    };
    assert_eq!(render(&list), ["[2,3]", "[4]"]);
    assert_eq!(render(&fixed), ["[3,4]", "[5,6]"]);
    assert_eq!(
        render(&st),
        ["null", "{\"x\":null,\"s\":\"z\",\"d\":null,\"b\":null}"]
    );
    assert_eq!(
        render(&sample_map()),
        ["null", "[{\"key\":\"z\",\"value\":5}]"]
    );
}

#[test]
fn zstd_compressed_multi_batch_stream() {
    let make = |v: Vec<i32>| batch(vec![("n", Arc::new(Int32Array::from(v)) as ArrayRef)]);
    let bytes = ipc(
        &[make(vec![1, 2]), make(vec![]), make(vec![3])],
        Some(CompressionType::ZSTD),
    );
    assert_eq!(tsv_with(&bytes, true, true), "n\n1\n2\n3\n");
}

#[test]
fn schema_only_stream_prints_header() {
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)]));
    let mut buf = Vec::new();
    StreamWriter::try_new(&mut buf, &schema)
        .unwrap()
        .finish()
        .unwrap();
    assert_eq!(tsv_with(&buf, true, true), "a\n");
}

#[test]
fn invalid_input_is_an_error() {
    assert!(convert(&b"not arrow"[..], Vec::new(), true, true).is_err());
}

#[test]
fn percent_encodes_queries() {
    assert_eq!(
        percent_encode("select 'a&b' = 1"),
        "select%20%27a%26b%27%20%3D%201"
    );
    assert_eq!(percent_encode("é"), "%C3%A9");
}

#[test]
fn trims_fractional_seconds() {
    let cases = [
        ("12:00:00", "12:00:00"),
        ("12:00:00.000", "12:00:00"),
        ("12:00:00.250", "12:00:00.25"),
        ("2024-01-01T00:00:01.500000", "2024-01-01T00:00:01.5"),
        (
            "2024-01-01T00:00:01.123456789",
            "2024-01-01T00:00:01.123456789",
        ),
        (
            "2024-01-01T00:00:01.100+05:30",
            "2024-01-01T00:00:01.1+05:30",
        ),
    ];
    for (input, expected) in cases {
        let mut s = format!("x{input}");
        scalar::trim_fraction(&mut s, 1);
        assert_eq!(s, format!("x{expected}"), "{input}");
    }
}

#[test]
fn fractions_match_the_server_per_type() {
    let b = batch(vec![
        (
            "ts",
            Arc::new(TimestampMicrosecondArray::from(vec![MICROS + 1_500_000])) as ArrayRef,
        ),
        (
            "tz",
            Arc::new(
                TimestampMicrosecondArray::from(vec![MICROS + 1_500_000]).with_timezone("Etc/UTC"),
            ),
        ),
        (
            "t",
            Arc::new(Time64MicrosecondArray::from(vec![
                12 * 3_600_000_000 + 250_000,
            ])),
        ),
    ]);
    assert_eq!(
        tsv(b),
        "ts\ttz\tt\n2024-01-01T00:00:01.5\t2024-01-01T00:00:01.500Z\t12:00:00.25\n"
    );
}

#[test]
fn zoned_timestamps_print_as_utc_at_any_depth() {
    // DuckDB labels TIMESTAMPTZ with the server's TimeZone setting; the output must not depend on it.
    let la = || {
        TimestampMicrosecondArray::from(vec![MICROS + 1_500_000])
            .with_timezone("America/Los_Angeles")
    };
    let values: ArrayRef = Arc::new(la());
    let list = ListArray::new(
        Arc::new(Field::new("item", values.data_type().clone(), true)),
        arrow_buffer::OffsetBuffer::from_lengths([1]),
        values,
        None,
    );
    let st = StructArray::from(vec![(
        Arc::new(Field::new("t", la().data_type().clone(), true)),
        Arc::new(la()) as ArrayRef,
    )]);
    let b = batch(vec![
        ("top", Arc::new(la()) as ArrayRef),
        ("l", Arc::new(list)),
        ("st", Arc::new(st)),
    ]);
    assert_eq!(
        tsv(b),
        "top\tl\tst\n\
         2024-01-01T00:00:01.500Z\t[\"2024-01-01T00:00:01.500Z\"]\t{\"t\":\"2024-01-01T00:00:01.500Z\"}\n"
    );
}

fn args(argv: &[&str]) -> Result<Command, String> {
    parse_args(argv.iter().map(|s| s.to_string()), None)
}

#[test]
fn parses_options() {
    let Ok(Command::Run(a)) = parse_args(
        [
            "http://h/v1/query",
            "-q",
            "select 1",
            "-H",
            "X-A: b:c",
            "--timeout",
            "1.5",
            "--raw",
            "--no-header",
        ]
        .map(String::from),
        Some("env-token".into()),
    ) else {
        panic!("expected Run");
    };
    assert_eq!(
        a,
        Args {
            url: Some("http://h/v1/query".into()),
            query: Some("select 1".into()),
            token: Some("env-token".into()),
            headers: vec![("X-A".into(), "b:c".into())],
            timeout: Some(Duration::from_millis(1500)),
            no_header_row: true,
            raw: true,
        }
    );
    // -t overrides $DD_TOKEN; an empty $DD_TOKEN counts as unset.
    let token = |argv: &[&str], env: Option<&str>| match parse_args(
        argv.iter().map(|s| s.to_string()),
        env.map(String::from),
    ) {
        Ok(Command::Run(a)) => a.token,
        other => panic!("{other:?}"),
    };
    assert_eq!(token(&["-t", "flag"], Some("env")), Some("flag".into()));
    assert_eq!(token(&[], Some("")), None);
    assert_eq!(args(&["-V"]), Ok(Command::Version));
    assert_eq!(args(&["--help", "--bogus"]), Ok(Command::Help));
    assert!(matches!(args(&["-"]), Ok(Command::Run(Args { url: Some(u), .. })) if u == "-"));
}

#[test]
fn rejects_bad_options() {
    for (argv, message) in [
        (&["-H", ": v"][..], "bad header ': v'"),
        (&["-H", "no-colon"], "bad header 'no-colon'"),
        (&["--timeout", "0"], "bad --timeout '0'"),
        (&["--timeout", "soon"], "bad --timeout 'soon'"),
        (&["-q", "select 1"], "--query needs a URL"),
        (&["-", "-q", "select 1"], "--query needs a URL"),
        (&["-t"], "-t needs a value"),
        (&["--bogus"], "unknown option --bogus"),
        (&["a", "b"], "unexpected argument b"),
    ] {
        let err = args(argv).unwrap_err();
        assert!(err.starts_with(message), "{argv:?}: {err}");
    }
}

#[test]
fn intervals_render_like_the_server() {
    use arrow_array::IntervalMonthDayNanoArray;
    use arrow_array::types::IntervalMonthDayNano;

    let values = || {
        IntervalMonthDayNanoArray::from(vec![
            Some(IntervalMonthDayNano::new(0, 1, 7_200_000_000_000)),
            Some(IntervalMonthDayNano::new(0, 0, -5_400_000_000_000)),
            None,
        ])
    };
    let list = ListArray::new(
        Arc::new(Field::new("item", values().data_type().clone(), true)),
        arrow_buffer::OffsetBuffer::from_lengths([3, 0, 0]),
        Arc::new(values()),
        None,
    );
    let b = batch(vec![
        ("i", Arc::new(values()) as ArrayRef),
        ("l", Arc::new(list)),
    ]);
    assert_eq!(
        tsv(b),
        "i\tl\nP1D PT2H\t[\"P1D PT2H\",\"P0D PT-1H-30M\",null]\nP0D PT-1H-30M\t[]\n\t[]\n"
    );
}

#[test]
fn timeout_stops_a_stalled_request() {
    // A server that accepts the connection and then never answers.
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}/v1/query", listener.local_addr().unwrap());
    let stall = std::thread::spawn(move || {
        let (_conn, _) = listener.accept().unwrap();
        std::thread::sleep(Duration::from_secs(10));
    });
    let a = Args {
        url: Some(url),
        timeout: Some(Duration::from_millis(500)),
        ..Args::default()
    };
    let started = std::time::Instant::now();
    let err = open_input(&a).err().expect("a stalled request must fail");
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "took {:?}",
        started.elapsed()
    );
    assert!(err.to_string().contains("timeout"), "{err}");
    drop(stall);
}

#[test]
fn timeout_also_covers_a_stalled_body() {
    use std::io::Write as _;
    // Headers arrive, then the body never does.
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}/v1/query", listener.local_addr().unwrap());
    std::thread::spawn(move || {
        let (mut conn, _) = listener.accept().unwrap();
        let mut request = [0u8; 4096];
        let _ = std::io::Read::read(&mut conn, &mut request);
        let _ = conn.write_all(
            b"HTTP/1.1 200 OK\r\nContent-Type: application/vnd.apache.arrow.stream\r\n\
              Transfer-Encoding: chunked\r\n\r\n",
        );
        std::thread::sleep(Duration::from_secs(10));
    });
    let a = Args {
        url: Some(url),
        timeout: Some(Duration::from_millis(500)),
        ..Args::default()
    };
    let started = std::time::Instant::now();
    let input = open_input(&a).expect("headers arrive");
    assert!(convert(input, Vec::new(), true, true).is_err());
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "took {:?}",
        started.elapsed()
    );
}
