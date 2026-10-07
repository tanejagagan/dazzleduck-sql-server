//! arrow2tsv — convert an Arrow IPC stream to TSV.
//!
//! Reads from a URL (HTTP GET) or stdin and writes a header row plus one tab-separated line per
//! row to stdout, one record batch at a time, so large results stream in constant memory.
//!
//! ```text
//! arrow2tsv http://localhost:8081/v1/query -q "select 1" -t "$TOKEN"
//! curl -s -H "Authorization: Bearer $TOKEN" "http://localhost:8081/v1/query?q=select%201" | arrow2tsv
//! ```

use std::io::{self, BufWriter, Read, Write};
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use arrow_array::{Array, RecordBatch};
use arrow_cast::display::FormatOptions;
use arrow_ipc::reader::StreamReader;
use arrow_schema::{ArrowError, DataType, Field, Schema};

mod json;
mod scalar;

/// The release version, injected by CI (`ARROW2TSV_VERSION`); a local build reports Cargo's.
const VERSION: &str = match option_env!("ARROW2TSV_VERSION") {
    Some(v) => v,
    None => env!("CARGO_PKG_VERSION"),
};

/// How long to wait for the TCP/TLS connection. There is no overall limit unless `--timeout` is
/// given: a long-running query is legitimate.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);

/// How one column's cells are rendered.
enum Column<'a> {
    Scalar(scalar::Scalar<'a>),
    /// List / struct / map as JSON.
    Nested(json::Encoder<'a>),
}

const USAGE: &str = "\
Usage: arrow2tsv [URL | -] [options]

Reads an Arrow IPC stream from URL (HTTP GET) or, without URL or with '-', from stdin, and writes
TSV to stdout: a header row, then one line per row. Lists, structs and maps are JSON.

Options:
  -q, --query SQL       append ?q=SQL (URL-encoded) to URL
  -t, --token TOKEN     send 'Authorization: Bearer TOKEN'; prefer $DD_TOKEN, the default,
                        since a token on the command line is visible to other users in ps
  -H, --header 'K: V'   extra request header (repeatable)
      --timeout SECS    give up if the whole request takes longer (default: no limit;
                        connecting is always limited to 30 seconds)
      --no-header       do not print the column-name row
      --raw             do not escape values (see below)
  -V, --version         print the version
  -h, --help            show this help

Escaping: in every cell, JSON ones included, tab, newline, carriage return and backslash are
written as \\t, \\n, \\r and \\\\, so undoing that on each cell restores the value (and valid
JSON). --raw writes values as they are, like the server's own TSV output.";

#[derive(Debug, Default, PartialEq)]
struct Args {
    url: Option<String>,
    query: Option<String>,
    token: Option<String>,
    headers: Vec<(String, String)>,
    timeout: Option<Duration>,
    no_header_row: bool,
    raw: bool,
}

#[derive(Debug, PartialEq)]
enum Command {
    Run(Args),
    Help,
    Version,
}

/// Parses the arguments after the program name; `env_token` is `$DD_TOKEN`.
fn parse_args(
    argv: impl IntoIterator<Item = String>,
    env_token: Option<String>,
) -> Result<Command, String> {
    let mut args = Args {
        token: env_token.filter(|t| !t.is_empty()),
        ..Args::default()
    };
    let mut it = argv.into_iter();
    while let Some(arg) = it.next() {
        let mut value = |name: &str| it.next().ok_or_else(|| format!("{name} needs a value"));
        match arg.as_str() {
            "-h" | "--help" => return Ok(Command::Help),
            "-V" | "--version" => return Ok(Command::Version),
            "-q" | "--query" => args.query = Some(value(&arg)?),
            "-t" | "--token" => args.token = Some(value(&arg)?),
            "-H" | "--header" => {
                let h = value(&arg)?;
                let (k, v) = h
                    .split_once(':')
                    .map(|(k, v)| (k.trim(), v.trim()))
                    .filter(|(k, _)| !k.is_empty())
                    .ok_or_else(|| format!("bad header '{h}', expected 'Name: value'"))?;
                args.headers.push((k.to_string(), v.to_string()));
            }
            "--timeout" => {
                let v = value(&arg)?;
                let secs = v
                    .parse::<f64>()
                    .ok()
                    .filter(|s| s.is_finite() && *s > 0.0)
                    .ok_or_else(|| format!("bad --timeout '{v}', expected a number of seconds"))?;
                args.timeout = Some(Duration::from_secs_f64(secs));
            }
            "--no-header" => args.no_header_row = true,
            "--raw" => args.raw = true,
            s if s.starts_with('-') && s != "-" => return Err(format!("unknown option {s}")),
            s if args.url.is_none() => args.url = Some(s.to_string()),
            s => return Err(format!("unexpected argument {s}")),
        }
    }
    if args.query.is_some() && args.url.as_deref().is_none_or(|u| u == "-") {
        return Err("--query needs a URL".into());
    }
    Ok(Command::Run(args))
}

fn percent_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

fn open_input(args: &Args) -> Result<Box<dyn Read>, Box<dyn std::error::Error>> {
    let Some(url) = args.url.as_deref().filter(|u| *u != "-") else {
        return Ok(Box::new(io::stdin().lock()));
    };
    let url = match &args.query {
        Some(q) => {
            let sep = if url.contains('?') { '&' } else { '?' };
            format!("{url}{sep}q={}", percent_encode(q))
        }
        None => url.to_string(),
    };
    let agent: ureq::Agent = ureq::Agent::config_builder()
        .http_status_as_error(false)
        .timeout_connect(Some(CONNECT_TIMEOUT))
        .timeout_global(args.timeout)
        .build()
        .into();
    let mut req = agent
        .get(&url)
        .header("Accept", "application/vnd.apache.arrow.stream");
    if let Some(token) = &args.token {
        req = req.header("Authorization", format!("Bearer {token}"));
    }
    for (k, v) in &args.headers {
        req = req.header(k, v);
    }
    let resp = req.call()?;
    let status = resp.status();
    let mut body = resp.into_body();
    if !status.is_success() {
        let text = body.read_to_string().unwrap_or_default();
        return Err(format!("HTTP {status}: {}", text.trim()).into());
    }
    Ok(Box::new(body.into_reader()))
}

/// Writes `s`, escaping the characters that would break TSV framing.
fn write_cell(out: &mut impl Write, s: &str, escape: bool) -> io::Result<()> {
    if !escape
        || !s
            .bytes()
            .any(|b| matches!(b, b'\t' | b'\n' | b'\r' | b'\\'))
    {
        return out.write_all(s.as_bytes());
    }
    for c in s.chars() {
        match c {
            '\t' => out.write_all(b"\\t")?,
            '\n' => out.write_all(b"\\n")?,
            '\r' => out.write_all(b"\\r")?,
            '\\' => out.write_all(b"\\\\")?,
            c => write!(out, "{c}")?,
        }
    }
    Ok(())
}

fn run(args: Args) -> Result<(), Box<dyn std::error::Error>> {
    let input = open_input(&args)?;
    let out = BufWriter::with_capacity(1 << 16, io::stdout().lock());
    convert(input, out, !args.no_header_row, !args.raw)
}

/// Converts the Arrow IPC stream in `input` to TSV on `out`, flushing after each batch.
fn convert(
    input: impl Read,
    mut out: impl Write,
    header_row: bool,
    escape: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let reader = StreamReader::try_new(input, None)?;

    if header_row {
        for (i, field) in reader.schema().fields().iter().enumerate() {
            if i > 0 {
                out.write_all(b"\t")?;
            }
            write_cell(&mut out, field.name(), escape)?;
        }
        out.write_all(b"\n")?;
    }

    let options = FormatOptions::default().with_display_error(true);
    let mut cell = String::new();
    for batch in reader {
        let batch = utc_timestamps(batch?)?;
        let columns = batch
            .columns()
            .iter()
            .map(|c| {
                Ok(if json::is_nested(c.data_type()) {
                    Column::Nested(json::Encoder::try_new(c.as_ref(), &options)?)
                } else {
                    Column::Scalar(scalar::Scalar::try_new(c.as_ref(), &options)?)
                })
            })
            .collect::<Result<Vec<_>, arrow_schema::ArrowError>>()?;
        for row in 0..batch.num_rows() {
            for (i, column) in columns.iter().enumerate() {
                if i > 0 {
                    out.write_all(b"\t")?;
                }
                cell.clear();
                match column {
                    Column::Scalar(f) => f.write(row, &mut cell)?,
                    // A null nested value is an empty cell, like any other null.
                    Column::Nested(enc) if batch.column(i).is_null(row) => {}
                    Column::Nested(enc) => enc.encode(row, &mut cell)?,
                }
                // One rule for every cell, JSON included, so a reader can unescape uniformly.
                write_cell(&mut out, &cell, escape)?;
            }
            out.write_all(b"\n")?;
        }
        out.flush()?;
    }
    out.flush()?;
    Ok(())
}

/// `dt` with every timezone-aware timestamp, at any depth, relabelled as UTC.
fn utc_type(dt: &DataType) -> DataType {
    let field = |f: &Field| Arc::new(f.clone().with_data_type(utc_type(f.data_type())));
    match dt {
        DataType::Timestamp(unit, Some(_)) => DataType::Timestamp(*unit, Some("UTC".into())),
        DataType::List(f) => DataType::List(field(f)),
        DataType::LargeList(f) => DataType::LargeList(field(f)),
        DataType::FixedSizeList(f, n) => DataType::FixedSizeList(field(f), *n),
        DataType::Struct(fs) => DataType::Struct(fs.iter().map(|f| field(f)).collect()),
        DataType::Map(f, sorted) => DataType::Map(field(f), *sorted),
        dt => dt.clone(),
    }
}

/// Prints timezone-aware timestamps as UTC instants (`...Z`), as the server does, rather than in
/// the column's timezone, which DuckDB sets from the server's `TimeZone` setting. The values are
/// already UTC instants, so this only changes the timezone label.
fn utc_timestamps(batch: RecordBatch) -> Result<RecordBatch, ArrowError> {
    let schema = batch.schema();
    if schema
        .fields()
        .iter()
        .all(|f| utc_type(f.data_type()) == *f.data_type())
    {
        return Ok(batch);
    }
    let mut fields = Vec::with_capacity(schema.fields().len());
    let mut columns = Vec::with_capacity(schema.fields().len());
    for (f, c) in schema.fields().iter().zip(batch.columns()) {
        let dt = utc_type(f.data_type());
        columns.push(if dt == *f.data_type() {
            c.clone()
        } else {
            arrow_cast::cast(c, &dt)?
        });
        fields.push(f.as_ref().clone().with_data_type(dt));
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
}

fn main() -> ExitCode {
    let args = match parse_args(std::env::args().skip(1), std::env::var("DD_TOKEN").ok()) {
        Ok(Command::Run(a)) => a,
        Ok(Command::Help) => {
            println!("{USAGE}");
            return ExitCode::SUCCESS;
        }
        Ok(Command::Version) => {
            println!("arrow2tsv {VERSION}");
            return ExitCode::SUCCESS;
        }
        Err(e) => {
            eprintln!("arrow2tsv: {e}\n\n{USAGE}");
            return ExitCode::from(2);
        }
    };
    match run(args) {
        Ok(()) => ExitCode::SUCCESS,
        // A closed pipe (e.g. `| head`) is a normal way to stop reading.
        Err(e)
            if e.downcast_ref::<io::Error>()
                .is_some_and(|e| e.kind() == io::ErrorKind::BrokenPipe) =>
        {
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("arrow2tsv: {e}");
            ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
mod tests;
