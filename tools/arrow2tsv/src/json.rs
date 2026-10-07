//! JSON rendering of nested (list / struct / map) columns, matching the server's TSV output:
//! lists are arrays, structs are objects, maps are arrays of `{"key":..,"value":..}` entries.
//! Numbers and booleans are bare, nulls are `null`, everything else (strings, temporals,
//! binaries as hex) is a JSON string in its display form.

use std::fmt::Write;

use arrow_array::cast::AsArray;
use arrow_array::{Array, OffsetSizeTrait};
use arrow_cast::display::{ArrayFormatter, FormatOptions};
use arrow_schema::{ArrowError, DataType};

/// True for the types rendered as JSON rather than as a plain display string.
pub fn is_nested(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::List(_)
            | DataType::LargeList(_)
            | DataType::FixedSizeList(_, _)
            | DataType::Struct(_)
            | DataType::Map(_, _)
    )
}

/// True for the types whose fractional seconds the server prints with only as many digits as
/// needed (`00:00:01.5`): times and timezone-less timestamps. Timestamps with a timezone keep
/// groups of three (`00:00:01.500Z`), which is also Arrow's default.
pub fn trims_fraction(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Time32(_) | DataType::Time64(_) | DataType::Timestamp(_, None)
    )
}

/// Drops trailing zeros from the fractional seconds in `s[start..]`, and the `.` if none remain.
pub fn trim_fraction(s: &mut String, start: usize) {
    let Some(dot) = s[start..].find('.').map(|d| start + d) else {
        return;
    };
    let digits_end = s[dot + 1..]
        .find(|c: char| !c.is_ascii_digit())
        .map_or(s.len(), |e| dot + 1 + e);
    let kept = s[dot + 1..digits_end].trim_end_matches('0').len();
    let cut_from = if kept == 0 { dot } else { dot + 1 + kept };
    s.replace_range(cut_from..digits_end, "");
}

/// A per-batch encoder for one array; child encoders index their own (child) arrays.
pub enum Encoder<'a> {
    Leaf {
        array: &'a dyn Array,
        fmt: ArrayFormatter<'a>,
        bare: bool,
        trim: bool,
    },
    List {
        array: &'a dyn Array,
        child: Box<Encoder<'a>>,
    },
    FixedSizeList {
        array: &'a dyn Array,
        size: usize,
        child: Box<Encoder<'a>>,
    },
    Struct {
        array: &'a dyn Array,
        fields: Vec<(String, Encoder<'a>)>,
    },
    Map {
        array: &'a dyn Array,
        key: Box<Encoder<'a>>,
        value: Box<Encoder<'a>>,
    },
}

impl<'a> Encoder<'a> {
    pub fn try_new(array: &'a dyn Array, options: &FormatOptions<'a>) -> Result<Self, ArrowError> {
        Ok(match array.data_type() {
            DataType::List(_) => {
                let child = array.as_list::<i32>().values().as_ref();
                Encoder::List {
                    array,
                    child: Box::new(Encoder::try_new(child, options)?),
                }
            }
            DataType::LargeList(_) => {
                let child = array.as_list::<i64>().values().as_ref();
                Encoder::List {
                    array,
                    child: Box::new(Encoder::try_new(child, options)?),
                }
            }
            DataType::FixedSizeList(_, size) => {
                let child = array.as_fixed_size_list().values().as_ref();
                Encoder::FixedSizeList {
                    array,
                    size: *size as usize,
                    child: Box::new(Encoder::try_new(child, options)?),
                }
            }
            DataType::Struct(fields) => {
                let s = array.as_struct();
                let fields = fields
                    .iter()
                    .zip(s.columns())
                    .map(|(f, c)| Ok((f.name().clone(), Encoder::try_new(c.as_ref(), options)?)))
                    .collect::<Result<_, ArrowError>>()?;
                Encoder::Struct { array, fields }
            }
            DataType::Map(_, _) => {
                let m = array.as_map();
                Encoder::Map {
                    array,
                    key: Box::new(Encoder::try_new(m.keys().as_ref(), options)?),
                    value: Box::new(Encoder::try_new(m.values().as_ref(), options)?),
                }
            }
            dt => Encoder::Leaf {
                array,
                fmt: ArrayFormatter::try_new(array, options)?,
                bare: dt.is_numeric() || *dt == DataType::Boolean,
                trim: trims_fraction(dt),
            },
        })
    }

    /// Appends the JSON for row `i` to `out`.
    pub fn encode(&self, i: usize, out: &mut String) -> Result<(), ArrowError> {
        let array = match self {
            Encoder::Leaf { array, .. }
            | Encoder::List { array, .. }
            | Encoder::FixedSizeList { array, .. }
            | Encoder::Struct { array, .. }
            | Encoder::Map { array, .. } => *array,
        };
        if array.is_null(i) {
            out.push_str("null");
            return Ok(());
        }
        match self {
            Encoder::Leaf {
                fmt, bare, trim, ..
            } => {
                let start = out.len();
                write!(out, "{}", fmt.value(i))
                    .map_err(|e| ArrowError::ExternalError(Box::new(e)))?;
                if *trim {
                    trim_fraction(out, start);
                }
                // NaN / inf are not JSON numbers; quote them like any other string.
                let text = &out[start..];
                let keep_bare = *bare
                    && (text == "true"
                        || text == "false"
                        || text.parse::<f64>().is_ok_and(f64::is_finite));
                if !keep_bare {
                    let text = out.split_off(start);
                    // Spell infinities the way the server (Jackson) does.
                    let text = match text.as_str() {
                        "inf" if *bare => "Infinity",
                        "-inf" if *bare => "-Infinity",
                        t => t,
                    };
                    push_json_string(out, text);
                }
            }
            Encoder::List { array, child } => {
                let range = match array.data_type() {
                    DataType::LargeList(_) => offsets(array.as_list::<i64>(), i),
                    _ => offsets(array.as_list::<i32>(), i),
                };
                encode_seq(range, child, out)?;
            }
            Encoder::FixedSizeList { size, child, .. } => {
                encode_seq(i * size..(i + 1) * size, child, out)?
            }
            Encoder::Struct { fields, .. } => {
                out.push('{');
                for (n, (name, enc)) in fields.iter().enumerate() {
                    if n > 0 {
                        out.push(',');
                    }
                    push_json_string(out, name);
                    out.push(':');
                    enc.encode(i, out)?;
                }
                out.push('}');
            }
            Encoder::Map { array, key, value } => {
                let offs = array.as_map().value_offsets();
                out.push('[');
                for (n, e) in (offs[i] as usize..offs[i + 1] as usize).enumerate() {
                    if n > 0 {
                        out.push(',');
                    }
                    out.push_str("{\"key\":");
                    key.encode(e, out)?;
                    out.push_str(",\"value\":");
                    value.encode(e, out)?;
                    out.push('}');
                }
                out.push(']');
            }
        }
        Ok(())
    }
}

fn offsets<O: OffsetSizeTrait>(
    list: &arrow_array::GenericListArray<O>,
    i: usize,
) -> std::ops::Range<usize> {
    let o = list.value_offsets();
    o[i].as_usize()..o[i + 1].as_usize()
}

fn encode_seq(
    range: std::ops::Range<usize>,
    child: &Encoder,
    out: &mut String,
) -> Result<(), ArrowError> {
    out.push('[');
    for (n, e) in range.enumerate() {
        if n > 0 {
            out.push(',');
        }
        child.encode(e, out)?;
    }
    out.push(']');
    Ok(())
}

fn push_json_string(out: &mut String, s: &str) {
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => {
                let _ = write!(out, "\\u{:04x}", c as u32);
            }
            c => out.push(c),
        }
    }
    out.push('"');
}
