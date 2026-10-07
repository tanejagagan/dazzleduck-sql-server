//! Formatting of single (non-nested) values the way the server's TSV prints them. Top-level cells
//! and the leaves of nested JSON values both go through here.

use std::fmt::Write;

use arrow_array::Array;
use arrow_array::cast::AsArray;
use arrow_array::types::{IntervalDayTimeType, IntervalMonthDayNanoType, IntervalYearMonthType};
use arrow_cast::display::{ArrayFormatter, FormatOptions};
use arrow_schema::{ArrowError, DataType, IntervalUnit};

const NANOS_PER_SECOND: i64 = 1_000_000_000;

/// A per-batch formatter for one non-nested array.
pub enum Scalar<'a> {
    /// Arrow's display form; `trim` drops trailing zeros from fractional seconds.
    Arrow { fmt: ArrayFormatter<'a>, trim: bool },
    /// Intervals as Java's `PeriodDuration` (`P1M2D PT3H`), which is what the server prints.
    Interval(&'a dyn Array),
}

impl<'a> Scalar<'a> {
    pub fn try_new(array: &'a dyn Array, options: &FormatOptions<'a>) -> Result<Self, ArrowError> {
        Ok(match array.data_type() {
            DataType::Interval(_) => Scalar::Interval(array),
            dt => Scalar::Arrow {
                fmt: ArrayFormatter::try_new(array, options)?,
                trim: trims_fraction(dt),
            },
        })
    }

    /// Appends row `i` to `out`; a null appends nothing.
    pub fn write(&self, i: usize, out: &mut String) -> Result<(), ArrowError> {
        match self {
            Scalar::Arrow { fmt, trim } => {
                let start = out.len();
                write!(out, "{}", fmt.value(i))
                    .map_err(|e| ArrowError::ExternalError(Box::new(e)))?;
                if *trim {
                    trim_fraction(out, start);
                }
            }
            Scalar::Interval(array) if array.is_null(i) => {}
            Scalar::Interval(array) => {
                let (months, days, nanos) = match array.data_type() {
                    DataType::Interval(IntervalUnit::MonthDayNano) => {
                        let v = array.as_primitive::<IntervalMonthDayNanoType>().value(i);
                        (v.months, v.days, v.nanoseconds)
                    }
                    DataType::Interval(IntervalUnit::DayTime) => {
                        let v = array.as_primitive::<IntervalDayTimeType>().value(i);
                        (0, v.days, v.milliseconds as i64 * 1_000_000)
                    }
                    _ => (array.as_primitive::<IntervalYearMonthType>().value(i), 0, 0),
                };
                write_period(out, months, days);
                out.push(' ');
                write_duration(out, nanos);
            }
        }
        Ok(())
    }
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

/// Java's `Period.toString()` for a period of `months` and `days` (no years): `P14M3D`, `P0D`.
fn write_period(out: &mut String, months: i32, days: i32) {
    if months == 0 && days == 0 {
        out.push_str("P0D");
        return;
    }
    out.push('P');
    if months != 0 {
        let _ = write!(out, "{months}M");
    }
    if days != 0 {
        let _ = write!(out, "{days}D");
    }
}

/// Java's `Duration.toString()` for `total_nanos`: `PT25H1M1S`, `PT-1H-30M`, `PT-0.5S`, `PT0S`.
/// Follows the JDK algorithm, including how it splits negative values into seconds and nanos.
fn write_duration(out: &mut String, total_nanos: i64) {
    if total_nanos == 0 {
        out.push_str("PT0S");
        return;
    }
    let seconds = total_nanos.div_euclid(NANOS_PER_SECOND);
    let nanos = total_nanos.rem_euclid(NANOS_PER_SECOND);
    let effective_secs = if seconds < 0 && nanos > 0 {
        seconds + 1
    } else {
        seconds
    };
    let hours = effective_secs / 3600;
    let minutes = (effective_secs % 3600) / 60;
    let secs = effective_secs % 60;
    let start = out.len();
    out.push_str("PT");
    if hours != 0 {
        let _ = write!(out, "{hours}H");
    }
    if minutes != 0 {
        let _ = write!(out, "{minutes}M");
    }
    if secs == 0 && nanos == 0 && out.len() - start > 2 {
        return;
    }
    if seconds < 0 && nanos > 0 && secs == 0 {
        out.push_str("-0");
    } else {
        let _ = write!(out, "{secs}");
    }
    if nanos > 0 {
        let fraction = if seconds < 0 {
            2 * NANOS_PER_SECOND - nanos
        } else {
            nanos + NANOS_PER_SECOND
        };
        // The leading "1" of the 10-digit number becomes the '.'.
        let digits = fraction.to_string();
        out.push('.');
        out.push_str(digits[1..].trim_end_matches('0'));
    }
    out.push('S');
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn intervals_match_java_period_duration() {
        // Expected strings produced by Java: Period.of(0, months, days) + " " + Duration.ofNanos(n).
        let cases: [(i32, i32, i64, &str); 15] = [
            (0, 1, 7_200_000_000_000, "P1D PT2H"),
            (14, 3, 14_706_500_000_000, "P14M3D PT4H5M6.5S"),
            (0, 0, -5_400_000_000_000, "P0D PT-1H-30M"),
            (0, 0, 0, "P0D PT0S"),
            (0, 0, -500_000_000, "P0D PT-0.5S"),
            (0, 0, -1_500_000_000, "P0D PT-1.5S"),
            (0, 0, 1, "P0D PT0.000000001S"),
            (-3, -2, 0, "P-3M-2D PT0S"),
            (12, 0, 0, "P12M PT0S"),
            (0, 0, 90_061_000_000_000, "P0D PT25H1M1S"),
            (0, 0, -1, "P0D PT-0.000000001S"),
            (0, 0, 59_000_000_000, "P0D PT59S"),
            (0, 0, -60_000_000_000, "P0D PT-1M"),
            (1, -1, 1_000_000_000, "P1M-1D PT1S"),
            (0, 0, 3_600_000_000_001, "P0D PT1H0.000000001S"),
        ];
        for (months, days, nanos, expected) in cases {
            let mut s = String::new();
            write_period(&mut s, months, days);
            s.push(' ');
            write_duration(&mut s, nanos);
            assert_eq!(s, expected, "({months}, {days}, {nanos})");
        }
    }
}
