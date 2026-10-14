"""Java CPU by subsystem from a JFR recording of OtelCollectorBenchmark's steady mode.

The method of #445: offer the SAME fixed rate to two builds and compare the CPU each spends,
alternating builds over two recordings each. protobuf decode is the control - nothing changes it,
so when its count moves between builds the other rows of that pair are noise, not a result.

1. Record (from the repo root; the surefire fork writes the larger .jfr):
     JAVA_TOOL_OPTIONS="-XX:StartFlightRecording=filename=/tmp/rec-%p.jfr,settings=profile" \
     ./mvnw -o test -pl dazzleduck-sql-otel-collector -Dtest=OtelCollectorBenchmark \
       -Dbenchmark.rate=60000 -Dbenchmark.duration=120 -Dbenchmark.batch=500 \
       -Dbenchmark.clients=96 -Dbenchmark.attributes=realistic | tee bench.log
   Check the log says "requests behind schedule 0", or the two builds did different work.
2. Export. --stack-depth matters: jfr print keeps only 5 frames by default, which silently
   drops the parent frames every category below is matched on.
     for ev in ExecutionSample GarbageCollection CPULoad; do
       jfr print --json --stack-depth 128 --events jdk.$ev /tmp/rec-PID.jfr > $ev.json; done
3. Summarize the measured window, from the STEADY-START / STEADY-END epoch-ms lines in bench.log:
     python3 -I jfr_breakdown.py ExecutionSample.json GarbageCollection.json CPULoad.json START END

A sample counts toward every category whose frames it contains (inclusive), so rows overlap.
"""
import json, re, sys
from datetime import datetime

def ts(s):  # '2026-10-14T13:07:01.123456789-07:00' -> epoch ms
    m = re.match(r'(.*T\d\d:\d\d:\d\d)(\.\d+)?(.*)', s)
    frac = float('0' + (m.group(2) or '.0'))
    base = datetime.fromisoformat(m.group(1) + (m.group(3) or '+00:00').replace('Z', '+00:00'))
    return base.timestamp() * 1000 + frac * 1000

def dur_ms(v):
    if isinstance(v, (int, float)): return v / 1e6
    m = re.match(r'PT(?:(\d+)M)?([\d.]+)S', v)
    if m: return int(m.group(1) or 0) * 60000 + float(m.group(2)) * 1000
    m = re.match(r'([\d.]+)\s*(ns|us|ms|s)', v)
    return float(m.group(1)) * {'ns': 1e-6, 'us': 1e-3, 'ms': 1, 's': 1000}[m.group(2)]

CATEGORIES = [
    ('batch writers (Log/Span/MetricBatchWriter.write)', r'collector/(Log|Span|Metric)\w*BatchWriter\.write$'),
    ('  attribute maps', r'collector/(AttributeMapsWriter[.$]|MapColumnWriter\.write$)'),
    ('Arrow vector ops (any caller)', r'^org/apache/arrow/vector/(?!ipc)'),
    ('UTF-8 encode (String.getBytes)', r'^java/lang/String\.(getBytes|encode)'),
    ('protobuf decode (control)', r'(^com/google/protobuf/CodedInputStream|parseFrom|MessageMarshaller\.parse)'),
    ('Arrow IPC file write', r'^org/apache/arrow/vector/ipc/'),
    ('ingestion queue (commons)', r'^io/dazzleduck/sql/commons/ingestion/'),
]

samples_file, gc_file, cpu_file, start, end = sys.argv[1:6]
start, end = float(start), float(end)
events = [e for e in json.load(open(samples_file))['recording']['events'] if start <= ts(e['values']['startTime']) <= end]
counts = {name: 0 for name, _ in CATEGORIES}
for e in events:
    frames = [f['method']['type']['name'] + '.' + f['method']['name'] for f in e['values']['stackTrace']['frames']]
    for name, pattern in CATEGORIES:
        rx = re.compile(pattern)
        if any(rx.search(f) for f in frames):
            counts[name] += 1
print(f"  {'Java CPU samples (all threads)':<50} {len(events):>7}")
for name, _ in CATEGORIES:
    print(f"  {name:<50} {counts[name]:>7}")
gcs = [e for e in json.load(open(gc_file))['recording']['events'] if start <= ts(e['values']['startTime']) <= end]
pauses = [dur_ms(e['values']['sumOfPauses']) for e in gcs]
print(f"  {'GC count / total pause / max pause':<50} {len(gcs)} / {sum(pauses):.0f} ms / {max(pauses, default=0):.1f} ms")
cpu = [e for e in json.load(open(cpu_file))['recording']['events'] if start <= ts(e['values']['startTime']) <= end]
if cpu:
    jvm = sum(e['values']['jvmUser'] + e['values']['jvmSystem'] for e in cpu) / len(cpu)
    print(f"  {'process CPU, mean % of machine (Java + native)':<50} {100 * jvm:.1f}%")
