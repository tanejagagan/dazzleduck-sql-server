import LineChartD3 from "./charts/LineChartD3";
import BarChartD3 from "./charts/BarChartD3";
import PieChartD3 from "./charts/PieChartD3";

/**
 * Universal chart display component
 * Supports Line, Bar, and Pie charts.
 * Auto-detects numeric, timestamp, and label columns.
 * 
 * Props:
 *  - logs: Array of objects (query results)
 *  - view: "line" | "bar" | "pie"
 *  - width (optional): number → chart width
 *  - height (optional): number → chart height
 */
export default function DisplayCharts({ logs, view, width, height }) {
  if (!logs || logs.length === 0)
    return <p className="text-gray-500 text-center p-6">No data to visualize.</p>;

  const keys = Object.keys(logs[0]);
  if (keys.length === 0)
    return <p className="text-gray-500">No valid data columns.</p>;

  // Use only first 3 columns for visualization (ignore the rest)
  const usedKeys = keys.slice(0, 3);
  const [col1, col2, col3] = usedKeys;

  // Base chart design (with optional width/height override)
  let chartData = [];
  let design = {
    width: width || 1200,
    height: height || 430,
    xAxisField: "x",
    yAxisField: "value",
    xAxisLabel: col1,
    yAxisLabel: col2,
    showGrid: true,
    legendPosition: "top-right",
    fontFamily: "Inter, Helvetica, Arial",
    animation: true,
  };

  // Helper function — detect timestamp-like values and convert
  const formatPossibleDate = (val) => {
    if (val == null) return val;

    // If numeric string timestamp (e.g. "1735689600000")
    if (typeof val === "string" && /^\d{12,}$/.test(val.trim())) {
      const num = Number(val);
      return !isNaN(num) ? new Date(num).toLocaleString() : val;
    }

    // If number timestamp (e.g. 1735689600000)
    if (typeof val === "number" && String(val).length >= 12) {
      try {
        return new Date(val).toLocaleString();
      } catch {
        return val;
      }
    }

    // If date-like string (e.g. "2025-01-01T00:00:00")
    if (typeof val === "string" && /\d{4}-\d{2}-\d{2}/.test(val)) {
      try {
        return new Date(val).toLocaleString();
      } catch {
        return val;
      }
    }

    return val;
  };

  // -------------------------------------------------
  // PIE CHART (always works using first 2 columns)
  // -------------------------------------------------
  if (view === "pie") {
    const [k1, k2] = keys.slice(0, 2); // only first two columns used

    // Detect numeric vs label column
    const numericKey = logs.some(row => !isNaN(Number(row[k1]))) ? k1 :
                       logs.some(row => !isNaN(Number(row[k2]))) ? k2 : null;

    const labelKey = numericKey === k1 ? k2 : k1;

    const pieData = logs.map(row => ({
      label: String(formatPossibleDate(row[labelKey])),
      value: Number(row[numericKey]) || 0,
    }));

    return (
      <PieChartD3
        data={pieData}
        design={{
          width: width || 600,
          height: height || 400,
        }}
      />
    );
  }

  // -------------------------------------------------
  // LINE / BAR charts (3 columns or less)
  // -------------------------------------------------
  const xKey = col1;
  let yKey = col2;
  let zKey = col3;

  // Between col2 & col3 → detect numeric for Y axis
  const sample = logs.find(r => r[col2] !== undefined && r[col3] !== undefined);
  if (sample) {
    const isCol2Numeric = !isNaN(Number(sample[col2]));
    const isCol3Numeric = !isNaN(Number(sample[col3]));
    if (isCol3Numeric && !isCol2Numeric) {
      yKey = col3;
      zKey = col2;
    }
  }

  // Normalize data for D3 charts
  chartData = logs.map(row => {
    const xValue = formatPossibleDate(row[xKey]);
    const yValue = Number(row[yKey]) || 0;
    const zValue = zKey ? String(formatPossibleDate(row[zKey])) : "Series";

    return {
      x: xValue,
      value: yValue,
      dataset: zValue,
      label: zValue,
    };
  });

  // Update chart design
  design = {
    ...design,
    width: width || design.width,
    height: height || design.height,
    xAxisField: "x",
    yAxisField: "value",
    xAxisLabel: xKey,
    yAxisLabel: yKey,
    legendPosition: "top-right",
  };

  // -------------------------------------------------
  // Render chart
  // -------------------------------------------------
  if (view === "line") return <LineChartD3 data={chartData} design={design} />;
  if (view === "bar") return <BarChartD3 data={chartData} design={design} />;
  if (view === "pie")
    return <p className="text-gray-500">Pie chart requires at least 2 columns.</p>;

  return <p className="text-gray-500">Unsupported visualization</p>;
}
