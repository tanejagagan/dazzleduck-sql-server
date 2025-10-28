import LineChartD3 from "./charts/LineChartD3";
import AreaChartD3 from "./charts/AreaChartD3";
import BarChartD3 from "./charts/BarChartD3";
import PieChartD3 from "./charts/PieChartD3";

export default function DisplayCharts({ logs, view }) {
    if (!logs || logs.length === 0) {
        return <p className="text-gray-500 text-center p-6">No data to visualize.</p>;
    }

    // -------------------------- detect keys --------------------------
    const keys = Object.keys(logs[0]);
    if (keys.length === 0) {
        return <p className="text-gray-500">No valid data columns.</p>;
    }

    const xKey = keys[0]; // first column → x-axis
    const yKeys = keys.slice(1); // remaining columns → y-axis datasets

    // -------------------------- normalize data --------------------------
    const formatted = logs.map((row, i) => {
        const xValue = row[xKey];
        let formattedX = xValue;

        // Convert timestamp → readable date
        if (typeof xValue === "number" && String(xValue).length >= 12) {
            try {
                formattedX = new Date(xValue).toLocaleDateString();
            } catch {
                formattedX = String(xValue);
            }
        }

        // Make it unique per row (optional: include another field even if duplicates)
        const uniqueX = `${formattedX} #${i + 1}`;

        return {
            x: uniqueX,
            ...Object.fromEntries(
                yKeys.map(k => [k, isNaN(row[k]) ? row[k] : Number(row[k])])
            ),
        };
    });

    // -------------------------- reshape data for different types of charts --------------------------
    const chartData = [];

    formatted.forEach(row => {
        yKeys.forEach(k => {
            const val = row[k];
            if (val !== null && val !== undefined && val !== "") {
                chartData.push({
                    x: row.x,
                    value: typeof val === "number" ? val : 1, // numeric for chart
                    label: val,                               // original value for display
                    dataset: k,
                    __raw: row,
                });
            }
        });
    });

    // -------------------------- chart design --------------------------
    const design = {
        width: 1200,
        height: 430,
        xAxisField: "x",
        yAxisField: "value",
        xAxisLabel: xKey,
        yAxisLabel: yKeys.join(", "),
        showGrid: true,
        legendPosition: "top",
        fontFamily: "Inter, Helvetica, Arial",
        animation: true,
    };

    // -------------------------- render chart type --------------------------
    if (view === "line") return <LineChartD3 data={chartData} design={design} />;
    if (view === "area") return <AreaChartD3 data={chartData} design={design} />;
    if (view === "bar") return <BarChartD3 data={chartData} design={design} />;

     if (view === "pie") {
        const labelKey = xKey;
        const valueKeys = yKeys;

        // Flatten all values into { label, value, key }
        const pieData = logs.flatMap(row =>
            valueKeys.map(k => ({
                labelKey: labelKey,
                label: labelKey == "dt" ? new Date(row[labelKey]).toLocaleDateString() : row[labelKey],
                valueKey: k,
                value: Number(row[k]) || 0, // can set default(0) to 1 or > if want to display non numaric data too
                defValue: row[k],
            }))
        );

        return <PieChartD3 data={pieData} design={{ width: 600, height: 400 }} />;
    }

    return <p className="text-gray-500">Unsupported visualization</p>;
}
