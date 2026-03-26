package io.dazzleduck.sql.otel.collector;

import io.dazzleduck.sql.common.types.JavaRow;
import io.opentelemetry.proto.common.v1.InstrumentationScope;
import io.opentelemetry.proto.metrics.v1.ExponentialHistogramDataPoint;
import io.opentelemetry.proto.metrics.v1.HistogramDataPoint;
import io.opentelemetry.proto.metrics.v1.Metric;
import io.opentelemetry.proto.metrics.v1.NumberDataPoint;
import io.opentelemetry.proto.metrics.v1.SummaryDataPoint;
import io.opentelemetry.proto.resource.v1.Resource;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

/**
 * Converts an OTLP Metric (with Resource and InstrumentationScope context)
 * into a list of JavaRows matching OtelMetricSchema.
 * One row is emitted per data point. Columns not relevant to the metric type are null.
 */
public class MetricConverter {

    public static List<JavaRow> toRows(Metric metric, Resource resource, InstrumentationScope scope) {
        List<JavaRow> rows = new ArrayList<>();
        switch (metric.getDataCase()) {
            case GAUGE -> {
                for (var dp : metric.getGauge().getDataPointsList()) {
                    rows.add(gaugeRow(metric, resource, scope, dp));
                }
            }
            case SUM -> {
                var sum = metric.getSum();
                String temporality = stripAggTemporalityPrefix(sum.getAggregationTemporality().name());
                for (var dp : sum.getDataPointsList()) {
                    rows.add(numberRow(metric, resource, scope, dp, "SUM",
                            sum.getIsMonotonic(), temporality));
                }
            }
            case HISTOGRAM -> {
                var hist = metric.getHistogram();
                String temporality = stripAggTemporalityPrefix(hist.getAggregationTemporality().name());
                for (var dp : hist.getDataPointsList()) {
                    rows.add(histogramRow(metric, resource, scope, dp, temporality));
                }
            }
            case EXPONENTIAL_HISTOGRAM -> {
                var expHist = metric.getExponentialHistogram();
                String temporality = stripAggTemporalityPrefix(expHist.getAggregationTemporality().name());
                for (var dp : expHist.getDataPointsList()) {
                    rows.add(expHistogramRow(metric, resource, scope, dp, temporality));
                }
            }
            case SUMMARY -> {
                for (var dp : metric.getSummary().getDataPointsList()) {
                    rows.add(summaryRow(metric, resource, scope, dp));
                }
            }
            default -> {} // DATA_NOT_SET — ignore
        }
        return rows;
    }

    private static JavaRow gaugeRow(Metric metric, Resource resource, InstrumentationScope scope,
                                    NumberDataPoint dp) {
        return numberRow(metric, resource, scope, dp, "GAUGE", null, null);
    }

    private static JavaRow numberRow(Metric metric, Resource resource, InstrumentationScope scope,
                                     NumberDataPoint dp, String metricType,
                                     Boolean isMonotonic, String aggregationTemporality) {
        Object[] v = baseRow(metric, resource, scope, metricType,
                dp.getStartTimeUnixNano(), dp.getTimeUnixNano(),
                LogRecordConverter.kvListToMap(dp.getAttributesList()));
        switch (dp.getValueCase()) {
            case AS_DOUBLE -> v[OtelMetricSchema.COL_VALUE_DOUBLE] = dp.getAsDouble();
            case AS_INT    -> v[OtelMetricSchema.COL_VALUE_INT] = dp.getAsInt();
            default -> {}
        }
        v[OtelMetricSchema.COL_IS_MONOTONIC] = isMonotonic;
        v[OtelMetricSchema.COL_AGGREGATION_TEMPORALITY] = aggregationTemporality;
        return new JavaRow(v);
    }

    private static JavaRow histogramRow(Metric metric, Resource resource, InstrumentationScope scope,
                                        HistogramDataPoint dp, String aggregationTemporality) {
        Object[] v = baseRow(metric, resource, scope, "HISTOGRAM",
                dp.getStartTimeUnixNano(), dp.getTimeUnixNano(),
                LogRecordConverter.kvListToMap(dp.getAttributesList()));
        v[OtelMetricSchema.COL_COUNT] = dp.getCount();
        v[OtelMetricSchema.COL_SUM] = dp.hasSum() ? dp.getSum() : null;
        v[OtelMetricSchema.COL_BUCKET_COUNTS] = dp.getBucketCountsList().isEmpty() ? null : dp.getBucketCountsList();
        v[OtelMetricSchema.COL_EXPLICIT_BOUNDS] = dp.getExplicitBoundsList().isEmpty() ? null : dp.getExplicitBoundsList();
        v[OtelMetricSchema.COL_AGGREGATION_TEMPORALITY] = aggregationTemporality;
        return new JavaRow(v);
    }

    private static JavaRow expHistogramRow(Metric metric, Resource resource, InstrumentationScope scope,
                                           ExponentialHistogramDataPoint dp, String aggregationTemporality) {
        Object[] v = baseRow(metric, resource, scope, "EXPONENTIAL_HISTOGRAM",
                dp.getStartTimeUnixNano(), dp.getTimeUnixNano(),
                LogRecordConverter.kvListToMap(dp.getAttributesList()));
        v[OtelMetricSchema.COL_COUNT] = dp.getCount();
        v[OtelMetricSchema.COL_SUM] = dp.hasSum() ? dp.getSum() : null;
        v[OtelMetricSchema.COL_AGGREGATION_TEMPORALITY] = aggregationTemporality;
        return new JavaRow(v);
    }

    private static JavaRow summaryRow(Metric metric, Resource resource, InstrumentationScope scope,
                                      SummaryDataPoint dp) {
        Object[] v = baseRow(metric, resource, scope, "SUMMARY",
                dp.getStartTimeUnixNano(), dp.getTimeUnixNano(),
                LogRecordConverter.kvListToMap(dp.getAttributesList()));
        v[OtelMetricSchema.COL_COUNT] = dp.getCount();
        v[OtelMetricSchema.COL_SUM] = dp.getSum();
        v[OtelMetricSchema.COL_QUANTILE_VALUES] = quantileValuesToList(dp);
        return new JavaRow(v);
    }

    private static Object[] baseRow(Metric metric, Resource resource, InstrumentationScope scope,
                                    String metricType, long startNanos, long timeNanos,
                                    java.util.Map<String, String> attributes) {
        Object[] v = new Object[19];
        v[OtelMetricSchema.COL_NAME] = metric.getName();
        v[OtelMetricSchema.COL_DESCRIPTION] = LogRecordConverter.emptyToNull(metric.getDescription());
        v[OtelMetricSchema.COL_UNIT] = LogRecordConverter.emptyToNull(metric.getUnit());
        v[OtelMetricSchema.COL_METRIC_TYPE] = metricType;
        v[OtelMetricSchema.COL_START_TIME_MS] = startNanos > 0 ? startNanos / 1_000_000L : null;
        v[OtelMetricSchema.COL_TIME_MS] = timeNanos > 0 ? timeNanos / 1_000_000L : null;
        v[OtelMetricSchema.COL_ATTRIBUTES] = attributes;
        v[OtelMetricSchema.COL_RESOURCE_ATTRIBUTES] = resource != null
                ? LogRecordConverter.kvListToMap(resource.getAttributesList())
                : new LinkedHashMap<>();
        v[OtelMetricSchema.COL_SCOPE_NAME] = scope != null ? LogRecordConverter.emptyToNull(scope.getName()) : null;
        v[OtelMetricSchema.COL_SCOPE_VERSION] = scope != null ? LogRecordConverter.emptyToNull(scope.getVersion()) : null;
        return v;
    }

    private static List<Object[]> quantileValuesToList(SummaryDataPoint dp) {
        if (dp.getQuantileValuesCount() == 0) return null;
        List<Object[]> result = new ArrayList<>();
        for (var qv : dp.getQuantileValuesList()) {
            result.add(new Object[]{qv.getQuantile(), qv.getValue()});
        }
        return result;
    }

    private static String stripAggTemporalityPrefix(String name) {
        String prefix = "AGGREGATION_TEMPORALITY_";
        return name.startsWith(prefix) ? name.substring(prefix.length()) : name;
    }

}
