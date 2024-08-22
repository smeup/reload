package com.smeup.dbnative.log

import io.opentelemetry.sdk.common.CompletableResultCode
import io.opentelemetry.sdk.trace.data.SpanData
import io.opentelemetry.sdk.trace.export.SpanExporter

class CustomSpanExporter(private val exporter: SpanExporter) : SpanExporter {

    private val logger = Logger.getSimpleInstance()

    override fun export(spans: Collection<SpanData>): CompletableResultCode {
        spans.forEach { span ->
            val startTime = span.startEpochNanos / 1_000_000
            val endTime = span.endEpochNanos / 1_000_000
            val duration = endTime - startTime

            // Custom formatted log without extra info
            val formattedLog = span.name.trimIndent()

            logger.logEvent(LoggingKey.performance_metric, formattedLog, duration)
        }
        return CompletableResultCode.ofSuccess()
    }

    override fun flush(): CompletableResultCode {
        return exporter.flush()
    }

    override fun shutdown(): CompletableResultCode {
        return exporter.shutdown()
    }
}