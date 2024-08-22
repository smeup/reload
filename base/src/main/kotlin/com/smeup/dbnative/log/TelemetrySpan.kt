package com.smeup.dbnative.log

import io.opentelemetry.api.GlobalOpenTelemetry
import io.opentelemetry.api.trace.Span
import io.opentelemetry.context.Context
import io.opentelemetry.context.Scope
import io.opentelemetry.sdk.OpenTelemetrySdk
import io.opentelemetry.sdk.trace.SdkTracerProvider
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor
import io.opentelemetry.exporter.logging.LoggingSpanExporter

class TelemetrySpan(spanName: String) {

    private var span: Span? = null
    private var scope: Scope? = null

    init {

        val ot = initializeOpenTelemetry()

        val tracerServiceName = System.getenv("OTEL_SERVICE_NAME") ?: "defaultReloadTracer"
        val tracer = ot?.tracerProvider?.get(tracerServiceName) ?: GlobalOpenTelemetry.getTracer(tracerServiceName)

        span = tracer.spanBuilder(spanName)
            .setParent(Context.current())
            .startSpan()

        scope = span?.makeCurrent()
    }

    private fun initializeOpenTelemetry(): OpenTelemetrySdk? {

        val isConsoleLoggingEnabled = System.getenv("ENABLE_CONSOLE_LOGGING")?.toBoolean() ?: true

        if (isConsoleLoggingEnabled) {
            val loggingExporter = CustomSpanExporter(LoggingSpanExporter.create())
            val tracerProvider = SdkTracerProvider.builder()
                .addSpanProcessor(SimpleSpanProcessor.create(loggingExporter))
                .build()

            return OpenTelemetrySdk.builder().setTracerProvider(tracerProvider).build()
        }


        return null
    }

    fun endSpan() {
        span?.end()
        scope?.close()
    }

}
