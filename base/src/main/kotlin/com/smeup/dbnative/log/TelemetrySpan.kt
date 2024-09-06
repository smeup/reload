package com.smeup.dbnative.log

import io.opentelemetry.api.GlobalOpenTelemetry
import io.opentelemetry.api.trace.Span
import io.opentelemetry.context.Context
import io.opentelemetry.context.Scope

class TelemetrySpan(spanName: String) {

    private var span: Span? = null
    private var scope: Scope? = null

    init {
        val serviceName = System.getenv("OTEL_SERVICE_NAME") ?: "defaultServiceName"

        val tracer = GlobalOpenTelemetry.getTracer(serviceName)

        span = tracer.spanBuilder(spanName)
            .setParent(Context.current())
            .startSpan()

        scope = span?.makeCurrent()
    }


    fun endSpan() {
        span?.end()
        scope?.close()
    }

}
