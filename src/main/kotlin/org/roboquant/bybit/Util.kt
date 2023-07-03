package org.roboquant.bybit

import com.github.ajalt.mordant.rendering.TextColors.*
import java.time.Instant
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter

enum class BrokerType {
    SimBroker,
    ByBitBroker,
}

fun printBroker(brokerType: BrokerType, content: String ) {

    val nowLong = Instant.now().toEpochMilli()

    val prefix = if (brokerType == BrokerType.SimBroker) {
        "  " + blue(brokerType.toString())
    } else {
        yellow(brokerType.toString())
    }

    println("${nowLong} [${prefix}] ${content}")


}
