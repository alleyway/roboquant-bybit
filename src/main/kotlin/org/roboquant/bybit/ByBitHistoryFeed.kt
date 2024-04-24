package org.roboquant.bybit

import bybit.sdk.rest.ByBitRestClient
import bybit.sdk.rest.market.KlineParams
import bybit.sdk.shared.Category
import org.roboquant.common.Asset
import org.roboquant.common.Logging
import org.roboquant.common.TimeSpan
import org.roboquant.feeds.Event
import org.roboquant.feeds.EventChannel
import org.roboquant.feeds.Feed
import org.roboquant.feeds.PriceBar
import java.lang.Integer.parseInt
import java.math.BigDecimal
import java.time.Duration
import java.time.Instant
import java.util.*

/**
 * Implementation of the Feed Interface that provides historical price (kline) data from ByBit using the GetKline endpoint.
 * These price bars will be sent to the roboquant event channel.
 *
 * @param configure additional configure logic, default is to do nothing
 * @param symbol the symbol to retrieve the data for
 * @param category the category of the symbol
 * @param interval the interval of the data, default is 1 minute
 * @param start the start time in milliseconds since epoch
 * @param end the end time in milliseconds since epoch, default is now
 * @param limit the maximum number of records to retrieve, default is 1000
 *
 * @constructor Create new ByBitHistoryFeed
 */
class ByBitHistoryFeed(
    configure: ByBitConfig.() -> Unit = {},
    private val symbol: String = "BTCUSDT",
    private val category: Category?,
    private val interval: String = "1",
    private var start: Long,
    private var end: Long?,
    private val limit: Int = 1000
) : Feed {

    private val logger = Logging.getLogger(ByBitHistoryFeed::class)
    private val client: ByBitRestClient
    private val config = ByBitConfig()
    private var duration = Duration.ofMinutes(1)

    init {
        config.configure()
        client = ByBit.getRestClient(config)

        if (end == null) {
            end = Instant.now().toEpochMilli()
        }

        duration = if (interval.toDoubleOrNull() != null) {
            Duration.ofMinutes(interval.toLong())
        } else {
            when (interval.uppercase(Locale.getDefault())) {
                "D" -> Duration.ofDays(1)
                "M" -> Duration.ofDays(30)
                "W" -> Duration.ofDays(7)
                else -> throw Exception("Unsupported interval $interval")
            }
        }
    }

    override suspend fun play(channel: EventChannel) {

        do {
            val response = client.marketClient.getKlineBlocking(
                KlineParams(
                    category = category,
                    symbol = symbol,
                    interval = interval,
                    start = start,
                    end = end,
                    limit = limit
                )
            )

            response.result.list.reversed().forEach { stringList ->
                val startTime = Instant.ofEpochMilli(stringList[0].toLong())
                val open = BigDecimal(stringList[1])
                val high = BigDecimal(stringList[2])
                val low = BigDecimal(stringList[3])
                val close = BigDecimal(stringList[4])
                val volume = BigDecimal(stringList[5])
                // val turnover = BigDecimal(stringList[6])

                val asset = Asset(symbol)
                val action = PriceBar(
                    asset,
                    open,
                    high,
                    low,
                    close,
                    volume,
                    TimeSpan(0, 0, 0, 0, parseInt(interval))
                )

                val actions = listOf(action)
                val event = Event(actions, startTime.plusSeconds(duration.toSeconds()))
                logger.info { "Sending event at ${event.time} with ${event.actions.size} actions" }

                channel.send(event)
            }

            start += duration.toMillis() * limit.toLong()
        } while (start < end!!)
    }
}
