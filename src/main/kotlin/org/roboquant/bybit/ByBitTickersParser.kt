package org.roboquant.bybit

import bybit.sdk.websocket.ByBitWebSocketMessage
import org.roboquant.common.Asset
import org.roboquant.common.Logging
import org.roboquant.feeds.PriceQuote


class ByBitTickersParser {

    private val logger = Logging.getLogger(this::class)

    private var cachedQuote: PriceQuote? = null

    fun parse(asset: Asset, message: ByBitWebSocketMessage.TopicResponse.TickerLinearInverse): PriceQuote {

        if (message.type == "snapshot") {
            clear()
        }

        val askPrice = message.data.ask1Price ?: cachedQuote?.askPrice ?: throw IllegalArgumentException()
        val askSize = message.data.ask1Size ?: cachedQuote?.askSize ?: throw IllegalArgumentException()

        val bidPrice = message.data.bid1Price ?: cachedQuote?.bidPrice ?: throw IllegalArgumentException()
        val bidSize = message.data.bid1Size ?: cachedQuote?.bidSize ?: throw IllegalArgumentException()

        return PriceQuote(asset, askPrice, askSize, bidPrice, bidSize)
    }

    fun clear() {
        cachedQuote = null
    }

}
