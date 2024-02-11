package org.roboquant.bybit

import bybit.sdk.websocket.ByBitWebSocketMessage
import org.roboquant.common.Asset
import org.roboquant.common.Logging
import org.roboquant.feeds.OrderBook

class ByBitOrderBookParser {

    private val logger = Logging.getLogger(this::class)

    private var cachedAsks: MutableMap<Double, Pair<Double, Int>> = mutableMapOf()
    private var cachedBids: MutableMap<Double, Pair<Double, Int>> = mutableMapOf()


    fun parse(asset: Asset, message: ByBitWebSocketMessage.TopicResponse.Orderbook): OrderBook {

        val u = message.data.updateId

        if (message.type == "snapshot" || u == 1) {
            cachedBids.clear()
            cachedAsks.clear()
        }

        message.data.asks.forEach {
            // 0 = price, 1 = volume
            if (it[1] == 0.0) {
                cachedAsks.remove(it[0])
            } else {
                cachedAsks[it[0]] = Pair(it[1], u)
            }
        }

        message.data.bids.forEach {
            if (it[1] == 0.0) {
                cachedBids.remove(it[0])
            } else {
                cachedBids[it[0]] = Pair(it[1], u)
            }
        }

        val nonZeroAsks = cachedAsks.entries.filter { it.value.first > 0 }.sortedBy { it.key }
        val nonZeroBids = cachedBids.entries.filter { it.value.first > 0 }.sortedBy { it.key }

        val bestBid = nonZeroBids.lastOrNull()
        val bestAsk = nonZeroAsks.firstOrNull()

        if (bestBid != null && bestAsk != null && bestBid.key > bestAsk.key) {
            // one side or the other has an ask/bid out of sync
            logger.warn("invalid cached orderbook data detected")
            if (bestAsk.value.second < bestBid.value.second) {
                // looks like bestAsk is older sequence, so delete it
                cachedAsks.remove(bestAsk.key)
            } else {
                cachedBids.remove(bestBid.key)
            }
        }

        val asks: List<OrderBook.OrderBookEntry> =
            cachedAsks.entries.map { OrderBook.OrderBookEntry(it.value.first, it.key) }

        val bids: List<OrderBook.OrderBookEntry> =
            cachedBids.entries.map { OrderBook.OrderBookEntry(it.value.first, it.key) }
        return OrderBook(asset, asks, bids)
    }

    fun clear() {
        cachedBids.clear()
        cachedAsks.clear()
    }

}
