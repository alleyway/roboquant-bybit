package org.roboquant.bybit

import bybit.sdk.rest.ByBitRestClient
import bybit.sdk.websocket.*
import kotlinx.coroutines.runBlocking
import org.roboquant.bybit.ByBit.availableAssets
import org.roboquant.bybit.ByBit.getRestClient
import org.roboquant.bybit.ByBit.getWebSocketClient
import org.roboquant.common.*
import org.roboquant.feeds.*
import java.lang.Integer.parseInt
import java.time.Instant

/**
 * Types of Price actions that can be subscribed to
 */
enum class ByBitActionType {

    /**
     * [TradePrice] actions
     */
    TRADE,

    /**
     * [PriceQuote] actions
     */
    QUOTE,

    /**
     * [OrderBook] actions
     */
    ORDERBOOK,

    /**
     * [PriceBar] actions aggregated per minute
     */
    BAR_PER_MINUTE,
}

/**
 *
 * @param configure additional configuration logic
 * @property useComputerTime use the computer time to stamp events or use the ByBit-supplied timestamps,
 * default is true
 */
class ByBitLiveFeed(
    configure: ByBitConfig.() -> Unit = {},
    private val endpoint: ByBitEndpoint,
    private val useComputerTime: Boolean = true
) : LiveFeed(), AssetFeed {

    private val config = ByBitConfig()
    private var client: ByBitRestClient
    private var wsClient: ByBitWebSocketClient
    private val logger = Logging.getLogger(ByBitLiveFeed::class)
    private val subscriptions = mutableMapOf<String, Asset>()
    private var cachedAsks: MutableMap<Double, Double> = mutableMapOf()
    private var cachedBids: MutableMap<Double, Double> = mutableMapOf()

    init {
        config.configure()
        require(config.apiKey.isNotBlank()) { "No api key provided" }
        require(config.secret.isNotBlank()) { "No secret provided" }

        val wsOptions = WSClientConfigurableOptions(
            endpoint,
            config.apiKey,
            config.secret,
            config.testnet
        )

        wsClient = getWebSocketClient(wsOptions, this::handler)
        client = getRestClient(config)

        runBlocking {
            wsClient.connect()
        }
    }

    val availableAssets: Map<String, Asset> by lazy {
        availableAssets(client)
    }

    override val assets
        get() = subscriptions.values.toSortedSet()


    private fun getTime(endTime: Long?): Instant {
        return if (useComputerTime || endTime == null) Instant.now() else Instant.ofEpochMilli(endTime)
    }

    /**
     * Get the full asset based on the symbol (aka ticker)
     */
    private fun getSubscribedAsset(symbol: String?): Asset {
        return subscriptions.getValue(symbol!!)
    }


    /**
     * Handle incoming messages
     */
    private fun handler(message: ByBitWebSocketMessage) {

        when (message) {
            is ByBitWebSocketMessage.RawMessage -> logger.info(message.data)

            is ByBitWebSocketMessage.TopicResponse.PublicTrade -> {
                message.data.forEach {
                    val asset = getSubscribedAsset(it.symbol)
                    val action = TradePrice(asset, it.price, it.volume ?: Double.NaN)
                    send(Event(listOf(action), getTime(it.timestamp)))
                }
            }

            is ByBitWebSocketMessage.TopicResponse.Orderbook -> {

                val asset = getSubscribedAsset(message.data.symbol)
                if (message.type == "snapshot") {
                    cachedBids.clear()
                    cachedAsks.clear()
                }

                message.data.asks.forEach {
                    cachedAsks.put(it[0], it[1])
                }
                message.data.bids.forEach {
                    cachedBids.put(it[0], it[1])
                }

                val asks: List<OrderBook.OrderBookEntry> =
                    cachedAsks.entries.map { OrderBook.OrderBookEntry(it.value, it.key) }

                val bids: List<OrderBook.OrderBookEntry> =
                    cachedBids.entries.map { OrderBook.OrderBookEntry(it.value, it.key) }

                val action = OrderBook(asset, asks, bids)

                send(Event(listOf(action), getTime(message.ts)))
            }

            is ByBitWebSocketMessage.TopicResponse.TickerLinearInverse -> {
                if (message.type == "snapshot") {
                    val asset = getSubscribedAsset(message.data.symbol)
                    val action = PriceQuote(
                        asset,
                        message.data.ask1Price!!,
                        message.data.ask1Size ?: Double.NaN,
                        message.data.bid1Price!!,
                        message.data.bid1Size ?: Double.NaN,
                    )
                    send(Event(listOf(action), getTime(message.ts)))
                } else {
                    logger.warn { "non-snapshot received for TickerLinearInverse" }
                }
            }

//            is ByBitWebSocketMessage.TopicResponse.TickerSpot -> {
//                // TODO: if delta instead of snapshot might be some values empty...
//                // should cache when snapshot
//                val asset = getSubscribedAsset(message.data.symbol)
//                val action = PriceQuote(
//                    asset,
//                    message.data.ask1Price!!,
//                    message.data.ask1Size ?: Double.NaN,
//                    message.data.bid1Price!!,
//                    message.data.bid1Size ?: Double.NaN,
//                )
//                send(Event(listOf(action), getTime(message.ts)))
//            }

            is ByBitWebSocketMessage.TopicResponse.Kline -> {

                val symbol = message.topic?.split(".")?.asReversed()?.get(0)
                if (message.type == "snapshot") {
                    message.data.forEach {
                        if (it.confirm) { // only send values that are confirmed
                            val asset = getSubscribedAsset(symbol)

                            val action = PriceBar(
                                asset,
                                it.open,
                                it.high,
                                it.low,
                                it.close,
                                it.volume,
                                TimeSpan(0, 0, 0, 0, parseInt(it.interval))
                            )
                            println(action)
                            send(Event(listOf(action), getTime(it.timestamp)))
                        }
                    }
                }
            }

            is ByBitWebSocketMessage.StatusMessage -> {
                if (message.success == false) {
                    logger.error("Error: ${message.retMsg}")
                } else {
                    logger.info(message.retMsg)
                }
            }

            else -> logger.warn("received message=$message")
        }
    }

    /**
     * Subscribe to the [symbols] for the specified action [type], default action is `ByBitActionType.TRADE`
     */
    fun subscribe(vararg symbols: String, type: ByBitActionType = ByBitActionType.TRADE) {

//        val idPrefix = when (endpoint) {
//            ByBitEndpoint.Spot -> {
//                "spot"
//            }
//
//            ByBitEndpoint.Linear, ByBitEndpoint.Inverse -> {
//                "linearInverse"
//            }
//
//            ByBitEndpoint.Option -> {
//                "option"
//            }
//
//            else -> {
//                "unknown"
//            }
//        }

        val assets = symbols.map {
            Asset(
                it, AssetType.CRYPTO, exchange = Exchange.CRYPTO, currency = Currency.USDT,
//                id = "$idPrefix:$it"
            )
        }
            .associateBy { it.symbol }
        subscriptions.putAll(assets)

        val bybitSubs = when (type) {

            ByBitActionType.TRADE -> symbols.map {
                ByBitWebSocketSubscription(ByBitWebsocketTopic.Trades, it)
            }

            ByBitActionType.QUOTE -> symbols.map {
                ByBitWebSocketSubscription(ByBitWebsocketTopic.Tickers, it)
            }

            ByBitActionType.BAR_PER_MINUTE -> symbols.map {
                ByBitWebSocketSubscription(ByBitWebsocketTopic.Kline.One_Minute, it)
            }

            ByBitActionType.ORDERBOOK -> symbols.map {
                ByBitWebSocketSubscription(ByBitWebsocketTopic.Orderbook.Level_50, it)
            }
        }

        wsClient.subscribeBlocking(bybitSubs)
    }

    /**
     * Disconnect from ByBit server and stop receiving market data
     */
    fun disconnect() {
        wsClient.disconnectBlocking()
    }


}
