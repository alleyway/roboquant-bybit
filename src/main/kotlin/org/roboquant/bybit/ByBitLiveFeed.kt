package org.roboquant.bybit

import bybit.sdk.rest.ByBitRestClient
import bybit.sdk.rest.market.PublicTradingHistoryParams
import bybit.sdk.shared.Side
import bybit.sdk.shared.toCategory
import bybit.sdk.websocket.*
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.BufferOverflow
import org.roboquant.bybit.ByBit.getRestClient
import org.roboquant.common.*
import org.roboquant.feeds.*
import java.lang.Integer.parseInt
import java.time.Instant


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
    private val subscriptions = mutableMapOf<String, Asset>() //maps symbol to asset
    private val bybitSubscriptions: MutableList<ByBitWebSocketSubscription> = mutableListOf()

    private val orderBookParser =  ByBitOrderBookParser()

    private val cachedLinearInverseItems = mutableMapOf<Asset, ByBitWebSocketMessage.TickerLinearInverseItem>()

    private var recentTradeHistoryQueue: MutableList<Event> = mutableListOf()

    init {
        config.configure()
        val wsOptions = WSClientConfigurableOptions(
            endpoint,
            config.apiKey,
            config.secret,
            config.testnet,
            name = "ByBitLiveFeed"
        )

        client = getRestClient(config)
        wsClient = ByBit.getWebSocketClient(wsOptions)

        val scope = CoroutineScope(Dispatchers.Default + Job())
        scope.launch {
            wsClient.connect(listOf())
            // Buffer X amount. Drop older "public" data: trades/tickers/orderbook/liquidations
            val channel = wsClient.getWebSocketEventChannel(10, BufferOverflow.DROP_OLDEST)
            while (true) {
                val msg = channel.receive()
                (this@ByBitLiveFeed::handler)(msg)
            }
        }

        runBlocking {
            delay(3000)
        }
    }

//    val availableAssets: Map<String, Asset> by lazy {
//        availableAssets(client)
//    }

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
    @Suppress("CyclomaticComplexMethod")
    private fun handler(message: ByBitWebSocketMessage) {

        when (message) {
            is ByBitWebSocketMessage.RawMessage -> {
                logger.info(message.data)
            }

            is ByBitWebSocketMessage.TopicResponse.PublicTrade -> {
                message.data.forEach {
                    val asset = getSubscribedAsset(it.symbol)
//                    val action = TradePriceByBit(asset, it.price, it.volume ?: Double.NaN, it.tickDirection)

                    val sign = if (it.side == Side.Sell) -1 else 1
                    val action = TradePriceByBit(asset, it.price, it.volume.times(sign), it.tickDirection)
                    send(Event(listOf(action), getTime(it.timestamp)))
                }
            }

            is ByBitWebSocketMessage.TopicResponse.Orderbook -> {

                val asset = getSubscribedAsset(message.data.symbol)

                val orderBook: OrderBook = orderBookParser.parse(asset, message)

                if (orderBook.asks.isNotEmpty() && orderBook.bids.isNotEmpty()) {
                    send(Event(listOf(orderBook), getTime(message.ts)))
                }
            }

            is ByBitWebSocketMessage.TopicResponse.TickerLinearInverse -> {
                val asset = getSubscribedAsset(message.data.symbol)

                if (message.type == "snapshot"
                    && cachedLinearInverseItems[asset] !== null
                ) {
                    cachedLinearInverseItems.remove(asset)
                }

                val cached = cachedLinearInverseItems[asset] ?: message.data

                message.data.let {
                    val newCached = ByBitWebSocketMessage.TickerLinearInverseItem(
                        cached.symbol,
                        price24hPcnt = it.price24hPcnt ?: cached.price24hPcnt,
                        fundingRate = it.fundingRate ?: cached.fundingRate,
                        markPrice = it.markPrice ?: cached.markPrice,
                        indexPrice = it.indexPrice ?: cached.indexPrice,
                        openInterestValue = it.openInterestValue ?: cached.openInterestValue,
                        bid1Price = it.bid1Price ?: cached.bid1Price,
                        bid1Size = it.bid1Size ?: cached.bid1Size,
                        ask1Price = it.ask1Price ?: cached.ask1Price,
                        ask1Size = it.ask1Size ?: cached.ask1Size,
                    )
                    cachedLinearInverseItems[asset] = newCached
                }

                val c = cachedLinearInverseItems[asset]!!

                if (
                    c.ask1Price !== null &&
                    c.ask1Size !== null &&
                    c.bid1Price !== null &&
                    c.bid1Size !== null &&
                    c.markPrice !== null
                ) {
                    val action = PriceQuoteByBitLinearInverse(asset,
                        askPrice = c.ask1Price!!,
                        askSize = c.ask1Size!!,
                        bidPrice = c.bid1Price!!,
                        bidSize = c.bid1Size!!,
                        markPrice = c.markPrice!!
                    )
                    send(Event(listOf(action), getTime(message.ts)))
                }
            }

            is ByBitWebSocketMessage.TopicResponse.Liquidation -> {
                val liqItem = message.data
                val asset = getSubscribedAsset(liqItem.symbol)
                val action = Liquidation(
                    asset,
                    liqItem.price,
                    liqItem.side,
                    liqItem.size
                )
                send(Event(listOf(action), getTime(message.ts)))
            }

//            is ByBitWebSocketMessage.TopicResponse.TickerSpot -> {
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
                    logger.trace(message.retMsg)
                }
            }

            else -> logger.warn("received message=$message")
        }
    }

    // creates some events that the algo can use to "warm up"
    private fun fetchRecentTradeHistoryEvents(symbol: String): List<Event> {
        val tradingHistoryResponse = client.marketClient.getPublicTradingHistoryBlocking(
            PublicTradingHistoryParams(
                category = endpoint.toCategory(),
                symbol,
                limit = 1000
            )
        )
        val asset = subscriptions[symbol]
        logger.info("loadRecentTradeHistory: loading ${tradingHistoryResponse.result.list.size} most recent trades")

        return tradingHistoryResponse.result.list.map {
            val sign = if (it.side == Side.Sell) -1 else 1
            val action = TradePrice(asset!!, it.price.toDouble(), it.size.toDouble().times(sign))
            Event(listOf(action), Instant.ofEpochMilli(it.time.toLong()))
        }.asReversed()
    }


    private fun symbolsToAssets(symbols: Array<out String>): Map<String, Asset> {
        val assets = symbols.map {
            val notFutures = it.endsWith("USDT") || it.endsWith("USD")
            var id = ""
            val currency = when (endpoint) {
                ByBitEndpoint.Spot -> {
                    id = "spot::"
                    Currency.USDT
                }

                ByBitEndpoint.Linear -> {
                    id = if (notFutures) {
                        "linearOrInverse::LinearPerpetual"
                    } else {
                        "linearOrInverse::LinearFutures"
                    }
                    Currency.USDT
                }

                ByBitEndpoint.Inverse -> {
                    id = if (notFutures) {
                        "linearOrInverse::InversePerpetual"
                    } else {
                        "linearOrInverse::InverseFutures"
                    }
                    Currency.getInstance(it.replace("USD", ""))
                }

                ByBitEndpoint.Option -> {
                    id = "option::"
                    Currency.USD
                }

                else -> {
                    Currency.USD
                }
            }
            Asset(
                it,
                AssetType.CRYPTO,
                currency = currency,
                exchange = Exchange.CRYPTO,
                id = id
            )
        }
            .associateBy { it.symbol }
        return assets
    }

    fun subscribeTrade(
        vararg symbols: String,
        loadRecentTradeHistory: Boolean = false
    ) {

        val assets = symbolsToAssets(symbols)

        subscriptions.putAll(assets)

        if (loadRecentTradeHistory) {
            symbols.forEach {
                val events = fetchRecentTradeHistoryEvents(it)
                recentTradeHistoryQueue.addAll(events)
            }
        }

        val tradeSubs = symbols.map {
            ByBitWebSocketSubscription(ByBitWebsocketTopic.Trades, it)
        }
        bybitSubscriptions.addAll(tradeSubs)

        runBlocking {
            wsClient.subscribe(tradeSubs)
        }
    }

    fun subscribeOrderBook(
        vararg symbols: String,
        level: ByBitWebsocketTopic.Orderbook = ByBitWebsocketTopic.Orderbook.Level_50
    ) {

        val assets = symbolsToAssets(symbols)

        subscriptions.putAll(assets)

        val orderBookSubs = symbols.map {
            ByBitWebSocketSubscription(level, it)
        }
        bybitSubscriptions.addAll(orderBookSubs)

        runBlocking {
            wsClient.subscribe(orderBookSubs)
        }
    }

    fun subscribeLiquidations(vararg symbols: String) {

        val assets = symbolsToAssets(symbols)

        subscriptions.putAll(assets)

        val liqSubs = symbols.map {
            ByBitWebSocketSubscription(ByBitWebsocketTopic.Liquidations, it)
        }
        bybitSubscriptions.addAll(liqSubs)

        runBlocking {
            wsClient.subscribe(liqSubs)
        }
    }

    fun subscribeTickers(vararg symbols: String) {

        val assets = symbolsToAssets(symbols)

        subscriptions.putAll(assets)

        val tickSubs = symbols.map {
            ByBitWebSocketSubscription(ByBitWebsocketTopic.Tickers, it)
        }
        bybitSubscriptions.addAll(tickSubs)

        runBlocking {
            wsClient.subscribe(tickSubs)
        }
    }

    override suspend fun play(channel: EventChannel) {
        recentTradeHistoryQueue.forEach {
            channel.send(it)
        }
        super.play(channel)
    }

    /**
     * Disconnect from ByBit server and stop receiving market data
     */

    override fun close() {
        wsClient.disconnect()
    }
}
