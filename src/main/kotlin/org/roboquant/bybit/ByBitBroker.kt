@file:Suppress("unused", "WildcardImport")

package org.roboquant.bybit

import bybit.sdk.CustomResponseException
import bybit.sdk.rest.ByBitRestClient
import bybit.sdk.rest.account.WalletBalanceParams
import bybit.sdk.rest.order.AmendOrderParams
import bybit.sdk.rest.order.CancelOrderParams
import bybit.sdk.rest.order.PlaceOrderParams
import bybit.sdk.rest.position.PositionInfoParams
import bybit.sdk.shared.*
import bybit.sdk.shared.TimeInForce
import bybit.sdk.websocket.*
import com.github.ajalt.mordant.rendering.TextColors
import io.github.resilience4j.ratelimiter.RateLimiter
import io.github.resilience4j.ratelimiter.RateLimiterConfig
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.launch
import org.roboquant.brokers.*
import org.roboquant.brokers.sim.execution.InternalAccount
import org.roboquant.common.*
import org.roboquant.common.Currency
import org.roboquant.feeds.Event
import org.roboquant.orders.*
import org.roboquant.orders.OrderStatus
import java.math.RoundingMode
import java.time.Duration
import java.time.Instant
import java.util.*


/**
 * Implementation of the broker interface for the ByBit exchange. This enables paper- and live-trading of
 * cryptocurrencies on the ByBit exchange. This broker only supports assets of the type [AssetType.CRYPTO].
 *
 * @param baseCurrency The base currency to use
 * @param configure additional configure logic, default is to do nothing
 *
 * @constructor
 */
class ByBitBroker(
    val category: Category = Category.spot,
    val baseCurrency: Currency = Currency.USDT,
    configure: ByBitConfig.() -> Unit = {}
) : Broker {

    private val client: ByBitRestClient
    private val wsClient: ByBitWebSocketClient
    private val _account = InternalAccount(baseCurrency)
    private val config = ByBitConfig()

    /**
     * @see Broker.account
     */
    override val account: Account
        get() = _account.toAccount()

    private val logger = Logging.getLogger(ByBitBroker::class)
    private val placedOrders = mutableMapOf<String, Int>()
    private val assetMap: Map<String, Asset>

    private val rateLimitConfig = RateLimiterConfig.custom()
        .limitForPeriod(10) // Allow 10 calls within a time window
        .limitRefreshPeriod(Duration.ofSeconds(1)) // Time window of 1 second
        .timeoutDuration(Duration.ofMillis(1000)) // Timeout for acquiring a permit
        .build()

    private val rateLimiter = RateLimiter.of("broker", rateLimitConfig)

    init {
        config.configure()
        client = ByBit.getRestClient(config)

        val wsOptions = WSClientConfigurableOptions(
            ByBitEndpoint.Private,
            config.apiKey,
            config.secret,
            config.testnet
        )
        wsClient = ByBit.getWebSocketClient(wsOptions)
        assetMap = ByBit.availableAssets(client, category)

        updateAccount()


        val subscriptions = listOf(
            ByBitWebSocketSubscription(ByBitWebsocketTopic.PrivateTopic.Execution),
            ByBitWebSocketSubscription(ByBitWebsocketTopic.PrivateTopic.Order),
            ByBitWebSocketSubscription(ByBitWebsocketTopic.PrivateTopic.Wallet)
        )

        val scope = CoroutineScope(Dispatchers.Default + Job())
        scope.launch {
            wsClient.connect(subscriptions)

            val channel = wsClient.getWebSocketEventChannel()

            while (true) {
                val msg = channel.receive()
                (this@ByBitBroker::handler)(msg)
            }
        }
    }

    override fun sync(event: Event) {
        logger.debug { "Sync()" }
        _account.updateMarketPrices(event)
        _account.lastUpdate = event.time
        // maybe once-in-awhile we're supposed to do some "expensive"
        // syncing?
    }

    private fun updateAccount() {
        _account.portfolio.clear()
        syncAccountCash()

        when (category) {
            Category.linear, Category.inverse, Category.option -> {
                for (position in client.positionClient.getPositionInfoBlocking(
                    PositionInfoParams(
                        category,
                        settleCoin = baseCurrency.currencyCode
                    )
                ).result.list) {
                    assetMap[position.symbol]?.let {
                        val sign = if (position.side == Side.Sell) -1 else 1
                        _account.setPosition(
                            Position(
                                it,
                                size = Size(position.size.toDouble().times(sign)),
//                                new_average_price = (existing_average_price * existing_shares + added_price * added_shares) / (existing_shares + added_shares)
                                avgPrice = position.avgPrice.toDouble(),
                                mktPrice = position.markPrice.toDouble(),
                                lastUpdate = Instant.ofEpochMilli(position.updatedTime.toLong())
                            )
                        )
                    }
                }
            }

            Category.spot -> {

                assetMap.get("BTCUSDT")?.let {
                    // or should I use equity??
                    _account.setPosition(
                        Position(
                            it,
                            Size(
                                _account.cash[Currency.BTC]
                            )
                        )
                    )
                }
            }

            else -> {
                // do nothing
            }
        }


//        client.orderClient.orderOpen()

        // TODO: Sync the open orders
//        for (order in _account.orders) {
//            val brokerOrder = api.getOrder(order.orderId)
//
//            // Fictitious implementation
//            when (brokerOrder.status) {
//                "RECEIVED" -> _account.updateOrder(order.order, Instant.now(), OrderStatus.ACCEPTED)
//                "DONE" -> _account.updateOrder(order.order, Instant.now(), OrderStatus.COMPLETED)
//            }
//        }


        // Sync buying-power
//        val buyingPower = api.getBuyingPower()
//        _account.buyingPower = buyingPower.USD

        // Set the lastUpdate time
        _account.lastUpdate = Instant.now()
    }

    private fun genOrderLinkId(): String {
        // should be no more than 36 characters
        return UUID.randomUUID().toString()
    }

    private fun handler(message: ByBitWebSocketMessage) {

        when (message) {

            is ByBitWebSocketMessage.StatusMessage -> {
//                message.op == "auth"
            }

            is ByBitWebSocketMessage.RawMessage -> {
                logger.debug { message.data }
            }

            is ByBitWebSocketMessage.PrivateTopicResponse.Order -> {

                for (order in message.data) {
                    val orderId = placedOrders[order.orderLinkId] ?: continue
                    val state = _account.getOrder(orderId) ?: continue

                    when (order.orderStatus) {
                        bybit.sdk.shared.OrderStatus.New -> {
                            // amend order comes here.
                            // it was found in our placedOrders map, so must have already existed
                            // state might be an instance of "UpdateOrder"

                            when (state) {
                                is UpdateOrder -> {
//                                    _account.updateOrder(state, Instant.now(), OrderStatus.ACCEPTED)
                                }

                                else -> {
                                    // logger.info("WS: bybit.OrderStatus.New -> roboquant.OrderStatus.ACCEPTED")
                                    _account.updateOrder(state, Instant.now(), OrderStatus.ACCEPTED)
                                }
                            }
                        }

                        bybit.sdk.shared.OrderStatus.Filled -> {
                            _account.updateOrder(state, Instant.now(), OrderStatus.COMPLETED)
                        }

                        bybit.sdk.shared.OrderStatus.PartiallyFilled -> {
//                            if (order.orderType == OrderType.Market) {
//                                logger.trace("order.cumExecValue: " + order.cumExecValue)
//                                _account.updateOrder(state, Instant.now(), OrderStatus.COMPLETED)
//                            }

                        }

                        bybit.sdk.shared.OrderStatus.Cancelled,
                        bybit.sdk.shared.OrderStatus.PartiallyFilledCanceled
                        -> {
                            logger.trace { "cancelled order: ${order.orderId}" }
                            _account.updateOrder(state, Instant.now(), OrderStatus.CANCELLED)
                        }

//                bybit.sdk.shared.OrderStatus.Exp ->
//                    _account.updateOrder(state, Instant.now(), OrderStatus.EXPIRED)

                        bybit.sdk.shared.OrderStatus.Rejected ->
                            _account.updateOrder(state, Instant.now(), OrderStatus.REJECTED)

                        else -> {
                            // NOTE: an amended order will show up here with OrderStatus "New"
                            logger.debug(
                                "WS update ( price: ${(state as LimitOrder).limit} ) with orderLinkId: " + TextColors.gray(
                                    order.orderLinkId
                                )
                            )
                            _account.updateOrder(state, Instant.now(), OrderStatus.ACCEPTED)
                        }
                    }
                }

            }

            is ByBitWebSocketMessage.PrivateTopicResponse.Wallet -> {

                for (coinItem in (message.data.filter { it.accountType == AccountType.CONTRACT }).first().coin) {
                    if (coinItem.coin == "BTC") {
                        val walletBalance = coinItem.walletBalance.toDouble()
                        logger.debug("ByBitWebSocketMessage.PrivateTopicResponse.Wallet | walletBalance: $walletBalance")
                        _account.cash.set(Currency.getInstance(coinItem.coin), walletBalance)
                    }
                }


                // NOTE: for debugging how well ByBitBroker is keeping track of our position

                if (false) {
                    for (coinItem in (message.data.filter { it.accountType == AccountType.SPOT }).first().coin) {
                        if (coinItem.coin == "BTC") {
                            assetMap.get("BTCUSDT")?.let {

                                val serverWallet = coinItem.walletBalance.toBigDecimal().setScale(8, RoundingMode.DOWN)
                                val positionSize =
                                    account.positions.getPosition(it).size.toBigDecimal().setScale(8, RoundingMode.DOWN)

                                logger.debug(
                                    "      Server Wallet:  ${serverWallet.toPlainString()}\n" +
                                            "      ByBitBrkr Pos:  ${positionSize.toPlainString()}\n" +
                                            "         Difference: ${TextColors.yellow((serverWallet - positionSize).toPlainString())}"
                                )
                            }
                        }
                    }
                }
            }

            is ByBitWebSocketMessage.PrivateTopicResponse.Execution -> {

                message.data.forEach {

                    when (it.execType) {
                        ExecType.Trade -> updateAccountWithExecution(it)

                        else -> {
                            logger.warn("execution type ${it.execType} not yet handled")
                            return
                        }
                    }


                }
            }

            else -> logger.warn("received message=$message")
        }

    }

    /**
     * Return all available assets to trade
     */
    val availableAssets
        get() = assetMap.values.toSortedSet()


    private fun syncAccountCash() {

        val accountType = when (category) {
            Category.inverse, Category.linear -> {
                AccountType.CONTRACT
            }

            Category.spot -> {
                AccountType.SPOT
            }

            Category.option -> {
                AccountType.OPTION
            }
        }

        val walletBalanceResp = client.accountClient.getWalletBalanceBlocking(WalletBalanceParams(accountType))

        for (coinItem in walletBalanceResp.result.list.first().coin.filter { it.coin == baseCurrency.currencyCode }) {
//            val equityValue = coinItem.equity.toDouble()
//            // in theory we could arrive at our cash by the following:
//            // equity - orders value - position value
//
//            _account.cash.set(Currency.getInstance(coinItem.coin), equityValue)

            // WARNING: be sure to sync with what's happening in the WS wallet updates too!
            val walletBalance = coinItem.walletBalance.toDouble()
            logger.info("syncAccountCash() walletBalance: $walletBalance")
            _account.cash.set(Currency.getInstance(coinItem.coin), walletBalance)

            // think i'm suppose to multiple my leverage by cash
//            _account.buyingPower = Amount(baseCurrency, coinItem.availableToBorrow.toDouble())
        }
    }

    private fun updatePosition(position: Position): Amount {
        val asset = position.asset
        val p = _account.portfolio
        val currentPos = p.getOrDefault(asset, Position.empty(asset))
        val newPosition = currentPos + position
        if (newPosition.closed)
            p.remove(asset)
        else
            p[asset] = newPosition
        return currentPos.realizedPNL(position)
    }

    private fun updateAccountWithExecution(execution: ByBitWebSocketMessage.ExecutionItem) {

        val asset = assetMap[execution.symbol]

        if (asset == null) {
            logger.warn("Received execution for unknown symbol: ${execution.symbol}")
            return
        }

        val rqOrderId = placedOrders[execution.orderLinkId]

        if (rqOrderId == null) {
            logger.warn("Received execution order not placed in system. orderLinkId : ${execution.orderLinkId}, orderId : ${execution.orderId}")
            //return
        }

        val sign = if (execution.side == Side.Buy) 1 else -1

        val execPrice = execution.execPrice.toDouble()

        val execSize = Size(execution.execQty) * sign

        logger.info(
            "ByBitBroker Executed ${execution.orderType} ${execution.side} "
                    + TextColors.cyan(execSize.toString())
                    + " @ ${TextColors.brightBlue(execPrice.toString())} "
                    + TextColors.gray(execution.orderLinkId)
        )

        if (rqOrderId !== null && execution.leavesQty == "0") {
            _account.getOrder(rqOrderId)?.let {
//                logger.debug("WS Execution leaves 0 qty, update order status to completed")
                _account.updateOrder(it, Instant.now(), OrderStatus.COMPLETED)
            }
        }

        val position = Position(asset, execSize, execPrice)

        // Calculate the fees that apply to this execution
        val fee = execution.execFee.toDouble()

        val pnl = updatePosition(position) - fee

        if (rqOrderId !== null) {
            val newTrade = Trade(
                Instant.ofEpochMilli(execution.execTime.toLong()),
                asset,
                execSize,
                execPrice,
                fee,
                pnl.value,
                rqOrderId
            )
            _account.addTrade(newTrade)
        }

        // in theory we shouldn't need to do this as will be updated by WebSockets
        //  _account.cash.withdraw(newTrade.totalCost)

    }



    /**
     * @param orders
     * @return
     */
    override fun place(orders: List<Order>, time: Instant) {
        logger.trace { "Received orders=${orders.size} time=$time" }

//        if (time < Instant.now() - 1.hours) throw UnsupportedException("cannot place orders in the past")

        _account.initializeOrders(orders)

        for (order in orders) {
            when (order) {
                is CancelOrder -> cancelOrder(order)

                is LimitOrder -> {
                    val symbol = order.asset.symbol
                    val orderLinkId = genOrderLinkId()
                    trade(orderLinkId, symbol, order)
                    placedOrders[orderLinkId] = order.id
                }

                is MarketOrder -> {
                    val symbol = order.asset.symbol
                    val orderLinkId = genOrderLinkId()
                    trade(orderLinkId, symbol, order)
                    placedOrders[orderLinkId] = order.id

                }

                is UpdateOrder -> {
                    val symbol = order.asset.symbol
                    try {
                        val orderLinkId = placedOrders.entries.find { it.value == order.order.id }?.key
                        if (orderLinkId != null) {
                            placedOrders[orderLinkId] = order.id
                            logger.debug("amend() will update status for orderLinkId: ${orderLinkId}")
                            amend(symbol, order, orderLinkId)
                        }
                    } catch (error: CustomResponseException) {
                        logger.warn(error.message)
                    }
                }

                else -> logger.warn {
                    "supports only cancellation, market and limit orders, received ${order::class} instead"
                }
            }

        }
    }


    /**
     * Cancel an order
     *
     * @param cancellation
     */
    private fun cancelOrder(cancellation: CancelOrder) {

        val orderLinkId = placedOrders.entries.find { it.value == cancellation.order.id }?.key


        if (orderLinkId !== null) {
            //placedOrders.entries.remove(entry)
            val now = Instant.now()
            _account.completeOrder(cancellation, now)
            try {
                // TODO: maybe we want this to be a new OrderStatus like "CANCELLING"
                _account.updateOrder(cancellation.order, Instant.now(), OrderStatus.CANCELLED)

            } catch (_: NoSuchElementException) {
                logger.warn("NoSuchElementException trying to cancel order: ${cancellation.order}")
                return
            }

            rateLimiter.executeRunnable {

                try {
                    val order = cancellation.order

                    client.orderClient.cancelOrderBlocking(
                        CancelOrderParams(
                            category,
                            symbol = order.asset.symbol,
                            orderLinkId = orderLinkId
                        )
                    )
                } catch (e: Exception) {
                    logger.error(e.message)
                }
            }
        }
    }

    /**
     * Place a limit order for a currency pair
     *
     * @param symbol
     * @param order
     */
    private fun trade(orderLinkId: String, symbol: String, order: LimitOrder) {

        val amount = when (category) {
            Category.spot -> {
                order.size.absoluteValue.toBigDecimal().setScale(6, RoundingMode.DOWN)
            }

            else -> {
                order.size.absoluteValue.toBigDecimal().setScale(0, RoundingMode.DOWN)
//                Size(
//                    (order.size.absoluteValue * order.limit).toBigDecimal().setScale(0, RoundingMode.HALF_UP)
//                ).toBigDecimal()
            }
        }

        val price = order.limit.toBigDecimal().setScale(2, RoundingMode.DOWN)

        rateLimiter.executeRunnable {

            val reduceOnly = order.tag.startsWith("tpOrder")

            try {
                val response = client.orderClient.placeOrderBlocking(
                    PlaceOrderParams(
                        category,
                        symbol,
                        side = if (order.buy) {
                            Side.Buy
                        } else {
                            Side.Sell
                        },
                        OrderType.Limit,
                        amount.toPlainString(),
                        reduceOnly = reduceOnly,
                        price.toPlainString(),
                        timeInForce = TimeInForce.PostOnly,
                        orderLinkId = orderLinkId,
                    )
                )
                if (response.retCode != 0) {
                    logger.warn { response.retMsg }
                    val orderId = placedOrders[orderLinkId]
                    orderId?.let {
                        val state = _account.getOrder(orderId)
                        state?.let {
                            _account.updateOrder(state, Instant.now(), OrderStatus.REJECTED)
                        }
                    }
                }
            } catch (e: Exception) {
                logger.error(e.message)
            }
        }
    }

    /**
     * Place a market order for a currency pair
     *
     * @param symbol
     * @param order
     */
    private fun trade(orderLinkId: String, symbol: String, order: MarketOrder) {

        val amount = order.size.absoluteValue

//        if (category == Category.spot) amount *= price!!

        // ByBit peculiarity!! for SPOT ONLY!
        // Order quantity. For Spot Market Buy order, please note that qty should be quote currency amount

        rateLimiter.executeRunnable {
            try {
                val response = client.orderClient.placeOrderBlocking(
                    PlaceOrderParams(
                        category,
                        symbol,
                        side = if (order.buy) {
                            Side.Buy
                        } else {
                            Side.Sell
                        },
                        OrderType.Market,
                        amount.toString(),
                        orderLinkId = orderLinkId
                    )
                )
                if (response.retCode != 0) {
                    logger.warn { response.retMsg }
                    val orderId = placedOrders[orderLinkId]
                    orderId?.let {
                        val state = _account.getOrder(orderId)
                        state?.let {
                            _account.updateOrder(state, Instant.now(), OrderStatus.REJECTED)
                        }
                    }
                }
            } catch (e: Exception) {
                logger.error(e.message)
            }
        }
    }


    private fun amend(symbol: String, order: UpdateOrder, orderLinkId: String) {
        if (category == Category.spot) throw Exception("Unable to amend orders for spot according to bybit docs")


        if (_account.getOrder(order.order.id) !== null)
            _account.updateOrder(order, Instant.now(), OrderStatus.ACCEPTED)
        else
            _account.updateOrder(order, Instant.now(), OrderStatus.REJECTED)

        when (order.update) {
            is LimitOrder -> {
                val originalLimitOrder = order.order as LimitOrder
                val updatedLimitOrder = order.update as LimitOrder

                // only update qty or price if different from original otherwise API complains
                val qty = when (updatedLimitOrder.size == originalLimitOrder.size) {
                    true -> null
                    false -> updatedLimitOrder.size.absoluteValue.toString()
                }
                val price = when (updatedLimitOrder.limit.equals(originalLimitOrder.limit)) {
                    true -> null
                    false -> updatedLimitOrder.limit.toString()
                }

                logger.info(
                    "Amending order from ${originalLimitOrder.limit} to ${updatedLimitOrder.limit} " + TextColors.gray(
                        orderLinkId
                    )
                )

                rateLimiter.executeRunnable {
                    val updatedOrderResponse = client.orderClient.amendOrderBlocking(
                        AmendOrderParams(
                            category,
                            symbol,
                            orderLinkId = orderLinkId,
                            qty = qty,
                            price = price
                        )
                    )
                    logger.info { "$updatedOrderResponse" }
                }
            }

            else -> {
                throw Exception("Tried to amend non-limit order type: ${order.update::class}")
            }
        }

    }

}

