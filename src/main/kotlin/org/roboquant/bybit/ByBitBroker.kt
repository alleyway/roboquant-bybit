@file:Suppress("unused", "WildcardImport")

package org.roboquant.bybit

import bybit.sdk.CustomResponseException
import bybit.sdk.rest.ByBitRestClient
import bybit.sdk.rest.account.WalletBalanceParams
import bybit.sdk.rest.order.*
import bybit.sdk.rest.position.ClosedPnLParams
import bybit.sdk.rest.position.ClosedPnLResponseItem
import bybit.sdk.rest.position.PositionInfoParams
import bybit.sdk.rest.position.closedPnLs
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
import org.roboquant.brokers.sim.AccountModel
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
    private val accountModel: AccountModel

    /**
     * @see Broker.account
     */
    override var account: Account
        private set

    private val logger = Logging.getLogger(ByBitBroker::class)
    private val placedOrders = mutableMapOf<String, Int>()
    private val assetMap: Map<String, Asset>

    private var lastExpensiveSync = Instant.MIN

    private val rateLimitConfig = RateLimiterConfig.custom()
        .limitForPeriod(10) // Allow 10 calls within a time window
        .limitRefreshPeriod(Duration.ofSeconds(1)) // Time window of 1 second
        .timeoutDuration(Duration.ofMillis(1000)) // Timeout for acquiring a permit
        .build()

    private val rateLimiter = RateLimiter.of("broker", rateLimitConfig)

    private val accountType: AccountType

    init {
        config.configure()
        accountModel = MarginAccountInverse(config.leverage, 0.5.percent)
        client = ByBit.getRestClient(config)

        val wsOptions = WSClientConfigurableOptions(
            ByBitEndpoint.Private,
            config.apiKey,
            config.secret,
            config.testnet
        )
        wsClient = ByBit.getWebSocketClient(wsOptions)
        assetMap = ByBit.availableAssets(client, category)

        accountType = when (category) {
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

        try {
            updateAccountFromAPI()
        } catch (e: Exception) {
            logger.error { "exception running updateAccountFromAPI(): ${e.message}" }
        }

        accountModel.updateAccount(_account)

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

        account = _account.toAccount()
    }

    suspend fun fetchClosedPnLs(
        symbol: String? = null,
        startTime: Long? = null,
        endTime: Long? = null,
        limit: Int = 50
    ): List<ClosedPnLResponseItem>? {
        val resp = client.positionClient.closedPnLs(ClosedPnLParams(category, symbol, startTime, endTime, limit))

        return if (resp.retCode == 0) {
            resp.result.list
        } else {
            logger.error { "unable to fetch closed PnLs: " + resp.retMsg }
            null
        }
    }

    override fun sync(event: Event) {
        logger.debug { "Sync()" }
        try {
            _account.updateMarketPrices(event)
        } catch (e: NoSuchElementException) {
            e.printStackTrace()
            logger.warn { "Captured NoSuchElementException" }
        }

        _account.lastUpdate = event.time

        accountModel.updateAccount(_account)

        val now = Instant.now()
        val period = Duration.between(lastExpensiveSync, now)
        if (period.seconds > 60) {

//            GlobalScope.launch {
//                launch(Dispatchers.IO) {
            // possibly launch an coroutine to return immediately
            try {
                updateAccountFromAPI()
                lastExpensiveSync = now
            } catch (e: Exception) {
                logger.error { "Caught exception trying to update account from API: \n ${e.stackTraceToString()}" }
            }
//                }
//            }

        }

        account = _account.toAccount()

    }

    private fun updateAccountFromAPI() {

        syncOrdersFromAPI()
        syncAccountCashFromAPI()


        when (category) {
            Category.linear, Category.inverse, Category.option -> {
                val serverPositions = client.positionClient.getPositionInfoBlocking(
                    PositionInfoParams(
                        category,
                        settleCoin = baseCurrency.currencyCode
                    )
                ).result.list

                val p = _account.portfolio

                p.keys.forEach { asset ->
                    if (serverPositions.none { it.symbol == asset.symbol }) {
                        logger.warn { "Had to remove Roboquant position that didn't exist on server: $asset" }
                        p.remove(asset)
                    }
                }

                serverPositions.forEach { serverPosition ->

                    val asset = assetMap[serverPosition.symbol]

                    if (asset == null) {
                        logger.warn { "Found serverPosition with symbol not in assetMap" }
                    } else {

                        val sign = if (serverPosition.side == Side.Sell) -1 else 1
                        val serverSize = Size(serverPosition.size.toDouble().times(sign))

                        val currentPos = p.getOrDefault(asset, Position.empty(asset))

                        if (serverSize.iszero && p.containsKey(asset)) {
                            p.remove(asset)
                        } else {
                            if (currentPos.size != serverSize) {
                                logger.warn { "Different position from server when syncing" }
                                _account.setPosition(
                                    Position(
                                        asset,
                                        size = serverSize,
                                        avgPrice = serverPosition.avgPrice.toDouble(),
                                        mktPrice = serverPosition.markPrice.toDouble(),
                                        lastUpdate = Instant.ofEpochMilli(serverPosition.updatedTime.toLong())
                                    )
                                )
                            }
                        }
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

        // SHOULD_DO: Sync the open orders
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
                    val accountOrder = _account.getOrder(orderId) ?: continue

                    when (order.orderStatus) {
                        bybit.sdk.shared.OrderStatus.New -> {
                            // amend order comes here.
                            // it was found in our placedOrders map, so must have already existed
                            // state might be an instance of "UpdateOrder"

                            when (accountOrder) {
                                is UpdateOrder -> {
//                                    _account.updateOrder(state, Instant.now(), OrderStatus.ACCEPTED)
                                }

                                else -> {
                                    // logger.info("WS: bybit.OrderStatus.New -> roboquant.OrderStatus.ACCEPTED")
                                    _account.updateOrder(accountOrder, Instant.now(), OrderStatus.ACCEPTED)
                                }
                            }
                        }

                        bybit.sdk.shared.OrderStatus.Filled -> {
                            _account.updateOrder(accountOrder, Instant.now(), OrderStatus.COMPLETED)
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
                            _account.updateOrder(accountOrder, Instant.now(), OrderStatus.CANCELLED)
                        }

//                bybit.sdk.shared.OrderStatus.Exp ->
//                    _account.updateOrder(state, Instant.now(), OrderStatus.EXPIRED)

                        bybit.sdk.shared.OrderStatus.Rejected ->
                            _account.updateOrder(accountOrder, Instant.now(), OrderStatus.REJECTED)

                        else -> {
                            // NOTE: an amended order will show up here with OrderStatus "New"
                            logger.debug(
                                "WS update ( price: ${(accountOrder as LimitOrder).limit} ) with orderLinkId: " + TextColors.gray(
                                    order.orderLinkId
                                )
                            )
                            _account.updateOrder(accountOrder, Instant.now(), OrderStatus.ACCEPTED)
                        }
                    }
                }

            }

            is ByBitWebSocketMessage.PrivateTopicResponse.Wallet -> {

                for (coinItem in (message.data.filter { it.accountType == AccountType.CONTRACT }).first().coin) {
                    if (coinItem.coin == "BTC") {
                        // available to withdraw removes orders on the books
                        val walletBalance = coinItem.walletBalance.toDouble()
                        val unrealizedPnL = coinItem.unrealisedPnl.toDouble()
                        val totalPositionIM = coinItem.totalPositionIM.toDouble()
                        val totalOrderIM = coinItem.totalOrderIM.toDouble()
                        logger.debug("ByBitWebSocketMessage.PrivateTopicResponse.Wallet | walletBalance: $walletBalance")

                        val cash = walletBalance + unrealizedPnL - totalPositionIM - totalOrderIM

                        _account.cash.set(Currency.getInstance(coinItem.coin), cash)
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


    /**
     * There's a little issue where the response only returns max 50 in one call
     * Therefore if you have a lot of orders before consolidation they wouldn't
     * show up. Maybe in the future paginate orders and run in coroutine, but...slow.
     *
     /
     *
     */

    private fun syncOrdersFromAPI() {

        try {
            val ordersOpenResponse = client.orderClient.ordersOpenBlocking(OrdersOpenParams(category, limit = 50))


            if (ordersOpenResponse.result.list.size < 50) {
                val openOrderLinkIds = ordersOpenResponse.result.list.map { it.orderLinkId }

                placedOrders.entries.forEach {
                    val orderState = _account.getOrderState(it.value)

                    if (orderState != null
                        && !openOrderLinkIds.contains(it.key)
                        && orderState.status != OrderStatus.INITIAL
                    ) {
                        logger.warn(
                            "Rejecting order that existed in _account.openOrders, but was not found on server:\n "
                                    + "${orderState.order} "
                        )
                        val now = Instant.now()
                        _account.updateOrder(orderState.order, now, OrderStatus.COMPLETED)
                    }
                }
            }

        } catch (error: CustomResponseException) {
            logger.warn(error.message)
        }
    }

    private fun syncAccountCashFromAPI() {

        val walletBalanceResp = client.accountClient.getWalletBalanceBlocking(WalletBalanceParams(accountType))

        for (coinItem in walletBalanceResp.result.list.first().coin.filter { it.coin == baseCurrency.currencyCode }) {
//            val equityValue = coinItem.equity.toDouble()
//            // in theory we could arrive at our cash by the following:
//            // equity - orders value - position value
//
//            _account.cash.set(Currency.getInstance(coinItem.coin), equityValue)

            // WARNING: be sure to sync with what's happening in the WS wallet updates too!
            val walletBalance = coinItem.availableToWithdraw.toDouble()
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
                _account.completeOrder(it, Instant.now())
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
        _account.initializeOrders(orders)

        val cancelAllOrders = orders.filterIsInstance<CancelOrder>().filter { it.tag == "CancelAll" }
        if (cancelAllOrders.isNotEmpty()) cancelAllOrders(cancelAllOrders)

        for (order in orders) {
            when (order) {
                is CancelOrder -> if (order.tag != "CancelAll") cancelOrder(order)

                is LimitOrder -> {
                    val symbol = order.asset.symbol
                    val orderLinkId = genOrderLinkId()
                    placedOrders[orderLinkId] = order.id
                    trade(orderLinkId, symbol, order)
                }

                is MarketOrder -> {
                    val symbol = order.asset.symbol
                    val orderLinkId = genOrderLinkId()
                    placedOrders[orderLinkId] = order.id
                    trade(orderLinkId, symbol, order)

                }

                is UpdateOrder -> {
                    val symbol = order.asset.symbol
                    try {
                        val orderLinkId = placedOrders.entries.find { it.value == order.order.id }?.key
                        if (orderLinkId != null) {
//                            placedOrders[orderLinkId] = order.id
                            logger.debug("amend() will update status for orderLinkId: ${orderLinkId}")
                            amend(orderLinkId, symbol, order)
                        } else {
                            logger.warn("")
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

        if (orderLinkId == null) {
            logger.error("Unable to find order.id in placedOrders: ${cancellation.order.id}")
            return
        }

        rateLimiter.executeRunnable {
            try {
                val order = cancellation.order

                val response = client.orderClient.cancelOrderBlocking(
                    CancelOrderParams(
                        category,
                        symbol = order.asset.symbol,
                        orderLinkId = orderLinkId
                    )
                )

                if (response.retCode != 0) {
                    logger.warn { "NonZero retCode cancelling Order: " + response.retMsg }
                    _account.rejectOrder(order, Instant.now())
                } else {
                    _account.completeOrder(order, Instant.now())
                }

            } catch (e: CustomResponseException) {
                if (e.retCode == 110001) { // order does not exist
                    logger.warn("Tried to cancel order that did not exist. OrderLinkId: $orderLinkId")
                    if (_account.getOrder(cancellation.order.id) != null) {
                        _account.rejectOrder(cancellation.order, Instant.now())
                    }
                    if (_account.getOrder(cancellation.id) != null) {
                        _account.rejectOrder(cancellation, Instant.now())
                    }
                } else {
                    logger.error(e.message)
                }
            } catch (e: Exception) {
                logger.error(e.message)
            }
        }

    }


    private fun cancelAllOrders(cancelOrders: List<CancelOrder>) {
        logger.debug { "Running cancelAllOrders()" }
        rateLimiter.executeRunnable {
            try {
                val response = client.orderClient.cancelAllOrdersBlocking(
                    CancelAllOrdersParams(category, symbol = cancelOrders[0].asset.symbol)
                )
                if (response.retCode != 0) {
                    logger.warn { "NonZero retCode cancelling All Orders: " + response.retMsg }
                } else {
                    when (response) {
                        is CancelAllOrdersResponse.CancelAllOrdersResponseOther -> {
                            response.result.list.forEach { item ->
                                val id = placedOrders[item.orderLinkId]
                                id?.let {
                                    _account.getOrder(id)?.let {
                                        _account.updateOrder(it, Instant.now(), OrderStatus.CANCELLED)
                                    }
                                    cancelOrders.firstOrNull { it.order.id == id }?.let { cancelOrder ->
                                        _account.updateOrder(cancelOrder, Instant.now(), OrderStatus.COMPLETED)
                                    }
                                }
                            }
                        }

                        is CancelAllOrdersResponse.CancelAllOrdersResponseSpot -> {
                            logger.warn { "Unfortunately bybit doesn't return orderLinkIds for SPOT!" }
                        }

                        else -> {
                            logger.error { "Unknown response type when cancelling all orders: ${response}" }
                        }
                    }

                }
            } catch (e: CustomResponseException) {
                logger.error(e.message)
            } catch (e: Exception) {
                logger.error(e.message)
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
                order.size.absoluteValue
            }

            else -> {
                order.size.absoluteValue
//                Size(
//                    (order.size.absoluteValue * order.limit).toBigDecimal().setScale(0, RoundingMode.HALF_UP)
//                ).toBigDecimal()
            }
        }

        val price = order.limit

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
                        amount.toBigDecimal().toPlainString(),
                        reduceOnly = reduceOnly,
                        price.toBigDecimal().toPlainString(),
                        timeInForce = TimeInForce.PostOnly,
                        orderLinkId = orderLinkId,
                    )
                )

                val orderId = placedOrders[orderLinkId]
                val accountOrder = _account.getOrder(orderId!!)

                if (response.retCode != 0) {
                    logger.warn { "NonZero retCode creating Order: " + response.retMsg }
                    accountOrder?.let { _account.rejectOrder(it, Instant.now()) }
                } else {
                    accountOrder?.let { _account.acceptOrder(it, Instant.now()) }
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

                val orderId = placedOrders[orderLinkId]
                val accountOrder = _account.getOrder(orderId!!)

                if (response.retCode != 0) {
                    logger.warn { "NonZero retCode creating Order: " + response.retMsg }
                    accountOrder?.let { _account.rejectOrder(it, Instant.now()) }
                } else {
                    accountOrder?.let { _account.acceptOrder(it, Instant.now()) }
                }

            } catch (e: Exception) {
                logger.error(e.message)
            }
        }
    }


    private fun amend(orderLinkId: String, symbol: String, updateOrder: UpdateOrder) {
        if (category == Category.spot) throw Exception("Unable to amend orders for spot according to bybit docs")

        when (updateOrder.update) {
            is LimitOrder -> {
                val originalLimitOrder = updateOrder.order as LimitOrder
                val updatedLimitOrder = updateOrder.update as LimitOrder

                // only update qty or price if different from original otherwise API complains
                val qty = when (updatedLimitOrder.size == originalLimitOrder.size) {
                    true -> null
                    false -> {
                        logger.info(
                            "Amending order qty: from ${originalLimitOrder.size} to ${updatedLimitOrder.size} " + TextColors.gray(
                                orderLinkId
                            )
                        )
                        updatedLimitOrder.size.absoluteValue.toString()
                    }
                }
                val price = when (updatedLimitOrder.limit.equals(originalLimitOrder.limit)) {
                    true -> null
                    false -> {
                        logger.info(
                            "Amending order price from ${originalLimitOrder.limit} to ${updatedLimitOrder.limit} " + TextColors.gray(
                                orderLinkId
                            )
                        )
                        updatedLimitOrder.limit.toString()
                    }
                }

                rateLimiter.executeRunnable {

                    try {
                        client.orderClient.amendOrderBlocking(
                            AmendOrderParams(
                                category,
                                symbol,
                                orderLinkId = orderLinkId,
                                qty = qty,
                                price = price
                            )
                        )

                        _account.completeOrder(updateOrder, Instant.now())
                        _account.updateOrder(updateOrder.update, Instant.now(), OrderStatus.ACCEPTED)

                        placedOrders[orderLinkId] = updateOrder.update.id // pretty sure i need this

                    } catch (e: CustomResponseException) {
                        logger.error { "Unable to amend order, server error: ${e.retMsg}" }
                        _account.rejectOrder(updateOrder, Instant.now())
                    } catch (e: Exception) {
                        logger.error { e.message.toString() }
                    }
                }
            }

            else -> {
                throw Exception("Tried to amend non-limit order type: ${updateOrder.update::class}")
            }
        }

    }

}

