@file:Suppress("unused", "WildcardImport")

package org.roboquant.bybit

import bybit.sdk.CustomResponseException
import bybit.sdk.RateLimitReachedException
import bybit.sdk.rest.ByBitRestClient
import bybit.sdk.rest.account.WalletBalanceParams
import bybit.sdk.rest.asset.CreateUniversalTransferParams
import bybit.sdk.rest.order.*
import bybit.sdk.rest.position.ClosedPnLParams
import bybit.sdk.rest.position.ClosedPnLResponseItem
import bybit.sdk.rest.position.PositionInfoParams
import bybit.sdk.shared.*
import bybit.sdk.shared.TimeInForce
import bybit.sdk.websocket.*
import com.github.ajalt.mordant.rendering.TextColors.*
import com.github.ajalt.mordant.rendering.TextStyles.dim
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.launch
import org.roboquant.brokers.Account
import org.roboquant.brokers.Broker
import org.roboquant.brokers.Position
import org.roboquant.brokers.Trade
import org.roboquant.brokers.getPosition
import org.roboquant.brokers.sim.AccountModel
import org.roboquant.brokers.sim.CashAccount
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
import kotlin.math.abs
import kotlin.system.measureTimeMillis


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
    private val accountModel: AccountModel = CashAccount(),
    configure: ByBitConfig.() -> Unit = {}
) : Broker {

    private val client: ByBitRestClient
    private val wsClient: ByBitWebSocketClient
    private val _account = InternalAccount(baseCurrency, 1.days)
    private val config = ByBitConfig()

    private val prevInitialOrderIds = mutableSetOf<Int>()

    /**
     * @see Broker.account
     */
    override var account: Account
        private set

    private val logger = Logging.getLogger(ByBitBroker::class)
    private val mutablePlacedOrders = mutableMapOf<String, Int>()
    private val placedOrders = Collections.synchronizedMap(mutablePlacedOrders)

    private val assetMap: Map<String, Asset>

    private var lastExpensiveSync = Instant.MIN

    private val accountType: AccountType

    init {
        config.configure()
        client = ByBit.getRestClient(config)

        val wsOptions = WSClientConfigurableOptions(
            ByBitEndpoint.Private,
            config.apiKey,
            config.secret,
            config.testnet,
            name = "ByBitBroker"
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
            ByBitWebSocketSubscription(ByBitWebsocketTopic.PrivateTopic.Wallet),
            ByBitWebSocketSubscription(ByBitWebsocketTopic.PrivateTopic.Position)
        )

        val scope = CoroutineScope(Dispatchers.Default + Job())
        scope.launch {
            val channel = wsClient.getWebSocketEventChannel()
            wsClient.connect(subscriptions)

            while (true) {
                val msg = channel.receive()
                (this@ByBitBroker::handler)(msg)
            }
        }

        account = _account.toAccount()
    }

    fun fetchClosedPnLs(
        symbol: String? = null,
        startTime: Long? = null,
        endTime: Long? = null,
        limit: Int = 50
    ): List<ClosedPnLResponseItem> {
        val resp = client.positionClient
            .closedPnLsPaginated(ClosedPnLParams(category, symbol, startTime, endTime, limit))
            .asStream().toList()
        return resp
    }

    fun transferCoin(
        coin: String,
        amount: String,
        fromMemberId: String,
        toMemberId: String,
        fromAccountType: AccountType,
        toAccountType: AccountType
    ): String {
        val resp = client.assetClient.createUniversalTransferBlocking(
            CreateUniversalTransferParams(
                UUID.randomUUID().toString(),
                coin,
                amount,
                fromMemberId,
                toMemberId,
                fromAccountType,
                toAccountType
            )
        )
        return resp.result.transferId
    }

    override fun sync(event: Event) {
        logger.trace { "Sync()" }

        _account.updateMarketPrices(event)

        _account.lastUpdate = event.time

        accountModel.updateAccount(_account)

        val now = Instant.now()
        val period = Duration.between(lastExpensiveSync, now)
        if (period.seconds > 10) {
            val initialOrderIds =
                _account.orders.filter { it.status == OrderStatus.INITIAL }.map { it.orderId }.toSet()
            if (initialOrderIds.isNotEmpty() && prevInitialOrderIds.isNotEmpty()) {
                prevInitialOrderIds.intersect(initialOrderIds).forEach {
                    val stuckOrder = _account.getOrder(it)
                    logger.warn { "rejecting account order of stuck at status INITIAL: [$stuckOrder]" }
                    _account.rejectOrder(stuckOrder!!, Instant.now())
                    prevInitialOrderIds.remove(it)
                }
            }
            prevInitialOrderIds.addAll(initialOrderIds)

            lastExpensiveSync = now

            CoroutineScope(Dispatchers.IO).launch {
                try {
                    updateAccountFromAPI()
                } catch (e: Exception) {
                    logger.error { "Caught exception trying to update account from API: \n ${e.stackTraceToString()}" }
                }
            }

        }
        account = _account.toAccount()

    }

    private fun updateAccountFromAPI() {
        logger.info("updateAccountFromAPI()")
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

                        // excluding other assets besides the one we're trading for now
                        if (asset.currency !== baseCurrency) return

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
                                        lastUpdate = Instant.ofEpochMilli(serverPosition.updatedTime.toLong()),
//                                        leverage = serverPosition.leverage.toDouble(),
//                                        margin = serverPosition.positionBalance.toDouble()
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
                                    // because the websocket can have a delay, it's better to just
                                    // be updating the order in the create/amend of the REST API call
                                    // A bug was occurring where this delayed update was overwriting
                                    // the most recent update from REST.

                                    // logger.info("WS: bybit.OrderStatus.New -> roboquant.OrderStatus.ACCEPTED")
                                    // _account.updateOrder(accountOrder, Instant.now(), OrderStatus.ACCEPTED)
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
                            logger.debug {
                                brightBlue(
                                    "WS: cancelled accountOrder (${accountOrder.id}): ${yellow(order.rejectReason)} " + gray(
                                        order.orderLinkId
                                    )
                                )
                            }
                            when (order.rejectReason) {
                                "EC_PostOnlyWillTakeLiquidity" -> {
                                    _account.rejectOrder(accountOrder, Instant.now())
                                }

                                else -> {
                                    _account.updateOrder(accountOrder, Instant.now(), OrderStatus.CANCELLED)
                                }
                            }

                        }

//                bybit.sdk.shared.OrderStatus.Exp ->
//                    _account.updateOrder(state, Instant.now(), OrderStatus.EXPIRED)

                        bybit.sdk.shared.OrderStatus.Rejected ->
                            _account.updateOrder(accountOrder, Instant.now(), OrderStatus.REJECTED)

                        else -> {
                            // NOTE: an amended order will show up here with OrderStatus "New"
                            logger.debug(
                                "WS update ( price: ${(accountOrder as LimitOrder).limit} ) with orderLinkId: " + gray(
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
                    if (coinItem.coin == baseCurrency.currencyCode) {

                        // https://www.bybit.com/fil-PH/help-center/article/Derivatives-or-Inverse-Derivatives-Acccount

                        // WARNING: be sure to sync with what's happening in the WS wallet updates too!

                        val p = cyan("WS:")

                        val walletBalance = Amount(baseCurrency, coinItem.walletBalance.toDouble())
                        logger.debug("$p walletBalance: ${blue(walletBalance.toString())}")

                        val totalPositionIM = Amount(baseCurrency, coinItem.totalPositionIM.toDouble())
                        logger.debug("$p totalPositionIM: ${brightWhite(totalPositionIM.toString())}")

                        val totalOrderIM = Amount(baseCurrency, coinItem.totalOrderIM.toDouble())
                        logger.debug("$p totalOrderIM: ${yellow(totalOrderIM.toString())}")

                        val availableToWithdraw = Amount(baseCurrency, coinItem.availableToWithdraw.toDouble())
                        logger.debug("$p availableToWithdraw: ${green(availableToWithdraw.toString())}")


//                        val availableToBorrow = Amount(baseCurrency, coinItem.availableToBorrow.toDouble())
//                        logger.debug("$p availableToBorrow: ${green(availableToBorrow.toString())} (~ account.buyingPower) (NOT SET)")

                        // -- Equity = Wallet Balance + Unrealized P&L (in Mark Price)
                        //val equity = coinItem.equity.toDouble()

                        val unrealizedPnL = coinItem.unrealisedPnl.toDouble()

                        val loss = if (unrealizedPnL.isFinite() && unrealizedPnL < 0) {
                            unrealizedPnL
                        } else 0.0


                        val cash = walletBalance.value
                        val existingCashAmount = _account.cash.convert(baseCurrency)
                        if (existingCashAmount != walletBalance) {
                            logger.debug("$p _account.cash.set(${dim(yellow(existingCashAmount.toString()))} → ${brightYellow(walletBalance.value.toString())})")
                            _account.cash.set(baseCurrency, cash)
                        }

                        // WARNING: be sure to sync with what's happening in the syncAccountCashFromAPI() updates too!

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
                                            "         Difference: ${yellow((serverWallet - positionSize).toPlainString())}"
                                )
                            }
                        }
                    }
                }
            }

            is ByBitWebSocketMessage.PrivateTopicResponse.Execution -> {

                message.data.forEach {

                    when (it.execType) {
                        ExecType.Trade -> executionUpdateFromWebsocket(it)

                        else -> {
                            logger.warn("execution type ${it.execType} not yet handled")
                            return
                        }
                    }


                }
            }

            is ByBitWebSocketMessage.PrivateTopicResponse.Position -> {
                message.data.forEach {
                    positionUpdateFromWebsocket(it)
                }
            }

            else -> logger.warn("received message=$message")
        }

    }

    private fun positionUpdateFromWebsocket(positionItem: ByBitWebSocketMessage.PositionItem) {

        val asset = assetMap[positionItem.symbol]

        if (asset == null) {
            logger.warn { "Found serverPosition with symbol not in assetMap" }
        } else {
            //val currentPos = _account.portfolio.getOrDefault(asset, Position.empty(asset))
            if (asset.currency !== baseCurrency) return

            val sign = if (positionItem.side == Side.Sell) -1 else 1
            val serverSize = Size(positionItem.size.toDouble().times(sign))

            val serverMargin = positionItem.positionBalance.toDouble()

            val newPosition = Position(
                asset,
                size = serverSize,
                avgPrice = positionItem.entryPrice.toDouble(),
                mktPrice = positionItem.markPrice.toDouble(),
                lastUpdate = Instant.ofEpochMilli(positionItem.updatedTime.toLong()),
//                leverage = positionItem.leverage.toDouble(),
            )

            val calculatedMargin = newPosition.totalCost.absoluteValue.value / positionItem.leverage.toDouble()

            val difference = abs(serverMargin - calculatedMargin)
            val percentDifference = (difference / serverMargin) * 100

            val threshold = 0.25
            if (percentDifference > threshold) {
                logger.warn("Server reported margin(${serverMargin}) and calculated margin(${calculatedMargin}) exceeds threshold ($threshold): $percentDifference")
            }
            _account.setPosition(
                newPosition
            )
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
                    ) {
                        val now = Instant.now()
                        if (orderState.status != OrderStatus.INITIAL
                            && orderState.openedAt.isAfter(now.plusSeconds(2))) { // sometimes server isn't aware immediately!
                            logger.warn(
                                "Completing order that existed in _account.openOrders, but was not found on server:\n "
                                        + "${orderState.order} "
                            )
                            _account.updateOrder(orderState.order, now, OrderStatus.COMPLETED)
                        } else {
                            // if we have some old order stuck in INITIAL status, maybe reject it? Don't have create time...

                            logger.warn(
                                "Found INITIAL order that existed in _account.openOrders, but was not found on server:\n "
                                        + "${orderState.order} ..doing nothing."
                            )
                        }
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

            // WARNING: be sure to sync with what's happening in the WS wallet updates too!

            val p = cyan("API:")

            val walletBalance = Amount(baseCurrency, coinItem.walletBalance.toDouble())
            logger.debug("$p walletBalance: ${blue(walletBalance.toString())}")

            val totalPositionIM = Amount(baseCurrency, coinItem.totalPositionIM.toDouble())
            logger.debug("$p totalPositionIM: ${brightWhite(totalPositionIM.toString())}")

            val totalOrderIM = Amount(baseCurrency, coinItem.totalOrderIM.toDouble())
            logger.debug("$p totalOrderIM: ${yellow(totalOrderIM.toString())}")

            val availableToWithdraw = Amount(baseCurrency, coinItem.availableToWithdraw.toDouble())
            logger.debug("$p availableToWithdraw: ${green(availableToWithdraw.toString())}")

//            val availableToBorrow = Amount(baseCurrency, coinItem.availableToBorrow.toDouble())
//            logger.debug("$p availableToBorrow: ${green(availableToBorrow.toString())} (~ account.buyingPower) (NOT SET)")

            val unrealizedPnL = coinItem.unrealisedPnl.toDouble()

            val cash = walletBalance.value
            val existingCashAmount = _account.cash.convert(baseCurrency)
            if (existingCashAmount.value != cash) {
                logger.debug("_account.cash.set(${dim(yellow(existingCashAmount.toString()))} → ${brightYellow(walletBalance.value.toString())})")
                _account.cash.set(baseCurrency, cash)
            }
        }
    }

    /**
     * Update the portfolio with the provided [position] and return the realized PNL as a consequence of this position
     * change.
     */

//    private fun updatePosition(position: Position): Amount {
//        val asset = position.asset
//        val p = _account.portfolio
//        val currentPos = p.getOrDefault(asset, Position.empty(asset))
//        val newPosition = currentPos + position
//        if (newPosition.closed) p.remove(asset) else p[asset] = newPosition
//        return currentPos.realizedPNL(position)
//    }

    private fun executionUpdateFromWebsocket(execution: ByBitWebSocketMessage.ExecutionItem) {

        val sign = if (execution.side == Side.Buy) 1 else -1

        val execPrice = execution.execPrice.toDouble()

        val execSize = Size(execution.execQty) * sign

        val pre = yellow("WS:")

        logger.debug(
            "$pre EXECUTED ${execution.orderType} ${execution.side} "
                    + cyan(execSize.toString())
                    + " @ ${brightBlue(execPrice.toString())} "
                    + gray(execution.orderLinkId)
        )

        val asset = assetMap[execution.symbol]

        if (asset == null) {
            logger.warn("$pre Received execution for unknown symbol: ${execution.symbol}")
            return
        }

        // Calculate the fees that apply to this execution
        val feeAmount = Amount(asset.currency, execution.execFee.toDouble())
        logger.debug("$pre _account.cash (before): ${brightYellow(_account.cash.toString())}")
        logger.debug("$pre feeAmount: ${brightYellow(feeAmount.toString())}")
//        _account.cash.withdraw(feeAmount)

        val rqOrderId = placedOrders[execution.orderLinkId]

        val executionInstant = Instant.ofEpochMilli(execution.execTime.toLong())

        if (rqOrderId !== null) {
            val rqOrder = _account.getOrder(rqOrderId)
            if (rqOrder !== null && execution.leavesQty == "0") {
                logger.debug("$pre Execution leaves 0 qty, update order status to completed")
                _account.completeOrder(rqOrder, executionInstant)
            }
        } else {
            logger.warn("$pre Received execution order not placed in system. orderLinkId : ${execution.orderLinkId}, orderId : ${execution.orderId}")
        }

        val execPos = Position(asset, execSize, execPrice)

        // sanity check: MAKE SURE that when I'm opening or adding to position, that my PnL only reflects the fee

        val p = _account.portfolio
        val existingPos = p.getOrDefault(asset, Position.empty(asset))
        val newPosition = existingPos + execPos // simply calculate size and avgPrice if adding to position
        if (newPosition.closed) p.remove(asset) else p[asset] = newPosition

        val pnl = existingPos.realizedPNL(execPos) - feeAmount

        val newTrade = Trade(
            executionInstant,
            asset,
            execSize,
            execPrice,
            feeAmount.value,
            pnl.convert(asset.currency).value,
            rqOrderId ?: -1
        )

        logger.debug("$pre pnl: ${brightYellow(pnl.toString())} (incl. fee to close)")

        _account.addTrade(newTrade)

        _account.cash.deposit(pnl)

        logger.debug("$pre _account.cash (after): ${brightYellow(_account.cash.toString())}")

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
                    val orderLinkId = placedOrders.entries.find { it.value == order.order.id }?.key
                    if (orderLinkId != null) {
//                            placedOrders[orderLinkId] = order.id
//                            logger.debug("amend() will update status for orderLinkId: ${orderLinkId}")

//                            if (_account.orders.none { order.tag == it.order.tag }) {
                        amend(orderLinkId, symbol, order)
//                            } else {
//                                logger.info("not amending order b/c existing _account.orders with same tag")
//                            }

                    } else {
                        logger.warn("Unable to find placed order of order to update, rejecting amend")
                        _account.rejectOrder(order, Instant.now())
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
        logger.debug("Cancelling $orderLinkId")

        if (orderLinkId == null) {
            logger.error("Unable to find order.id in placedOrders: ${cancellation.order.id}")
            return
        }

        executeFun {
            val accountOrder = _account.getOrder(cancellation.id) as CancelOrder? ?: return@executeFun

            try {
                val order = cancellation.order
                client.orderClient.cancelOrderBlocking(
                    CancelOrderParams(
                        category,
                        symbol = order.asset.symbol,
                        orderLinkId = orderLinkId
                    )
                )
                _account.completeOrder(cancellation, Instant.now())
//                _account.updateOrder(cancellation.order, Instant.now(), OrderStatus.CANCELLED) // handled by WS

            } catch (e: CustomResponseException) {
                logger.warn {
                    "Failed (${red(e.message)}) cancelling order: ${
                        yellow(
                            accountOrder.toString()
                        )
                    }"
                }

                val now = Instant.now()
                when (e.retCode) {
                    110001 -> { // order does not exist on server
                        if (_account.getOrder(accountOrder.order.id) !== null) {
                            logger.warn("Completing order that was to be amended b/c did not exist on server")
                            _account.completeOrder(accountOrder.order, now)
                        }
                        _account.rejectOrder(accountOrder, now)
                    }

                    else -> {
                        logger.warn(e.retMsg)
                        _account.rejectOrder(accountOrder, now)
                    }
                }
            } catch (e: RateLimitReachedException) {
                logger.warn("Reached rate limit ${e.message}")
                _account.rejectOrder(accountOrder, Instant.now())
            } catch (e: Exception) {
                _account.rejectOrder(accountOrder, Instant.now())
                logger.error(e.message)
            }
        }
    }


    private fun cancelAllOrders(cancelOrders: List<CancelOrder>) {
        logger.debug { "Running cancelAllOrders()" }
        executeFun {
            try {
                val response = client.orderClient.cancelAllOrdersBlocking(
                    CancelAllOrdersParams(category, symbol = cancelOrders[0].asset.symbol)
                )

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
        val orderId = order.id
        executeFun {
            val reduceOnly = order.tag.startsWith("tpOrder")

            val accountOrder = _account.getOrder(orderId) ?: return@executeFun

            try {
                client.orderClient.placeOrderBlocking(
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

                _account.acceptOrder(accountOrder, Instant.now())

                logger.debug {
                    "Server accepts create ${yellow(accountOrder.toString())} ${
                        gray(
                            orderLinkId
                        )
                    }"
                }
            } catch (e: CustomResponseException) {
                logger.warn {
                    "Failed (${red(e.message)}) create order: ${
                        yellow(
                            accountOrder.toString()
                        )
                    }"
                }

                val now = Instant.now()
                when (e.retCode) {
                    10001 -> {
                        logger.warn("parameter error")
                        _account.rejectOrder(accountOrder, now)
                    }

                    110079 -> {
                        logger.warn(e.retMsg)
                        _account.rejectOrder(accountOrder, now)
                    }

                    else -> {
                        _account.rejectOrder(accountOrder, now)
                    }
                }
            } catch (e: RateLimitReachedException) {
                logger.warn(e.message)
                _account.rejectOrder(accountOrder, Instant.now())
            } catch (e: Exception) {
                _account.rejectOrder(accountOrder, Instant.now())
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

        val orderId = order.id
        executeFun {
            val accountOrder = _account.getOrder(orderId) as MarketOrder? ?: return@executeFun

            try {
                client.orderClient.placeOrderBlocking(
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

                _account.completeOrder(accountOrder, Instant.now())

                logger.debug {
                    "Server accepts Market order ${yellow(accountOrder.toString())} ${
                        gray(
                            orderLinkId
                        )
                    }"
                }

            } catch (e: CustomResponseException) {
                logger.warn {
                    "Failed (${red(e.message)}) market order: ${
                        yellow(
                            order.toString()
                        )
                    }"
                }

                val now = Instant.now()
                when (e.retCode) {
                    10001 -> {
                        logger.warn("parameter error")
                        _account.rejectOrder(accountOrder, now)
                    }

                    110079 -> {
                        logger.warn(e.retMsg)
                        _account.rejectOrder(accountOrder, now)
                    }

                    else -> {
                        _account.rejectOrder(accountOrder, now)
                    }
                }


            } catch (e: RateLimitReachedException) {
                logger.warn(e.message)
                _account.rejectOrder(accountOrder, Instant.now())
            } catch (e: Exception) {
                _account.rejectOrder(accountOrder, Instant.now())
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
                            "Amending order qty: ${dim(yellow(originalLimitOrder.size.toString()))} → ${
                                brightYellow(
                                    updatedLimitOrder.size.toString()
                                )
                            } " + gray(
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
                            "Amending order price from ${originalLimitOrder.limit} to ${updatedLimitOrder.limit} " + gray(
                                orderLinkId
                            )
                        )
                        updatedLimitOrder.limit.toString()
                    }
                }
                val updateOrderId = updateOrder.id

                executeFun {
                    val updateAOrder = _account.getOrder(updateOrderId) as UpdateOrder? ?: return@executeFun

                    if (_account.getOrder(updateAOrder.order.id) == null) {
                        logger.warn("Rejecting amend for order that is no longer open")
                        _account.rejectOrder(updateAOrder, Instant.now())
                    }
                    val timeSpent = measureTimeMillis {

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

                            val now = Instant.now()
                            _account.updateOrder(updateAOrder.update, now, OrderStatus.ACCEPTED)
                            _account.completeOrder(updateAOrder, now)

                            logger.debug {
                                "Server accepts amend ${brightYellow(updateAOrder.toString())} ${gray(orderLinkId)}"
                            }
                            //placedOrders[orderLinkId] = updateOrder.update.id // turns out this isn't needed, same id

                        } catch (e: CustomResponseException) {
                            logger.warn {
                                "Failed (${red(e.message)}) amending order: ${yellow(updateAOrder.toString())}"
                            }

                            val now = Instant.now()
                            when (e.retCode) {
                                110001 -> { // order does not exist on server
                                    _account.rejectOrder(updateAOrder, now)
                                    if (_account.getOrder(updateAOrder.order.id) !== null) {
                                        logger.warn("Completing order that was to be amended b/c did not exist on server")
                                        _account.completeOrder(updateAOrder.order, now)
                                    }
                                }

                                10001 -> {
                                    logger.warn("parameter error, maybe amend is not different from existing")
                                    _account.rejectOrder(updateAOrder, now)
                                }

                                110079 -> {
                                    logger.warn(e.retMsg)
                                    _account.rejectOrder(updateAOrder, now)
                                }

                                else -> {
                                    _account.rejectOrder(updateAOrder, now)
                                }
                            }

                        } catch (e: RateLimitReachedException) {
                            logger.warn(e.message)
                            _account.rejectOrder(updateAOrder, Instant.now())
                        } catch (e: Exception) {
                            _account.rejectOrder(updateAOrder, Instant.now())
                            logger.error(e.message)
                        }
                    }
                    logger.info(
                        "Spent ${
                            brightBlue(
                                timeSpent.toString()
                            )
                        } amending order"
                    )
                }
            }

            else -> {
                throw Exception("Tried to amend non-limit order type: ${updateOrder.update::class}")
            }
        }

    }

    private fun executeFun(myfunc: () -> Unit) {
        CoroutineScope(Dispatchers.IO).launch {
            try {
                myfunc()
            } catch (e: Exception) {
                logger.error { e.message.toString() }
            }
        }
    }

}

