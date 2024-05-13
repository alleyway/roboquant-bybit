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
import io.ktor.util.reflect.*
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.channels.Channel
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
import kotlin.math.roundToInt
import kotlin.system.measureTimeMillis
import kotlin.time.Duration.Companion.milliseconds


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

    val api = yellow(dim("API:"))
    val ws = cyan(dim("WS:"))

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
            // We don't want to miss these critical WS messages relating to execution/orders/position!
            val channel = wsClient.getWebSocketEventChannel(Channel.UNLIMITED, BufferOverflow.SUSPEND)
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
                    _account.lastUpdate = Instant.now()
                } catch (e: Exception) {
                    logger.error { "Caught exception trying to update account from API: \n ${e.stackTraceToString()}" }
                }
            }

        }

        accountModel.updateAccount(_account)
        _account.updateMarketPrices(event)
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
                        logger.warn("$api Received position info for unknown symbol: ${serverPosition.symbol}")
                        return
                    }

                    // excluding other assets besides the one we're trading for now
                    if (asset.currency !== baseCurrency) return

                    val sign = if (serverPosition.side == Side.Sell) -1 else 1
                    val serverSize = Size(serverPosition.size.toDouble().times(sign))
                    val serverLeverage = serverPosition.leverage.toDouble()

                    val currentPos = p.getOrDefault(asset, Position.empty(asset))

                    if (serverSize.iszero && p.containsKey(asset)) {
                        p.remove(asset)
                    } else {
                        if (currentPos.size != serverSize || currentPos.leverage != serverLeverage) {
                            logger.warn { "Local position (${currentPos.size})/leverage (${currentPos.leverage}) Different from server(${serverSize})/(${serverLeverage}) when syncing" }
                            _account.setPosition(
                                Position(
                                    asset,
                                    size = serverSize,
                                    avgPrice = serverPosition.avgPrice.toDouble(),
                                    mktPrice = serverPosition.markPrice.toDouble(),
                                    lastUpdate = Instant.ofEpochMilli(serverPosition.updatedTime.toLong()),
//                                margin = serverPosition.positionIM.toDouble()
                                        leverage = serverLeverage,
                                )
                            )
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

        logger.warn("${magenta("WS:")} ${message::class.simpleName}")

        when (message) {

            is ByBitWebSocketMessage.StatusMessage -> {
//                message.op == "auth"
            }

            is ByBitWebSocketMessage.RawMessage -> {
                logger.debug { message.data }
            }

            is ByBitWebSocketMessage.PrivateTopicResponse.Order -> {

                val orderItems = message.data.sortedBy { it.updatedTime }

                for (orderItem in orderItems) {
                    logger.warn { "$ws server order: $orderItem" }

                    val rqOrderId = placedOrders[orderItem.orderLinkId]

                    if (rqOrderId == null) {
                        logger.warn { "$ws received update for unknown server order: $orderItem" }
                        continue
                    }

                    val asset = assetMap[orderItem.symbol]

                    if (asset == null) {
                        logger.warn("$ws Received execution for unknown symbol: ${orderItem.symbol}")
                        return
                    }


                    val accountOrder = _account.getOrder(rqOrderId) ?: continue

                    when (orderItem.orderStatus) {
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
                            logger.debug { "$ws order Filled." }
                            // execution also might also update the order..

//                            val filledTrades = _account.trades.toList().filter { it.orderId == rqOrderId }
//                            handleTradeExecution(TradeExecution.fromFilledOrder(asset, orderItem, filledTrades))
                        }

                        bybit.sdk.shared.OrderStatus.PartiallyFilled -> {
                            logger.debug { "$ws order PartiallyFilled." }

//                            val filledTrades = _account.trades.toList().filter { it.orderId == rqOrderId }
//                            handleTradeExecution(TradeExecution.fromFilledOrder(asset, orderItem, filledTrades))

                        }

                        bybit.sdk.shared.OrderStatus.Cancelled,
                        bybit.sdk.shared.OrderStatus.PartiallyFilledCanceled
                        -> {
//                            logger.debug {
//                                brightBlue(
//                                    "$ws cancelled accountOrder (${accountOrder.id}): ${yellow(order.rejectReason)} " + gray(
//                                        order.orderLinkId
//                                    )
//                                )
//                            }
//                            when (order.rejectReason) {
//                                "EC_PostOnlyWillTakeLiquidity" -> {
//                                    _account.rejectOrder(accountOrder, Instant.now())
//                                }
//
//                                else -> {
//                                    _account.updateOrder(accountOrder, Instant.now(), OrderStatus.CANCELLED)
//                                }
//                            }

                        }

//                bybit.sdk.shared.OrderStatus.Exp ->
//                    _account.updateOrder(state, Instant.now(), OrderStatus.EXPIRED)

                        bybit.sdk.shared.OrderStatus.Rejected -> {
                            logger.debug { "$ws server rejects order: $orderItem" }
                            _account.rejectOrder(accountOrder, Instant.now())
                        }

                        else -> {
                            // NOTE: an amended order will show up here with OrderStatus "New"
                            logger.debug(
                                "$ws ( price: ${(accountOrder as LimitOrder).limit} ) with orderLinkId: " + gray(
                                    orderItem.orderLinkId
                                )
                            )
                            _account.acceptOrder(accountOrder, Instant.now())
                        }
                    }
                }

            }

            is ByBitWebSocketMessage.PrivateTopicResponse.Wallet -> {

                for (coinItem in (message.data.filter { it.accountType == AccountType.CONTRACT }).first().coin) {
                    if (coinItem.coin == baseCurrency.currencyCode) {

                        // https://www.bybit.com/fil-PH/help-center/article/Derivatives-or-Inverse-Derivatives-Acccount

                        // WARNING: be sure to sync with what's happening in the WS wallet updates too!


                        val walletBalance = Amount(baseCurrency, coinItem.walletBalance.toDouble())
                        logger.debug("$ws walletBalance: ${blue(walletBalance.toString())}")

                        val totalPositionIM = Amount(baseCurrency, coinItem.totalPositionIM.toDouble())
                        logger.debug("$ws totalPositionIM: ${brightWhite(totalPositionIM.toString())}")

                        val totalOrderIM = Amount(baseCurrency, coinItem.totalOrderIM.toDouble())
                        logger.debug("$ws totalOrderIM: ${yellow(totalOrderIM.toString())}")

//                        val totalPositionMM = Amount(baseCurrency, coinItem.totalPositionMM.toDouble())
//                        logger.debug("$ws totalPositionMM: ${yellow(totalPositionMM.toString())}")

                        val availableToWithdraw = Amount(baseCurrency, coinItem.availableToWithdraw.toDouble())
                        logger.debug("$ws availableToWithdraw: ${green(availableToWithdraw.toString())}")


//                        val availableToBorrow = Amount(baseCurrency, coinItem.availableToBorrow.toDouble())
//                        logger.debug("$ws availableToBorrow: ${green(availableToBorrow.toString())} (~ account.buyingPower) (NOT SET)")

                        // -- Equity = Wallet Balance + Unrealized P&L (in Mark Price)
                        //val equity = coinItem.equity.toDouble()

                        val unrealizedPnL = coinItem.unrealisedPnl.toDouble()

                        val loss = if (unrealizedPnL.isFinite() && unrealizedPnL < 0) {
                            unrealizedPnL
                        } else 0.0


                        val cash = walletBalance.value
                        val existingCashAmount = _account.cash.convert(baseCurrency)
                        if (existingCashAmount != walletBalance) {
                            logger.debug(
                                "$ws _account.cash.set(${dim(yellow(existingCashAmount.toString()))} → ${
                                    brightYellow(
                                        walletBalance.value.toString()
                                    )
                                })"
                            )
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

                    val asset = assetMap[it.symbol]

                    if (asset == null) {
                        logger.warn("$ws Received execution for unknown symbol: ${it.symbol}")
                        return
                    }

                    when (it.execType) {
                        ExecType.Trade -> {
                            handleTradeExecution(TradeExecution.fromExecutionItem(asset, it))
                        }
                        ExecType.Funding -> {

                            val fundingFee = it.execFee.toDouble()
                            val execPrice = it.execPrice.toDouble()
                            val usdValue = fundingFee * execPrice
                            logger.warn("T: Funding fee: $fundingFee ($${usdValue}) , but not yet handled")
                            // handleTradeExecution(TradeExecution.fromFunding(asset, it))
                        }
                        else -> {
                            logger.warn("T: execution type ${it.execType} not yet handled")
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

//            val serverMargin = positionItem.positionBalance.toDouble()

            val newPosition = Position(
                asset,
                size = serverSize,
                avgPrice = positionItem.entryPrice.toDouble(),
                mktPrice = positionItem.markPrice.toDouble(),
                lastUpdate = Instant.ofEpochMilli(positionItem.updatedTime.toLong()),
//                leverage = positionItem.leverage.toDouble(),
            )
            logger.warn("$ws position update: ${brightYellow(newPosition.toString())}")
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
                val serverOrdersByOrderLinkId = ordersOpenResponse.result.list.associateBy { it.orderLinkId }

                placedOrders.entries.forEach {
                    val orderState = _account.getOrderState(it.value)
                    if (orderState != null) {
                        val matchingServerOrder = serverOrdersByOrderLinkId[it.key]
                        val now = Instant.now()
                        if (matchingServerOrder != null) {
                            val singleOrder = orderState.order as LimitOrder
                            val leavesQty = matchingServerOrder.leavesQty.toInt()
                            if (leavesQty != singleOrder.size.absoluteValue.toDouble().roundToInt()) {

                                val newSize = Size(leavesQty * singleOrder.size.sign)
                                val newOrder = LimitOrder(
                                    orderState.order.asset,
                                    newSize,
                                    singleOrder.limit,
                                    singleOrder.tif, singleOrder.tag
                                )
                                newOrder.id = singleOrder.id
                                logger.warn(
                                    "Server order leavesQty($leavesQty) different from local($newSize)." +
                                            " updating _account order. orderLinkId: ${gray(it.key)}"
                                )
                                _account.updateOrder(newOrder, now, orderState.status)
                            }
                        } else { // couldn't find our placed order on the server

                            if (orderState.status != OrderStatus.INITIAL
                                && orderState.openedAt.isBefore(now.minusSeconds(2))
                            ) { // sometimes server isn't aware immediately!
                                logger.warn(
                                    "Completing order that existed in _account.openOrders, but was not found on server:\n "
                                            + "${orderState.order} "
                                )
                                _account.completeOrder(orderState.order, now)
                            } else {
                                // if we have some old order maybe stuck in INITIAL status, maybe reject it? Don't have create time...

                                logger.warn(
                                    "Found order with state (${orderState.status}) existing in _account.openOrders, but not found on server:\n "
                                            + "${orderState.order} ..doing nothing."
                                )

                            }
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


            val walletBalance = Amount(baseCurrency, coinItem.walletBalance.toDouble())
            logger.debug("$api walletBalance: ${blue(walletBalance.toString())}")

            val totalPositionIM = Amount(baseCurrency, coinItem.totalPositionIM.toDouble())
            logger.debug("$api totalPositionIM: ${brightWhite(totalPositionIM.toString())}")

            val totalOrderIM = Amount(baseCurrency, coinItem.totalOrderIM.toDouble())
            logger.debug("$api totalOrderIM: ${yellow(totalOrderIM.toString())}")

//            val totalPositionMM = Amount(baseCurrency, coinItem.totalPositionMM.toDouble())
//            logger.debug("$api totalPositionMM: ${yellow(totalPositionMM.toString())}")

            val availableToWithdraw = Amount(baseCurrency, coinItem.availableToWithdraw.toDouble())
            logger.debug("$api availableToWithdraw: ${green(availableToWithdraw.toString())}")

//            val availableToBorrow = Amount(baseCurrency, coinItem.availableToBorrow.toDouble())
//            logger.debug("$api availableToBorrow: ${green(availableToBorrow.toString())} (~ account.buyingPower) (NOT SET)")

            val unrealizedPnL = coinItem.unrealisedPnl.toDouble()

            val cash = walletBalance.value
            val existingCashAmount = _account.cash.convert(baseCurrency)
            if (existingCashAmount.value != cash) {
                logger.debug(
                    "_account.cash.set(${dim(yellow(existingCashAmount.toString()))} → ${
                        brightYellow(
                            walletBalance.value.toString()
                        )
                    })"
                )
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

    private fun handleTradeExecution(execution: TradeExecution) {


        val execPrice = execution.execPrice

        val execSize = execution.execSize

        val asset = execution.asset

        logger.warn(
            "$ws EXECUTED ${execution.orderType} "
                    + cyan(execSize.toString())
                    + " @ ${brightBlue(execPrice.toString())} "
                    + gray(execution.orderLinkId)
        )


        // Calculate the fees that apply to this execution

        logger.debug("$ws _account.cash (before): ${brightYellow(_account.cash.toString())}")
        logger.debug("$ws feeAmount: ${brightYellow(execution.feeAmount.toString())}")
//        _account.cash.withdraw(feeAmount)

        val rqOrderId = placedOrders[execution.orderLinkId]

        val executionInstant = execution.execInstant

        if (rqOrderId !== null) {
            val rqOrder = _account.getOrder(rqOrderId)
            val now = Instant.now()
            when (rqOrder) {
                is LimitOrder -> {
                    if (execution.leavesQty == "0") {
                        logger.debug("$ws Execution leaves 0 qty, update order status to completed")
                        _account.completeOrder(rqOrder, executionInstant)
                    } else {
                        logger.warn("Leaves non-zero qty: ${yellow(execution.leavesQty)}")
                        val newOrder = LimitOrder(
                            rqOrder.asset,
                            Size(execution.leavesQty.toDouble() * rqOrder.size.sign),
                            rqOrder.limit,
                            rqOrder.tif, rqOrder.tag
                        )
                        newOrder.id = rqOrder.id
                        logger.warn("replacing with order: ${yellow(newOrder.toString())}")
                        _account.acceptOrder(newOrder, now)
                    }
                }

                is MarketOrder -> {
                    if (execution.leavesQty == "0") {
                        logger.warn("was a MARKET order to handle...odd")
                        logger.debug("$ws Execution leaves 0 qty, update order status to completed")
                        _account.completeOrder(rqOrder, executionInstant)
                    }
                }

                null -> {
                    logger.warn("$ws _account.getOrder(placedOrders[execution.orderLinkId ${gray(execution.orderLinkId)}])  = null | rqOrderId: $rqOrderId | NOT finishing TradeExecution")
                    // this could happen if Execution & Order messages come in at the same time and both "complete" the order
                    return
                }

                else -> {
                    logger.warn("$ws unhandled order type")
                }
            }
        } else {
            logger.warn("$ws Received execution order not placed in system. orderLinkId : ${execution.orderLinkId}, orderId : ${execution.orderId}")
        }

        val execPos = Position(asset, execSize, execPrice)

        // sanity check: MAKE SURE that when I'm opening or adding to position, that my PnL only reflects the fee

        val p = _account.portfolio
        val existingPos = p.getOrDefault(asset, Position.empty(asset))
        val newPosition = existingPos + execPos // simply calculate size and avgPrice if adding to position
        if (newPosition.closed) p.remove(asset) else p[asset] = newPosition

        val pnl = existingPos.realizedPNL(execPos) - execution.feeAmount

        val newTrade = Trade(
            executionInstant,
            asset,
            execSize,
            execPrice,
            execution.feeAmount.value,
            pnl.convert(asset.currency).value,
            rqOrderId ?: -1
        )

        logger.debug("$ws pnl: ${brightYellow(pnl.toString())} (incl. fee to close)")

        _account.addTrade(newTrade)

        _account.cash.deposit(pnl)

        logger.debug("$ws _account.cash (after): ${brightYellow(_account.cash.toString())}")

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
        logger.warn("cancelOrder(): ${red(cancellation.toString())} ${gray(orderLinkId.toString())}")

        if (orderLinkId == null) {
            logger.error("Unable to find order.id in placedOrders: ${cancellation.order.id}")
            return
        }

        executeFun {
            val accountOrder = _account.getOrder(cancellation.id) as CancelOrder? ?: return@executeFun

            try {
                val order = cancellation.order

                val resp = client.orderClient.cancelOrderBlocking(
                    CancelOrderParams(
                        category,
                        symbol = order.asset.symbol,
                        orderLinkId = orderLinkId
                    )
                )

                logger.warn("cancelled ${white(resp.result.orderId)} | ${gray(resp.result.orderLinkId)}")
                val now = Instant.now()
                _account.completeOrder(cancellation, now)
                val orderToCancel = _account.getOrder(cancellation.order.id)
                orderToCancel?.let {
                    _account.updateOrder(orderToCancel, now, OrderStatus.CANCELLED)
                }

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
                        val freshAccountOrder = _account.getOrderState(accountOrder.order.id)
                        if (freshAccountOrder !== null && freshAccountOrder.openedAt.isBefore(now.minusMillis(200))) {
                            logger.warn("Completing order that was to be cancelled b/c did not exist on server")
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
                                    _account.completeOrder(cancelOrder, Instant.now())
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
                val resp = client.orderClient.placeOrderBlocking(
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

                logger.warn {
                    "Server accepts create ${yellow(accountOrder.toString())} orderId: ${
                        white(
                            resp.result.orderId
                        )
                    } | orderLinkId: ${
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
                        logger.warn { "rejecting order. retCode: ${e.retCode}, message: ${e.message}" }
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
                        logger.debug(
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
                        logger.warn(
                            "_account.getOrder(updateAOrder.order.id ${gray(updateAOrder.order.id.toString())}) == null | Rejecting ${
                                dim(
                                    yellow(updateAOrder.toString())
                                )
                            }"
                        )
                        _account.rejectOrder(updateAOrder, Instant.now())
                    } else {

                        val timeSpent = measureTimeMillis {

                            try {
                                val resp = client.orderClient.amendOrderBlocking(
                                    AmendOrderParams(
                                        category,
                                        symbol,
                                        orderLinkId = orderLinkId,
                                        qty = qty,
                                        price = price
                                    )
                                )

                                val now = Instant.now()
                                _account.acceptOrder(updateAOrder.update, now)
                                _account.completeOrder(updateAOrder, now)

                                logger.warn {
                                    "Server accepts amend ${yellow(updateAOrder.toString())} orderId: ${
                                        white(
                                            resp.result.orderId
                                        )
                                    } | orderLinkId: ${
                                        gray(
                                            orderLinkId
                                        )
                                    }"
                                }
                                //placedOrders[orderLinkId] = updateOrder.update.id // turns out this isn't needed, same id

                            } catch (e: CustomResponseException) {
                                logger.warn {
                                    "Failed (${red(e.message)}) amending order: ${yellow(updateAOrder.toString())}"
                                }

                                val now = Instant.now()
                                when (e.retCode) {
                                    110001 -> { // order does not exist on server

                                        logger.warn("rejecting update for order that did not exist on server..will 10ms wait before completing")
                                        _account.rejectOrder(updateAOrder, now)

                                        runBlocking {
                                            delay(10.milliseconds)
                                            val freshAccountOrder = _account.getOrderState(updateAOrder.order.id)
                                            if (freshAccountOrder !== null) {
                                                logger.warn("Order that was to be amended did not exist on server")
                                                //TODO: MAYBE handle trade execution ???
//                                                handleTradeExecution(TradeExecution(
//                                                    updateAOrder.asset,
//                                                    "",
//
//                                                ))
                                                _account.completeOrder(updateAOrder.order, now)

                                            }
                                        }
                                    }

                                    10001 -> {
                                        logger.warn("parameter error, maybe amend is not different from existing")
                                        _account.rejectOrder(updateAOrder, now)

                                        val origOrder =
                                            _account.getOrder(updateAOrder.order.id) as LimitOrder? ?: return@executeFun

                                        if ((updateAOrder.update as LimitOrder).size != origOrder.size) {
                                            val newOrder = LimitOrder(
                                                origOrder.asset,
                                                (updateAOrder.update as LimitOrder).size,
                                                origOrder.limit,
                                                origOrder.tif, origOrder.tag
                                            )
                                            newOrder.id = origOrder.id
                                            logger.warn(
                                                "Replacing existing _account order size to match server. ${
                                                    yellow(
                                                        dim(origOrder.toString())
                                                    )
                                                } -> ${yellow(dim(newOrder.toString()))}"
                                            )
                                            _account.updateOrder(newOrder, Instant.now(), OrderStatus.ACCEPTED)
                                        }

                                    }

                                    110079 -> {
                                        logger.warn(e.retMsg)
                                        _account.rejectOrder(updateAOrder, now)
                                    }

                                    else -> {
                                        logger.warn("Unhandled error code, rejecting UpdateOrder")
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
                        logger.debug(
                            "Spent ${
                                brightBlue(
                                    timeSpent.toString()
                                )
                            } amending order"
                        )
                    }
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

