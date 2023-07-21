@file:Suppress("unused", "WildcardImport")

package org.roboquant.bybit

import bybit.sdk.rest.ByBitRestClient
import bybit.sdk.rest.account.WalletBalanceParams
import bybit.sdk.rest.order.*
import bybit.sdk.rest.position.PositionInfoParams
import bybit.sdk.shared.*
import bybit.sdk.websocket.*
import com.github.ajalt.mordant.rendering.TextColors
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.async
import org.roboquant.brokers.*
import org.roboquant.brokers.sim.execution.InternalAccount
import org.roboquant.common.*
import org.roboquant.common.Currency
import org.roboquant.feeds.Event
import org.roboquant.orders.*
import org.roboquant.orders.OrderStatus
import java.math.RoundingMode
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

    init {
        config.configure()
        client = ByBit.getRestClient(config)

        val wsOptions = WSClientConfigurableOptions(
            ByBitEndpoint.Private,
            config.apiKey,
            config.secret,
            config.testnet
        )
        wsClient = ByBit.getWebSocketClient(wsOptions, this::handler)
        assetMap = ByBit.availableAssets(client, category)

//        updateAccount()
        sync()
        wsClient.connectBlocking()

        wsClient.subscribeBlocking(
            listOf(
                ByBitWebSocketSubscription(ByBitWebsocketTopic.PrivateTopic.Execution),
                ByBitWebSocketSubscription(ByBitWebsocketTopic.PrivateTopic.Order),
                ByBitWebSocketSubscription(ByBitWebsocketTopic.PrivateTopic.Wallet)
            )
        )

    }

    fun sync() {
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
                        _account.setPosition(
                            Position(
                                it,
                                size = Size(position.positionValue),
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
                logger.info { message.data }
            }

            is ByBitWebSocketMessage.PrivateTopicResponse.Order -> {

                for (order in message.data) {
                    val orderId = placedOrders[order.orderLinkId] ?: continue
                    val state = _account.getOrder(orderId) ?: continue

                    when (order.orderStatus) {
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

                        else -> _account.updateOrder(state, Instant.now(), OrderStatus.ACCEPTED)
                    }
                }

            }

            is ByBitWebSocketMessage.PrivateTopicResponse.Wallet -> {

                for (coinItem in (message.data.filter { it.accountType == AccountType.CONTRACT }).first().coin) {
                    if (coinItem.coin == "BTC") {
                        _account.cash.set(Currency.getInstance(coinItem.coin), coinItem.availableToWithdraw.toDouble())
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

                                logger.debug (
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
                        ExecType.Trade -> updateAccount(it)

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
            _account.cash.set(Currency.getInstance(coinItem.coin), coinItem.walletBalance.toDouble())

        // think i'm suppose to multiple my leverage by cash
//            _account.buyingPower = Amount(baseCurrency, coinItem.availableToBorrow.toDouble())
        }
    }

    private fun updatePosition(position: Position): Amount {
        val asset = position.asset
        val p = _account.portfolio
        val currentPos = p.getOrDefault(asset, Position.empty(asset))
        val newPosition = currentPos + position
        if (newPosition.closed) p.remove(asset) else p[asset] = newPosition
        return currentPos.realizedPNL(position)
    }

    private fun updateAccount(execution: ByBitWebSocketMessage.ExecutionItem) {

        val asset = assetMap.get(execution.symbol)

        if (asset == null) {
            logger.warn("Received execution for unknown symbol: ${execution.symbol}")
            return
        }

        val rqOrderId = placedOrders[execution.orderLinkId]

        if (rqOrderId == null) {
            logger.warn("Received execution order not placed in system. orderLinkId : ${execution.orderLinkId}, orderId : ${execution.orderId}")
            return
        }

        val sign = if (execution.side == Side.Buy) 1 else -1

        val execPrice = execution.execPrice.toDouble()

        val execSize = Size(execution.execQty) * sign

        logger.info(" Executed ${execution.orderType} ${execution.side} "
                    + TextColors.cyan(execSize.toString())
                    + " @ ${TextColors.brightBlue(execPrice.toString())} "
                    + TextColors.gray(execution.execTime)
        )

        val position = Position(asset, execSize, execPrice)

        // Calculate the fees that apply to this execution
        val fee = execution.execFee.toDouble()

        val pnl = updatePosition(position) - fee

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
        _account.cash.withdraw(newTrade.totalCost)

    }

    @Deprecated("No longer used")
    private fun updateAccountOld() {

        // START this is very specific to BTCUSDT..just wanted a way to have a position
//        val walletBalanceResp = client.accountClient.getWalletBalanceBlocking(WalletBalanceParams(AccountType.SPOT))
//        for (coinItem in walletBalanceResp.result.list.first().coin) {
//            if (coinItem.coin == "BTC") {
//                assetMap.get("BTCUSDT")?.let {
//                    // or should I use equity??
//                    _account.setPosition(
//                        Position(
//                            it,
//                            Size(
//                                coinItem.walletBalance.toBigDecimal().setScale(8, RoundingMode.DOWN)
//                            )
//                        )
//                    )
//                }
//            }
//        }
        // END specific to bitcoin

        val ordersOpenPaginated = client.orderClient.ordersOpenPaginated(
            OrdersOpenParams(
                category,
                symbol = "BTCUSD"
            )
        )

        for (order in ordersOpenPaginated) {
            val orderId = placedOrders[order.orderLinkId] ?: continue
            val state = _account.getOrder(orderId) ?: continue

            when (order.orderStatus) {
                bybit.sdk.shared.OrderStatus.Filled ->
                    _account.updateOrder(state, Instant.now(), OrderStatus.COMPLETED)

                bybit.sdk.shared.OrderStatus.Cancelled,
                bybit.sdk.shared.OrderStatus.PartiallyFilledCanceled
                ->
                    _account.updateOrder(state, Instant.now(), OrderStatus.CANCELLED)

//                bybit.sdk.shared.OrderStatus.Exp ->
//                    _account.updateOrder(state, Instant.now(), OrderStatus.EXPIRED)

                bybit.sdk.shared.OrderStatus.Rejected ->
                    _account.updateOrder(state, Instant.now(), OrderStatus.REJECTED)

                else -> _account.updateOrder(state, Instant.now(), OrderStatus.ACCEPTED)
            }

        }

    }

    /**
     * @param orders
     * @return
     */
    override fun place(orders: List<Order>, event: Event): Account {
        logger.trace { "Received ${orders.size} orders at ${event.time}" }
        _account.initializeOrders(orders)

        for (order in orders) {
            when (order) {
                is CancelOrder -> {
                    cancelOrder(order)
                }

                is LimitOrder -> {
                    val symbol = order.asset.symbol
                    val orderLinkId = genOrderLinkId()
                    placedOrders[orderLinkId] = order.id
                    trade(orderLinkId, symbol, order)
                }

                is MarketOrder -> {
                    val symbol = order.asset.symbol
                    val orderLinkId = genOrderLinkId()
                    val assetPrice = event.prices.get(order.asset)?.getPrice()
                    placedOrders[orderLinkId] = order.id
                    trade(orderLinkId, symbol, order, assetPrice)
                }

                is UpdateOrder -> {
                    val symbol = order.asset.symbol
                    val updatedOrder = amend(symbol, order)
//                    placedOrders[updatedOrder.result.orderId] = order.id
                }

                else -> logger.warn {
                    "supports only cancellation, market and limit orders, received ${order::class} instead"
                }
            }

        }
        _account.updateMarketPrices(event)
        _account.lastUpdate = event.time

        // maybe supposed to update account?


        return account
    }


    /**
     * Cancel an order
     *
     * @param cancellation
     */
    private fun cancelOrder(cancellation: CancelOrder) {

        val orderLinkId = placedOrders.entries.find { it.value == cancellation.order.id }?.key

        GlobalScope.async {

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
                logger.error { e.message }
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

        GlobalScope.async {

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
                        amount.toPlainString(),
                        price.toPlainString(),
                        orderLinkId = orderLinkId
                    )
                )
            } catch (e: Exception) {
                logger.error { e.message }
            }
        }
    }

    /**
     * Place a market order for a currency pair
     *
     * @param symbol
     * @param order
     */
    private fun trade(orderLinkId: String, symbol: String, order: MarketOrder, price: Double?) {

        var amount = order.size.absoluteValue

        if (category == Category.spot) amount *= price!!

        // ByBit peculiarity!! for SPOT ONLY!
        // Order quantity. For Spot Market Buy order, please note that qty should be quote currency amount

        GlobalScope.async {
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
            } catch (e: Exception) {
                logger.error { e.message }
            }
        }


    }


    // only applies to derivatives, not SPOT!
    private fun amend(symbol: String, order: UpdateOrder): AmendOrderResponse {

        when (order.update) {
            is LimitOrder -> {

                val limitOrder = order.update as LimitOrder
                val orderLinkId = placedOrders.entries.find { it.value == order.order.id }?.key

                val qty = limitOrder.size.toString()
                val updatedOrder = client.orderClient.amendOrderBlocking(
                    AmendOrderParams(
                        "inverse",
                        symbol,
                        orderLinkId = orderLinkId,
                        qty = qty,
                        price = limitOrder.limit.toString()
//                orderLinkId = order.id.toString()
                    )
                )
                logger.info { "$updatedOrder" }
                return updatedOrder
            }

            else -> {
                throw Exception("Tried to amend non-limit order type: ${order.update::class}")
            }
        }

    }

}

