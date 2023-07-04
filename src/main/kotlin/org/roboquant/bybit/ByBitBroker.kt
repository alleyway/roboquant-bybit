@file:Suppress("unused", "WildcardImport")

package org.roboquant.bybit

import bybit.sdk.rest.ByBitRestClient
import bybit.sdk.rest.account.WalletBalanceParams
import bybit.sdk.rest.order.*
import bybit.sdk.shared.AccountType
import bybit.sdk.shared.Category
import bybit.sdk.shared.OrderType
import bybit.sdk.shared.Side
import bybit.sdk.websocket.*
import com.github.ajalt.mordant.rendering.TextColors
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.async
import org.roboquant.brokers.Account
import org.roboquant.brokers.Broker
import org.roboquant.brokers.Position
import org.roboquant.brokers.getPosition
import org.roboquant.brokers.sim.execution.InternalAccount
import org.roboquant.common.*
import org.roboquant.common.Currency
import org.roboquant.feeds.Event
import org.roboquant.orders.*
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
    baseCurrency: Currency = Currency.USDT,
    configure: ByBitConfig.() -> Unit = {}
) : Broker {

    private val brokerType = BrokerType.ByBitBroker
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
        logger.info("Created ByBitBroker with client $client")

        val wsOptions = WSClientConfigurableOptions(
            ByBitEndpoint.Private,
            config.apiKey,
            config.secret,
            config.testnet
        )
        wsClient = ByBit.getWebSocketClient(wsOptions, this::handler)
        assetMap = ByBit.availableAssets(client)

        updateAccount()

        wsClient.connectBlocking()

        wsClient.subscribeBlocking(
            listOf(
                ByBitWebSocketSubscription(ByBitWebsocketTopic.PrivateTopic.Execution),
                ByBitWebSocketSubscription(ByBitWebsocketTopic.PrivateTopic.Order),
                ByBitWebSocketSubscription(ByBitWebsocketTopic.PrivateTopic.Wallet)
            )
        )

    }

    private fun genOrderLinkId(): String {
        return UUID.randomUUID().toString()
    }

    private fun handler(message: ByBitWebSocketMessage) {

        when (message) {

            is ByBitWebSocketMessage.StatusMessage -> {
//                message.op == "auth"
            }

            is ByBitWebSocketMessage.RawMessage -> {
                println(message.data)
            }

            is ByBitWebSocketMessage.PrivateTopicResponse.Order -> {

                for (order in message.data) {
                    val orderId = placedOrders[order.orderId] ?: continue
                    val state = _account.getOrder(orderId) ?: continue

                    when (order.orderStatus) {
                        bybit.sdk.shared.OrderStatus.Filled -> {
                            _account.updateOrder(state, Instant.now(), OrderStatus.COMPLETED)
                        }

                        bybit.sdk.shared.OrderStatus.PartiallyFilled -> {
//                            if (order.orderType == OrderType.Market) {
//                                println("order.cumExecValue: " + order.cumExecValue)
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

                for (coinItem in (message.data.filter { it.accountType == AccountType.SPOT }).first().coin) {
                    if (coinItem.coin == "BTC") {
                        assetMap.get("BTCUSDT")?.let {


                            val serverWallet = coinItem.walletBalance.toBigDecimal().setScale(8, RoundingMode.DOWN)
                            val positionSize =
                                account.positions.getPosition(it).size.toBigDecimal().setScale(8, RoundingMode.DOWN)


                            println(
                                "      Server Wallet:  ${serverWallet.toPlainString()}\n" +
                                        "      ByBitBrkr Pos:  ${positionSize.toPlainString()}\n" +
                                        "         Difference: ${TextColors.yellow((serverWallet - positionSize).toPlainString())}"
                            )

//                            _account.setPosition(
//                                Position(
//                                    it,
//                                    Size(
//                                        coinItem.walletBalance.toBigDecimal().setScale(8, RoundingMode.DOWN)
//                                    )
//                                )
//                            )
                        }
                    }
                }


            }

            is ByBitWebSocketMessage.PrivateTopicResponse.Execution -> {

                message.data.forEach {
                    val asset = assetMap.get(it.symbol)
                    if (asset == null) {
                        logger.warn("Received execution for unknown symbol: ${it.symbol}")
                        return
                    }
                    val position = account.positions.firstOrNull { it.asset == asset }

                    val execPrice = it.execPrice.toDouble()
                    val execQty = Size(it.execQty)

                    val type = if (it.isMaker) {
                        "Limit"
                    } else {
                        "Market"
                    }

                    printBroker(
                        brokerType,
                        " Executed ${type} ${it.side} " +
                                TextColors.cyan(execQty.toString()) +
                                " @ ${
                            TextColors.brightBlue(
                                execPrice.toString()
                            )
                        } " + TextColors.gray(it.execTime)
                    )

                    if (position == null) {
                        if (it.side == Side.Buy)
                            _account.setPosition(
                                Position(
                                    asset,
                                    size = execQty,
                                    avgPrice = execPrice,
                                    lastUpdate = Instant.ofEpochMilli(it.execTime.toLong())
                                )
                            )
                    } else {

                        val avgPrice = if (it.side == Side.Buy) {
                            (position.avgPrice.times(position.size.toDouble()) + execPrice.times(execQty.toDouble())) / (position.size.toDouble() + execQty.toDouble())
                        } else {
                            position.avgPrice
                        }

                        val newSize = if (it.side == Side.Buy) {
                            execQty + position.size
                        } else {
                            position.size - execQty
                        }

                        _account.setPosition(
                            Position(
                                asset,
                                size = newSize,
//                                new_average_price = (existing_average_price * existing_shares + added_price * added_shares) / (existing_shares + added_shares)
                                avgPrice = avgPrice,
                                lastUpdate = Instant.ofEpochMilli(it.execTime.toLong())
                            )
                        )
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

    private fun updateAccount() {

        // TODO: this is very specific to BTCUSDT..just wanted a way to have a position
        val walletBalanceResp = client.accountClient.getWalletBalanceBlocking(WalletBalanceParams(AccountType.SPOT))

        for (coinItem in walletBalanceResp.result.list.first().coin) {
            if (coinItem.coin == "BTC") {
                assetMap.get("BTCUSDT")?.let {
                    // or should I use equity??
                    _account.setPosition(
                        Position(
                            it,
                            Size(
                                coinItem.walletBalance.toBigDecimal().setScale(8, RoundingMode.DOWN)
                            )
                        )
                    )
                }
            }
        }

        // end specific to bitcoin

        val ordersOpenPaginated = client.orderClient.ordersOpenPaginated(
            OrdersOpenParams(
                Category.spot
            )
        )
        for (order in ordersOpenPaginated) {
            val orderId = placedOrders[order.orderId] ?: continue
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
                    placedOrders[updatedOrder.result.orderId] = order.id
                }

                else -> logger.warn {
                    "supports only cancellation, market and limit orders, received ${order::class} instead"
                }
            }

        }

        return account
    }


    /**
     * Cancel an order
     *
     * @param cancellation
     */
    private fun cancelOrder(cancellation: CancelOrder) {

        val orderLinkId = placedOrders.entries.find { it.value == cancellation.order.id }?.key

        val deferred = GlobalScope.async {

            val order = cancellation.order
            val params = CancelOrderParams(
                Category.spot,
                symbol = order.asset.symbol,
                orderLinkId = orderLinkId
            )
            val cancelOrderResponse = client.orderClient.cancelOrder(params)

            if (cancelOrderResponse.retMsg != "OK") {
                println(cancelOrderResponse.retMsg)
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
        val amount = order.size.absoluteValue.toBigDecimal().setScale(6, RoundingMode.DOWN).toPlainString()
        val price = order.limit.toBigDecimal().setScale(2, RoundingMode.DOWN)

        val deferred = GlobalScope.async {
            val newOrder = client.orderClient.placeOrder(
                PlaceOrderParams(
                    Category.spot,
                    symbol,
                    side = if (order.buy) {
                        Side.Buy
                    } else {
                        Side.Sell
                    },
                    OrderType.Limit,
                    amount,
                    price.toPlainString(),
                    orderLinkId = orderLinkId
                )
            )
            logger.info { "$newOrder" }
        }
    }

    /**
     * Place a market order for a currency pair
     *
     * @param symbol
     * @param order
     */
    private fun trade(orderLinkId: String, symbol: String, order: MarketOrder, price: Double?) {


        val amount = order.size.absoluteValue * price!!

        // ByBit peculiarity!! for SPOT ONLY!
        // Order quantity. For Spot Market Buy order, please note that qty should be quote currency amount

        val deferred = GlobalScope.async {

            val newOrder = client.orderClient.placeOrder(
                PlaceOrderParams(
                    Category.spot,
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
            logger.info { "$newOrder" }
        }
    }


    // only applies to derivatives, not SPOT!
    private fun amend(symbol: String, order: UpdateOrder): AmendOrderResponse {

        when (order.update) {
            is LimitOrder -> {

                val limitOrder = order.update as LimitOrder
                val orderLinkId = placedOrders.entries.find { it.value == order.order.id }?.key

                with(orderLinkId) {
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
            }

            else -> {
                throw Exception("Tried to amend non-limit order type: ${order.update::class}")
            }
        }

    }

}

