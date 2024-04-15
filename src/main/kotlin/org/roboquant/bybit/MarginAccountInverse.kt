package org.roboquant.bybit

import com.github.ajalt.mordant.rendering.TextColors.*
import org.roboquant.brokers.sim.AccountModel
import org.roboquant.brokers.sim.execution.InternalAccount
import org.roboquant.common.Logging
import org.roboquant.common.Size
import org.roboquant.common.Wallet
import org.roboquant.common.percent
import org.roboquant.common.sumOf
import org.roboquant.orders.LimitOrder
import kotlin.math.ceil
import kotlin.math.floor

class MarginAccountInverse(
    private val leverage: Double,
    val makerRate: Double,
    val takerRate: Double,
    private val minimumMarginRate: Double = 0.5.percent
) : AccountModel {

    private val logger = Logging.getLogger(this::class)

    init {
        require(leverage in 0.0..120.0) { "leverage between 0.0 and 120.0" }
        require(minimumMarginRate in 0.0..1.0) { "minimumMarginRate between 0.0 and 1.0" }
    }


    private fun calculateOrdersMargin(ordersList: List<LimitOrder>, currentPositionSize: Size): Wallet {
        val wallet = Wallet()

        val isSellOrders = ordersList.isNotEmpty() && ordersList.first().sell

        val sortedOrders = if (isSellOrders)
            ordersList.sortedBy { it.limit }
        else
            ordersList.sortedByDescending { it.limit }

        var remaining =
            if (currentPositionSize.isPositive || isSellOrders)
                Size(0.0)
            else
                currentPositionSize.unaryMinus()

        sortedOrders.forEach {

            val adjustedSize = if (remaining.absoluteValue >= it.size.absoluteValue) {
                remaining -= it.size
                Size(0.0)
            } else {
                val thisSize = it.size - remaining
                remaining = Size(0.0)
                thisSize
            }

            val orderValue = it.asset.value(adjustedSize, it.limit).absoluteValue

            val initialMargin = orderValue / leverage
            val feeToOpenPosition = orderValue * takerRate

            // this bankruptcy price is for ISOLATED margin mode!
            val bankruptcyPrice = if (it.size.isPositive) {
                ceil(it.limit * leverage / (leverage + 1))
            } else {
                floor(it.limit * leverage / (leverage - 1))
            }

            val feeToClosePosition = it.asset.value(adjustedSize.absoluteValue, bankruptcyPrice) * takerRate
            wallet.deposit(initialMargin + feeToOpenPosition + feeToClosePosition)
        }
        return wallet
    }


    /**
     * @see [AccountModel.updateAccount]
     */
    override fun updateAccount(account: InternalAccount) {
        val time = account.lastUpdate
        val currency = account.baseCurrency
        val positions = account.portfolio.values

        val cash = account.cash
        logger.debug("account.cash = ${blue(cash.toString())} (~ByBit: walletBalance)")

        // Attempt to replicate ByBit's totalPositionIM in the wallet API
        val totalPositionIM = positions.sumOf {
            it.calcMarginUsage(leverage, takerRate)
        }.convert(currency, time)

//        logger.debug("totalPositionIM = ${brightWhite(totalPositionIM.toString())}")

        // https://www.bybit.com/en/help-center/article/Order-Cost-Inverse-Contract

        // https://www.bybit.com/en/help-center/article/Bankruptcy-Price-Inverse-Contract

        // Attempt to replicate ByBit's totalOrderIM in the wallet API, compare to logging output in ByBitBroker

        val currentPositionSize = positions.firstOrNull()?.size ?: Size(0.0)
        val orders = account.orders.toList() // should make a copy
        val buyOrders = orders
            .filter { it.order.type == "LIMIT" && (it.order as LimitOrder).buy }
            .map { (it.order as LimitOrder) }

        val sellOrders = orders
            .filter { it.order.type == "LIMIT" && (it.order as LimitOrder).sell }
            .map { (it.order as LimitOrder) }

        val buyOrderIM = calculateOrdersMargin(buyOrders, currentPositionSize).convert(currency, time)
        val sellOrderIM = calculateOrdersMargin(sellOrders, currentPositionSize).convert(currency, time)

        val totalOrderIM = if (buyOrderIM >= sellOrderIM) {
            buyOrderIM
        } else {
            sellOrderIM
        }

        logger.debug("totalOrderIM = ${yellow(totalOrderIM.convert(currency, time).toString())}")

        val cashRemaining = cash - totalOrderIM - totalPositionIM

        logger.debug("cashRemaining = ${green(cashRemaining.toString())} (~ByBit: availableToWithdraw)")

        val buyingPower = cashRemaining * leverage

        logger.debug { "buyingPower = $buyingPower" }


//        val excessMargin = account.cash + positions.marketValue

        // totalCost is just size * avgPrice


        // Currently, the position.margin is updated via websocket stream
        // in the simulator, it's being stuck at 0.0


        // ByBit ADA example
        // Qty      Value               EntryPrice      Liq Price       Position Margin
        // 4033     Qty/EntryPrice      0.5764          ???             Value/BrokerLeverage + fees to close

        // in roboquant "Value" above is position.totalCost

        // positions.first().totalCost.absoluteValue.value  = size * avgPrice (more or less)


        /*
            I could calculate position margin by simply
         */


        // val pl = position.totalCost.absoluteValue.value / (position.margin + account.cashAmount.absoluteValue.value) * position.size.sign


//        val longExposure = positions.long.exposure.convert(currency, time) * minimumMarginRate
//        excessMargin.withdraw(longExposure)
//
//        val shortExposure = positions.short.exposure.convert(currency, time) * minimumMarginRate
//        excessMargin.withdraw(shortExposure)

//      https://www.bybit.com/en-US/help-center/bybitHC_Article?id=360039261214&language=en_US
        //// comment out above the following sorta works  for live trading

//        if (cashRemaining.convert(currency, time) < 0) {
//            //logger.error { "oops, our cash remaining which determines buyingPower is negative!"}
//        }


        account.buyingPower = buyingPower.convert(currency, time)
    }

}
