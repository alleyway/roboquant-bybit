package org.roboquant.bybit

import bybit.sdk.shared.OrderType
import bybit.sdk.shared.Side
import bybit.sdk.websocket.ByBitWebSocketMessage
import org.roboquant.brokers.Trade
import org.roboquant.common.Amount
import org.roboquant.common.Asset
import org.roboquant.common.Size
import org.roboquant.common.Wallet
import org.roboquant.common.sumOf
import java.time.Instant

class TradeExecution (
    val asset: Asset,
    val orderId: String,
    val execPrice: Double,
    val execSize: Size,
    val orderLinkId: String,
    val orderType: OrderType,
    val feeAmount: Amount,
    val leavesQty: String,
    val execInstant: Instant
) {
    companion object {

        // not using for now since seemingly we get everything from normal executions
        fun fromFilledOrder(asset: Asset, order: ByBitWebSocketMessage.OrderItem, trades: List<Trade>, ): TradeExecution {
            val sign = if (order.side == Side.Buy) 1 else -1

            val filledQty = Size(trades.map { it.size.toDouble() }.sum())

            val cumExecQty = Size(order.cumExecQty).times(sign)

            val execQty = cumExecQty - filledQty

            val filledFee = trades.sumOf { it.fee }

            val cumExecFeeAmount = Amount(asset.currency, order.cumExecFee.toDouble())

            val execFeeWallet = Wallet(cumExecFeeAmount)

            execFeeWallet.withdraw(filledFee)

            return TradeExecution(
                asset,
                order.orderId,
                order.price.toDouble(),
                execQty,
                order.orderLinkId,
                order.orderType,
                execFeeWallet.convert(asset.currency),
                order.leavesQty,
                Instant.ofEpochMilli(order.updatedTime.toLong())
            )
        }

        fun fromExecutionItem(asset: Asset, exec: ByBitWebSocketMessage.ExecutionItem): TradeExecution {
            val sign = if (exec.side == Side.Buy) 1 else -1

            return TradeExecution(
                asset,
                exec.orderId,
                exec.execPrice.toDouble(),
                Size(exec.execQty).times(sign),
                exec.orderLinkId,
                exec.orderType,
                Amount(asset.currency, exec.execFee.toDouble()),
                exec.leavesQty,
                Instant.ofEpochMilli(exec.execTime.toLong()))
        }
    }
}
