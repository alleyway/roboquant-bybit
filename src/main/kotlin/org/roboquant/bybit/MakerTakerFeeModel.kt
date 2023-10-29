package org.roboquant.bybit

import org.roboquant.brokers.Trade
import org.roboquant.brokers.sim.FeeModel
import org.roboquant.brokers.sim.execution.Execution
import org.roboquant.orders.LimitOrder
import org.roboquant.orders.MarketOrder
import java.time.Instant
import kotlin.math.absoluteValue

/**
 * The PercentageFeeModel defines a percentage of total value as the fee. For every trade it will calculate to the
 * total value and then use the [feePercentage] as the fee.
 *
 * @property makerFeePercentage fee as a percentage of total execution cost, 0.01 = 1%. Default is 0.0
 * @constructor Create a new percentage fee model
 */
class MakerTakerFeeModel (
    private val makerFeePercentage: Double = 0.0,
    private val takerFeePercentage: Double = 0.0,
) : FeeModel {

    override fun calculate(execution: Execution, time: Instant, trades: List<Trade>): Double {

        return when (execution.order) {
            is LimitOrder -> {
                execution.value.absoluteValue * makerFeePercentage
            }

            is MarketOrder -> {
                execution.value.absoluteValue * takerFeePercentage
            }

            else -> {
                throw Exception("Don't know how to calculate fees for this order: ${execution.order::class}")
            }
        }


    }

}
