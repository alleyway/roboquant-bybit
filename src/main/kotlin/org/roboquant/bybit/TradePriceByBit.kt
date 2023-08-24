package org.roboquant.bybit

import bybit.sdk.shared.TickDirection
import org.roboquant.common.Asset
import org.roboquant.feeds.PriceAction


data class TradePriceByBit(override val asset: Asset, val price: Double, override val volume: Double = Double.NaN, val tickDirection: TickDirection) : PriceAction {

    override fun getPrice(type: String): Double {
        return price
    }

}
