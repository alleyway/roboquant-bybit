package org.roboquant.bybit

import bybit.sdk.shared.TickDirection
import org.roboquant.common.Asset
import org.roboquant.feeds.TradePrice


data class TradePriceByBit(

    // standard stuff...
    override val asset: Asset,
    override val price: Double,
    override val volume: Double = Double.NaN,

    // bybit-specific
    val tickDirection: TickDirection

) : TradePrice(
    asset,
    price,
    volume
)
