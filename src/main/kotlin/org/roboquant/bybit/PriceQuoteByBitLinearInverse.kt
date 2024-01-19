package org.roboquant.bybit

import org.roboquant.common.*
import org.roboquant.feeds.PriceQuote

data class PriceQuoteByBitLinearInverse(

    // standard stuff...
    override val asset: Asset,
    override val askPrice: Double,
    override val askSize: Double,
    override val bidPrice: Double,
    override val bidSize: Double,

    // bybit-specific
    val markPrice: Double
) :
    PriceQuote(asset, askPrice, askSize, bidPrice, bidSize) {


}
