package org.roboquant.bybit

import org.roboquant.brokers.sim.AccountModel
import org.roboquant.brokers.sim.execution.InternalAccount
import org.roboquant.brokers.totalCost
import org.roboquant.common.*

class MarginAccountInverse(
    private val leverage: Double, private val minimumMarginRate: Double = 0.5.percent
) : AccountModel {

    init {
        require(leverage in 0.0..120.0) { "leverage between 0.0 and 120.0" }
        require(minimumMarginRate in 0.0..1.0) { "minimumMarginRate between 0.0 and 1.0" }
    }

    /**
     * @see [AccountModel.updateAccount]
     */
    override fun updateAccount(account: InternalAccount) {
        val time = account.lastUpdate
        val currency = account.baseCurrency
        val positions = account.portfolio.values

        val excessMargin = account.cash + (positions.totalCost / leverage)

        // https://www.bybit.com/en-US/help-center/bybitHC_Article?id=360039261214&language=en_US

//        val longExposure = positions.long.totalCost.convert(currency, time) * minimumMarginRate
//        excessMargin.withdraw(longExposure)
//
//        val shortExposure = positions.short.totalCost.convert(currency, time) * minimumMarginRate
//        excessMargin.withdraw(shortExposure)

        val buyingPower = excessMargin.convert(currency, time) * leverage
        account.buyingPower = buyingPower
    }

}
