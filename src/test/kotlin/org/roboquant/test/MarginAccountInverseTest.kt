package org.roboquant.test

import org.roboquant.brokers.Account
import org.roboquant.brokers.Broker
import org.roboquant.brokers.sim.AccountModel
import org.roboquant.brokers.sim.NoCostPricingEngine
import org.roboquant.brokers.sim.SimBroker
import org.roboquant.brokers.sim.execution.InternalAccount
import org.roboquant.bybit.MakerTakerFeeModel
import org.roboquant.bybit.MarginAccountInverse
import org.roboquant.common.*
import org.roboquant.common.Currency.Companion.BTC
import org.roboquant.feeds.Event
import org.roboquant.feeds.TradePrice
import org.roboquant.orders.MarketOrder
import java.time.Instant
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

internal class MarginAccountInverseTest {

    val bitcoinAsset = Asset(
        "BTCUSD",
        AssetType.CRYPTO,
        Currency.BTC,
        exchange = Exchange.CRYPTO,
        id = "linearOrInverse::InversePerpetual"
    )

    private val makerRate = 0.00018
    private val takerRate = 0.0004

    fun bitcoinInternalAccount(): InternalAccount {
        val account = InternalAccount(BTC)
        account.cash.deposit(0.1.BTC)
        return account
    }

    @Test
    fun test3() {
        val account = bitcoinInternalAccount()
        val cc = account.baseCurrency
        val uc = MarginAccountInverse(3.0, makerRate, takerRate)
        uc.updateAccount(account)
        assertTrue(account.buyingPower.value > account.cash[cc])
    }


    private fun update(broker: Broker, asset: Asset, price: Number, orderSize: Int = 0): Account {
        val orders = if (orderSize == 0) emptyList() else listOf(MarketOrder(asset, orderSize))
        val action = TradePrice(asset, price.toDouble())
        val event = Event(listOf(action), Instant.now())
        broker.place(orders, event.time)
        broker.sync(event)
        return broker.account
    }

    private fun getSimBroker(deposit: Amount, accountModel: AccountModel): SimBroker {
        val wallet = deposit.toWallet()
        return SimBroker(
            wallet,
            accountModel = accountModel,
            pricingEngine = NoCostPricingEngine(),
//            feeModel = NoFeeModel()
                    feeModel = MakerTakerFeeModel(0.00018, 0.0004),
        )
    }

    @Test
    fun testMarginAccountLong() {
        val initial = 0.1.BTC
        val broker = getSimBroker(initial, MarginAccountInverse(3.0, makerRate, takerRate))

        var account = update(broker, bitcoinAsset, 30_000, 1000)

        account.positions[0].marketValue // will calculate different if inverse

        assertEquals(0.26657333333333333.BTC, account.buyingPower)

        account = update(broker, bitcoinAsset, 31_000, -2000)

        account = update(broker, bitcoinAsset, 30_000, 1000)

//        account = update(broker, bitcoinAsset, 31_000, -1500)
        assertTrue(account.cashAmount > initial)

    }

    @Test
    fun testMarginAccountShort() {

        val initial = 0.1.BTC
        val broker = getSimBroker(initial, MarginAccountInverse(3.0, makerRate, takerRate))

        var account = update(broker, bitcoinAsset, 30_000, -1000)

        account.positions[0].marketValue // will calculate different if inverse

        assertEquals(0.2666.BTC, account.buyingPower)

        account = update(broker, bitcoinAsset, 29_000, 1000)

        assertTrue(account.cashAmount > initial)


    }

}
