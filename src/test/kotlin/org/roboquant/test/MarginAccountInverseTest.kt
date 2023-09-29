package org.roboquant.test

import org.roboquant.brokers.Account
import org.roboquant.brokers.Broker
import org.roboquant.brokers.Position
import org.roboquant.brokers.sim.AccountModel
import org.roboquant.brokers.sim.NoCostPricingEngine
import org.roboquant.brokers.sim.NoFeeModel
import org.roboquant.brokers.sim.SimBroker
import org.roboquant.brokers.sim.execution.InternalAccount
import org.roboquant.bybit.MarginAccountInverse
import org.roboquant.common.*
import org.roboquant.common.Currency.Companion.BTC
import org.roboquant.feeds.Event
import org.roboquant.feeds.TradePrice
import org.roboquant.orders.MarketOrder
import org.roboquant.orders.OrderStatus
import java.math.BigDecimal
import java.time.Instant
import kotlin.test.Test
import kotlin.test.assertEquals

internal class MarginAccountInverseTest {

    fun internalAccount(): InternalAccount {
        val asset1 = Asset.BITCOIN_INVERSE_PERP
        val account = InternalAccount(BTC)
        account.cash.deposit(0.1.USD)
        account.setPosition(Position(asset1, Size(100), 30_000.0))

        val order = MarketOrder(asset1, 100)
        // val state = MutableOrderState(order, OrderStatus.COMPLETED, Instant.now(), Instant.now())
        account.initializeOrders(listOf(order))
        account.updateOrder(order, Instant.now(), OrderStatus.COMPLETED)
        return account
    }

//    @Test
//    fun test3() {
//        val account = internalAccount()
//        val cc = account.baseCurrency
//        val uc = MarginAccountInverse(3.0)
//        uc.updateAccount(account)
//        assertTrue(account.buyingPower.value > account.cash[cc])
//    }
//
//    @Test
//    fun test4() {
//        val account = internalAccount()
//        val cc = account.baseCurrency
//        val uc = MarginAccountInverse(20.0)
//        uc.updateAccount(account)
//        assertTrue(account.buyingPower.value > account.cash[cc])
//    }

    private fun update(broker: Broker, asset: Asset, price: Number, orderSize: Int = 0): Account {
        val orders = if (orderSize == 0) emptyList() else listOf(MarketOrder(asset, orderSize))
        val action = TradePrice(asset, price.toDouble())
        val event = Event(listOf(action), Instant.now())
        broker.place(orders)
        broker.sync(event)
        return broker.account
    }

    private fun getSimBroker(deposit: Amount, accountModel: AccountModel): SimBroker {
        val wallet = deposit.toWallet()
        return SimBroker(
            wallet,
            accountModel = accountModel,
            pricingEngine = NoCostPricingEngine(),
            feeModel = NoFeeModel()
        )
    }

//    @Test
//    fun testMarginAccountLong() {
//        // Slide-2 example in code
//        val initial = 1_000_000.JPY
//        val broker = getSimBroker(initial, MarginAccountInverse(3.0))
//        val abc = Asset("ABC", currencyCode = "JPY")
//
//        var account = update(broker, abc, 1000)
//        assertEquals(2_000_000.JPY, account.buyingPower)
//
//        account = update(broker, abc, 1000, 500)
//        assertEquals(1_700_000.JPY, account.buyingPower)
//        assertEquals(initial, account.equityAmount)
//
//        account = update(broker, abc, 500)
//        assertEquals(1_350_000.JPY, account.buyingPower)
//
//        account = update(broker, abc, 500, 2000)
//        assertEquals(750_000.JPY, account.buyingPower)
//
//        account = update(broker, abc, 400)
//        assertEquals(400_000.JPY, account.buyingPower)
//
//        account = update(broker, abc, 400, -2500)
//        assertEquals(1_000_000.JPY, account.buyingPower)
//
//    }

    @Test
    fun testMarginAccountShort() {
        // The example on slide-3 in code
        val initial = 0.1.BTC
        val broker = getSimBroker(initial, MarginAccountInverse(3.0))
        val bitcoinAsset = Asset.BITCOIN_INVERSE_PERP

        val multipliedAmount = Amount(BTC, BigDecimal.valueOf(Amount(BTC, 0.1).value) * BigDecimal.valueOf(3.0))
        assertEquals(0.3.BTC, multipliedAmount)

//        assertEquals(0.3.BTC, broker.account.buyingPower)

//
//        var account = update(broker, bitcoinAsset, 20_000, 0)
//        assertEquals(0.3.BTC, account.buyingPower)
//        assertEquals(initial, account.equityAmount)
//
//        account = update(broker, bitcoinAsset, 300)
//        assertEquals(Amount(USD, 21_000), account.buyingPower)
//
//        account = update(broker, bitcoinAsset, 300, -50)
//        assertEquals(12_000.USD, account.buyingPower)
//
//        account = update(broker, bitcoinAsset, 300, 100)
//        assertEquals(30_000.USD, account.buyingPower)
//        assertEquals(15_000.USD.toWallet(), account.cash)
    }

}
