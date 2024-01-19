package org.roboquant.test

import bybit.sdk.shared.Category
import kotlinx.coroutines.launch
import org.junit.jupiter.api.assertDoesNotThrow
import org.roboquant.bybit.ByBitBroker
import org.roboquant.common.Asset
import org.roboquant.common.Currency
import org.roboquant.common.ParallelJobs
import org.roboquant.common.Size
import org.roboquant.orders.CancelOrder
import org.roboquant.orders.LimitOrder
import org.roboquant.orders.MarketOrder
import org.roboquant.orders.Order
import org.roboquant.orders.UpdateOrder
import kotlin.math.ceil
import kotlin.test.Ignore
import kotlin.test.Test
import kotlin.test.assertEquals

internal class ByBitBrokerTest {
    private val asset = Asset.BITCOIN_INVERSE_PERP

    @Test @Ignore
    fun rateLimitTest() {

        val nThreads = 10
        val loop = 100
        val jobs = ParallelJobs()
//        val iAccount = InternalAccount(Currency.USD)


        val broker = ByBitBroker(
            configure = {
                testnet = true
                // api/secret from env variables
            },
            category = Category.inverse,
            baseCurrency = Currency.BTC,
            //accountModel = MarginAccountInverse(brokerLeverage, 0.5.percent)
        )

        //Thread.sleep(3000)

        val dummyOrder = LimitOrder(
            asset,
            size = Size(100),
            limit = 10_000.0
        )
        val cancelOrder = CancelOrder(dummyOrder, tag = "CancelAll")

        broker.place(listOf(dummyOrder, cancelOrder))
        Thread.sleep(2000)

        val pos = broker.account.positions.firstOrNull { it.asset == asset }
        val orders = mutableListOf<Order>()

        pos?.let {
            orders.add(
                MarketOrder(
                    asset,
                    quantity = ceil(pos.size.unaryMinus().toDouble()).toInt(),
                ))
        }




        val origOrder = LimitOrder(
            asset,
            size = Size(100),
            limit = 10_000.0
        )

        orders.add(origOrder)

        broker.place(orders)

        Thread.sleep(4_000)
        val random = java.util.Random()


        repeat(nThreads) {
            jobs.add {
                launch {
                    assertDoesNotThrow {
                        repeat(loop) {
                            val amendOrder = LimitOrder(
                                asset, Size(random.nextInt(100,200)), 10_200.0
                            )
                            broker.place(listOf(UpdateOrder(origOrder, amendOrder)))
                            Thread.sleep(1)
                        }
                        Thread.sleep(1)
                        repeat(10) {
                            val amendOrder = LimitOrder(
                                asset, Size(random.nextInt(300,400)), 10_300.0
                            )
                            broker.place(listOf(UpdateOrder(origOrder, amendOrder)))
                        }
                    }
                }
            }
        }

        assertEquals(nThreads, jobs.size)
        jobs.joinAllBlocking()
        println("ALL JOINED!! Remaining sleep for 30 seconds")
        Thread.sleep(30_000)

    }

}
