package org.roboquant.test

import bybit.sdk.websocket.ByBitEndpoint
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertTrue
import org.roboquant.bybit.ByBitLiveFeed
import org.roboquant.common.*
import org.roboquant.feeds.PriceAction
import org.roboquant.feeds.filter
import kotlin.test.Test
import kotlin.test.assertEquals

internal class ByBitLiveFeedTest {

    @Test
    fun subscribeTest() {

        runBlocking {
            val feed = ByBitLiveFeed(endpoint = ByBitEndpoint.Inverse, configure = { testnet = false })
            delay(4000)
            feed.subscribeTrade("BTCUSD")
            feed.subscribeOrderBook("ETHUSD")

            assertEquals(2, feed.assets.size)

            /// Run it for a little
            val timeframe = Timeframe.next(30.seconds)
            val result = feed.filter<PriceAction>(timeframe = timeframe)
            feed.close()
            assertTrue(result.isNotEmpty(), "Did not receive any price action")
        }
    }

}
