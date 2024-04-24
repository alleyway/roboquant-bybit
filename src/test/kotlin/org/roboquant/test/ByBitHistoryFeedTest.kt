package org.roboquant.test

import bybit.sdk.shared.Category
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertTrue
import org.roboquant.bybit.ByBitHistoryFeed
import org.roboquant.common.days
import org.roboquant.common.minus
import org.roboquant.feeds.PriceAction
import org.roboquant.feeds.filter
import java.time.Instant
import kotlin.test.Test

/**
 * Test ByBitHistoryFeed
 */
internal class ByBitHistoryFeedTest {

    @Test
    fun feedTest() {

        runBlocking {

            val now = Instant.now()
            val feed = ByBitHistoryFeed(
                configure = { testnet = true },
                start = now.minus(1.days).toEpochMilli(),
                end = now.toEpochMilli(),
                category = Category.spot,
                interval = "60"
            )
            delay(4000)

            /// Run it for a little
            val result = feed.filter<PriceAction>()
            feed.close()
            assertTrue(result.isNotEmpty(), "Did not receive any price action")
        }
    }

}
