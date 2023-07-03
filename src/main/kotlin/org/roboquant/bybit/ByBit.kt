package org.roboquant.bybit

import bybit.sdk.rest.ByBitRestClient
import bybit.sdk.rest.market.InstrumentsInfoParams
import bybit.sdk.rest.market.InstrumentsInfoResultItem
import bybit.sdk.shared.Category
import bybit.sdk.websocket.ByBitWebSocketClient
import bybit.sdk.websocket.ByBitWebSocketListener
import bybit.sdk.websocket.ByBitWebSocketMessage
import bybit.sdk.websocket.WSClientConfigurableOptions
import org.roboquant.common.*


/**
 * Configuration for ByBit connections
 *
 * @property apiKey the ByBit api key to use (property name is bybit.apikey)
 * @property secret the ByBit secret to use (property name is bybit.secret)
 */
data class ByBitConfig(
    var apiKey: String = Config.getProperty("bybit.apikey", ""),
    var secret: String = Config.getProperty("bybit.secret", ""),
    var testnet: Boolean = Config.getProperty("bybit.testnet", "true") == "true"
)

/**
 * Shared logic between feeds
 */
internal object ByBit {

    private val logger = Logging.getLogger(ByBit::class)


    internal fun getRestClient(config: ByBitConfig): ByBitRestClient {
        return ByBitRestClient(config.apiKey, config.secret, config.testnet,
//            httpClientProvider = okHttpClientProvider
        )
    }

    internal fun getWebSocketClient(
        options: WSClientConfigurableOptions,
        handler: (message: ByBitWebSocketMessage) -> Unit
    ): ByBitWebSocketClient {

        val websocketClient = ByBitWebSocketClient(
            options,
            object : ByBitWebSocketListener {

                override fun onAuthenticated(client: ByBitWebSocketClient) {
                    logger.trace("Authenticated")
                }

                override fun onReceive(client: ByBitWebSocketClient, message: ByBitWebSocketMessage) {
                    handler(message)
                }

                override fun onDisconnect(client: ByBitWebSocketClient) {
                    logger.trace("Disconnected")
                }

                override fun onError(client: ByBitWebSocketClient, error: Throwable) {
                    logger.error(error.toString())
                }

            })

        return websocketClient

    }

    private fun InstrumentsInfoResultItem.toAsset(): Asset? {

        val idPrefix = when(this) {
            is InstrumentsInfoResultItem.InstrumentsInfoResultItemSpot -> {
                "spot"
            }

            is InstrumentsInfoResultItem.InstrumentsInfoResultItemLinearInverse -> {
//                "linearInverse"
                this.contractType
            }

            is InstrumentsInfoResultItem.InstrumentsInfoResultItemOption -> {
                "option"
            }

            else -> {
                "unknown"
            }
        }

        return Asset(
            this.symbol,
            type = AssetType.CRYPTO,
            currency = Currency.USDT,
            Exchange.CRYPTO,
            id= "$idPrefix:${symbol}"
        )
    }

    internal fun availableAssets(client: ByBitRestClient): Map<String, Asset> {
        val assets = mutableListOf<Asset>()
        val params = InstrumentsInfoParams(
            category = Category.spot,
            limit = 1000,
        )

        client.marketClient.listSupportedInstruments(params).asSequence().forEach {
            val asset = it.toAsset()
            assets.addNotNull(asset)
        }

        return assets.associateBy { it.symbol }
    }

}
