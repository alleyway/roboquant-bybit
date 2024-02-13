package org.roboquant.bybit

import bybit.sdk.rest.ByBitRestClient
import bybit.sdk.rest.market.InstrumentsInfoParams
import bybit.sdk.rest.market.InstrumentsInfoResultItem
import bybit.sdk.shared.Category
import bybit.sdk.shared.ContractType
import bybit.sdk.websocket.ByBitWebSocketClient
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
        return ByBitRestClient(
            config.apiKey, config.secret, config.testnet,
//            httpClientProvider = okHttpClientProvider
        )
    }

    internal fun getWebSocketClient(
        options: WSClientConfigurableOptions
    ): ByBitWebSocketClient {
        return ByBitWebSocketClient(options)
    }

    private fun InstrumentsInfoResultItem.toAsset(): Asset {

        var currency = Currency.getInstance(this.quoteCoin)

        val id = when (this) {
            is InstrumentsInfoResultItem.InstrumentsInfoResultItemSpot -> {
                "spot::"
            }

            is InstrumentsInfoResultItem.InstrumentsInfoResultItemLinearInverse -> {

                when (this.contractType) {
                    ContractType.InversePerpetual, ContractType.InverseFutures -> {
                        currency = Currency.getInstance(this.baseCoin)
                    }

                    else -> {
                        // nothing
                    }
                }
                "linearOrInverse::${this.contractType}"
            }

            is InstrumentsInfoResultItem.InstrumentsInfoResultItemOption -> {
                "option::"
            }

            else -> {
                "unknown::"
            }
        }
        return Asset(
            this.symbol,
            type = AssetType.CRYPTO,
            currency,
            Exchange.CRYPTO,
            id = id
        )
    }

    internal fun availableAssets(client: ByBitRestClient, category: Category): Map<String, Asset> {
        val assets = mutableListOf<Asset>()
        val params = InstrumentsInfoParams(
            category,
            limit = 1000,
        )

        client.marketClient.listSupportedInstruments(params).asStream().forEach {
            val asset = it.toAsset()
            assets.addNotNull(asset)
        }

        return assets.associateBy { it.symbol }
    }

}
