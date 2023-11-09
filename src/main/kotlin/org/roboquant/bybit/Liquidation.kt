package org.roboquant.bybit

import bybit.sdk.shared.Side
import org.roboquant.common.*
import org.roboquant.feeds.Action

data class Liquidation(val asset: Asset, val price: Double, val side: Side, val volume: Double) : Action
