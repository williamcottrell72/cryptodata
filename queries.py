"""
Query configurations and constants for The Graph DEX data downloader.

This module contains:
- SubgraphConfig: Dataclass for subgraph endpoint configuration
- SUBGRAPH_CONFIGS: Dictionary of available DEX subgraph configurations
- QUERIES_V3: GraphQL queries for V3 DEXes (Uniswap V3, PancakeSwap V3)
- QUERIES_V2: GraphQL queries for V2 DEXes (PancakeSwap V2)
- Helper functions for query selection
"""

from dataclasses import dataclass


# =============================================================================
# CONFIGURATION
# =============================================================================

@dataclass
class SubgraphConfig:
    """
    Configuration for a Graph Protocol subgraph endpoint.

    The Graph provides hosted service endpoints and decentralized network endpoints.
    The hosted service is being deprecated, so prefer decentralized endpoints.

    Attributes:
        name: Human-readable name of the DEX
        url: The GraphQL endpoint URL for the subgraph
        description: Brief description of what this subgraph indexes
        dex_type: The DEX type for query schema selection ("uniswap_v3", "sushiswap_v3", "pancakeswap_v3", "sushiswap_v2")

    Example:
        >>> config = SubgraphConfig(
        ...     name="Uniswap V3 Ethereum",
        ...     url="https://gateway.thegraph.com/api/[api-key]/subgraphs/id/...",
        ...     description="Uniswap V3 on Ethereum mainnet",
        ...     dex_type="uniswap_v3"
        ... )
    """
    name: str
    url: str
    description: str
    dex_type: str = "uniswap_v3"  # Default to Uniswap V3 schema


# Subgraph endpoints for various DEXes
# Note: The Graph's hosted service has been deprecated. These endpoints use
# the decentralized network which may require an API key for high volume usage.
#
# To get an API key:
# 1. Go to https://thegraph.com/studio/
# 2. Create an account and generate an API key
# 3. Set the GRAPH_API_KEY environment variable
#
# Without an API key, you can still use the free tier with rate limits.

# Base URL for The Graph decentralized network
GRAPH_BASE_URL = "https://gateway.thegraph.com/api/{api_key}/subgraphs/id/"

SUBGRAPH_CONFIGS = {
    # =========================================================================
    # UNISWAP V3
    # Schema: Standard Uniswap V3 subgraph schema
    # =========================================================================
    "uniswap_v3_ethereum": SubgraphConfig(
        name="Uniswap V3 Ethereum",
        url=GRAPH_BASE_URL + "5zvR82QoaXYFyDEKLZ9t6v9adgnptxYpKpSbxtgVENFV",
        description="Uniswap V3 protocol on Ethereum mainnet",
        dex_type="uniswap_v3"
    ),
    "uniswap_v3_arbitrum": SubgraphConfig(
        name="Uniswap V3 Arbitrum",
        url=GRAPH_BASE_URL + "FbCGRftH4a3yZugY7TnbYgPJVEv2LvMT6oF1fxPe9aJM",
        description="Uniswap V3 protocol on Arbitrum One",
        dex_type="uniswap_v3"
    ),
    "uniswap_v3_polygon": SubgraphConfig(
        name="Uniswap V3 Polygon",
        url=GRAPH_BASE_URL + "3hCPRGf4z88VC5rsBKU5AA9FBBq5nF3jbKJG7VZCbhjm",
        description="Uniswap V3 protocol on Polygon",
        dex_type="uniswap_v3"
    ),
    "uniswap_v3_base": SubgraphConfig(
        name="Uniswap V3 Base",
        url=GRAPH_BASE_URL + "43Hwfi3dJSoGpyas9VwNoDAv55yjgGrPpNSmbQZArzMG",
        description="Uniswap V3 protocol on Base",
        dex_type="uniswap_v3"
    ),
    "uniswap_v3_optimism": SubgraphConfig(
        name="Uniswap V3 Optimism",
        url=GRAPH_BASE_URL + "Cghf4LfVqPiFw6fp6Y5X5Ubc8UpmUhSfJL82zwiBFLaj",
        description="Uniswap V3 protocol on Optimism",
        dex_type="uniswap_v3"
    ),
    "uniswap_v3_celo": SubgraphConfig(
        name="Uniswap V3 Celo",
        url=GRAPH_BASE_URL + "ESdrTJ3twMwWVoQ1hUE2u7PugEHX3QkenudD6aXCkDQ4",
        description="Uniswap V3 protocol on Celo",
        dex_type="uniswap_v3"
    ),
    "uniswap_v3_avalanche": SubgraphConfig(
        name="Uniswap V3 Avalanche",
        url=GRAPH_BASE_URL + "GVH9h9KZ9CqheUEL93qMbq7QwgoBu32QXQDPR6bev4Eo",
        description="Uniswap V3 protocol on Avalanche",
        dex_type="uniswap_v3"
    ),
    "uniswap_v3_bsc": SubgraphConfig(
        name="Uniswap V3 BSC",
        url=GRAPH_BASE_URL + "F85MNzUGYqgSHSHRGgeVMNsdnW1KtZSVgFULumXRZTw2",
        description="Uniswap V3 protocol on BNB Smart Chain",
        dex_type="uniswap_v3"
    ),

    # =========================================================================
    # PANCAKESWAP V3
    # Schema: Similar to Uniswap V3 (forked)
    # Source: https://developer.pancakeswap.finance/apis/subgraph
    # =========================================================================
    "pancakeswap_v3_bsc": SubgraphConfig(
        name="PancakeSwap V3 BSC",
        url=GRAPH_BASE_URL + "Hv1GncLY5docZoGtXjo4kwbTvxm3MAhVZqBZE4sUT9eZ",
        description="PancakeSwap V3 on BNB Smart Chain",
        dex_type="pancakeswap_v3"
    ),
    "pancakeswap_v3_ethereum": SubgraphConfig(
        name="PancakeSwap V3 Ethereum",
        url=GRAPH_BASE_URL + "CJYGNhb7RvnhfBDjqpRnD3oxgyhibzc7fkAMa38YV3oS",
        description="PancakeSwap V3 on Ethereum mainnet",
        dex_type="pancakeswap_v3"
    ),
    "pancakeswap_v3_arbitrum": SubgraphConfig(
        name="PancakeSwap V3 Arbitrum",
        url=GRAPH_BASE_URL + "251MHFNN1rwjErXD2efWMpNS73SANZN8Ua192zw6iXve",
        description="PancakeSwap V3 on Arbitrum One",
        dex_type="pancakeswap_v3"
    ),
    "pancakeswap_v3_polygon_zkevm": SubgraphConfig(
        name="PancakeSwap V3 Polygon zkEVM",
        url=GRAPH_BASE_URL + "7HroSeAFxfJtYqpbgcfAnNSgkzzcZXZi6c75qLPheKzQ",
        description="PancakeSwap V3 on Polygon zkEVM",
        dex_type="pancakeswap_v3"
    ),
    "pancakeswap_v3_zksync": SubgraphConfig(
        name="PancakeSwap V3 zkSync",
        url=GRAPH_BASE_URL + "3dKr3tYxTuwiRLkU9vPj3MvZeUmeuGgWURbFC72ZBpYY",
        description="PancakeSwap V3 on zkSync Era",
        dex_type="pancakeswap_v3"
    ),
    "pancakeswap_v3_linea": SubgraphConfig(
        name="PancakeSwap V3 Linea",
        url=GRAPH_BASE_URL + "6gCTVX98K3A9Hf9zjvgEKwjz7rtD4C1V173RYEdbeMFX",
        description="PancakeSwap V3 on Linea",
        dex_type="pancakeswap_v3"
    ),
    "pancakeswap_v3_base": SubgraphConfig(
        name="PancakeSwap V3 Base",
        url=GRAPH_BASE_URL + "BHWNsedAHtmTCzXxCCDfhPmm6iN9rxUhoRHdHKyujic3",
        description="PancakeSwap V3 on Base",
        dex_type="pancakeswap_v3"
    ),

    # =========================================================================
    # PANCAKESWAP V2 (StableSwap)
    # Schema: Uses "pairs" similar to Uniswap V2/SushiSwap V2
    # =========================================================================
    "pancakeswap_v2_ethereum": SubgraphConfig(
        name="PancakeSwap V2 Ethereum",
        url=GRAPH_BASE_URL + "9opY17WnEPD4REcC43yHycQthSeUMQE26wyoeMjZTLEx",
        description="PancakeSwap V2 on Ethereum mainnet",
        dex_type="pancakeswap_v2"
    ),
    "pancakeswap_v2_arbitrum": SubgraphConfig(
        name="PancakeSwap V2 Arbitrum",
        url=GRAPH_BASE_URL + "EsL7geTRcA3LaLLM9EcMFzYbUgnvf8RixoEEGErrodB3",
        description="PancakeSwap V2 on Arbitrum One",
        dex_type="pancakeswap_v2"
    ),
    "pancakeswap_v2_polygon_zkevm": SubgraphConfig(
        name="PancakeSwap V2 Polygon zkEVM",
        url=GRAPH_BASE_URL + "37WmH5kBu6QQytRpMwLJMGPRbXvHgpuZsWqswW4Finc2",
        description="PancakeSwap V2 on Polygon zkEVM",
        dex_type="pancakeswap_v2"
    ),
    "pancakeswap_v2_zksync": SubgraphConfig(
        name="PancakeSwap V2 zkSync",
        url=GRAPH_BASE_URL + "6dU6WwEz22YacyzbTbSa3CECCmaD8G7oQ8aw6MYd5VKU",
        description="PancakeSwap V2 on zkSync Era",
        dex_type="pancakeswap_v2"
    ),
    "pancakeswap_v2_linea": SubgraphConfig(
        name="PancakeSwap V2 Linea",
        url=GRAPH_BASE_URL + "Eti2Z5zVEdARnuUzjCbv4qcimTLysAizsqH3s6cBfPjB",
        description="PancakeSwap V2 on Linea",
        dex_type="pancakeswap_v2"
    ),
    "pancakeswap_v2_base": SubgraphConfig(
        name="PancakeSwap V2 Base",
        url=GRAPH_BASE_URL + "2NjL7L4CmQaGJSacM43ofmH6ARf6gJoBeBaJtz9eWAQ9",
        description="PancakeSwap V2 on Base",
        dex_type="pancakeswap_v2"
    ),
}


# =============================================================================
# GRAPHQL QUERIES
# =============================================================================

# Query templates organized by DEX type
# V3 DEXes (Uniswap V3, SushiSwap V3, PancakeSwap V3) use "pools" and similar schemas
# V2 DEXes (SushiSwap V2, PancakeSwap V2) use "pairs" with different field names
#
# These use GraphQL syntax - learn more at https://graphql.org/learn/

# -----------------------------------------------------------------------------
# V3 QUERIES (Uniswap V3, SushiSwap V3, PancakeSwap V3)
# -----------------------------------------------------------------------------
QUERIES_V3 = {
    "pools": """
    query GetPools($first: Int!, $skip: Int!, $orderBy: String!, $orderDirection: String!) {
        pools(
            first: $first
            skip: $skip
            orderBy: $orderBy
            orderDirection: $orderDirection
        ) {
            id
            token0 {
                id
                symbol
                name
                decimals
            }
            token1 {
                id
                symbol
                name
                decimals
            }
            feeTier
            liquidity
            sqrtPrice
            token0Price
            token1Price
            volumeUSD
            txCount
            totalValueLockedUSD
            createdAtTimestamp
        }
    }
    """,

    "swaps": """
    query GetSwaps($first: Int!, $skip: Int!, $poolId: String, $minAmountUSD: String!, $startTime: BigInt, $endTime: BigInt) {
        swaps(
            first: $first
            skip: $skip
            where: {
                pool_: { id: $poolId }
                amountUSD_gte: $minAmountUSD
                timestamp_gte: $startTime
                timestamp_lte: $endTime
            }
            orderBy: timestamp
            orderDirection: desc
        ) {
            id
            transaction {
                id
                blockNumber
                timestamp
                gasUsed
                gasPrice
            }
            timestamp
            pool {
                id
                token0 {
                    symbol
                }
                token1 {
                    symbol
                }
            }
            sender
            recipient
            origin
            amount0
            amount1
            amountUSD
            sqrtPriceX96
            tick
            logIndex
        }
    }
    """,

    "swaps_all": """
    query GetSwapsAll($first: Int!, $skip: Int!, $minAmountUSD: String!, $startTime: BigInt, $endTime: BigInt) {
        swaps(
            first: $first
            skip: $skip
            where: {
                amountUSD_gte: $minAmountUSD
                timestamp_gte: $startTime
                timestamp_lte: $endTime
            }
            orderBy: timestamp
            orderDirection: desc
        ) {
            id
            transaction {
                id
                blockNumber
                timestamp
            }
            timestamp
            pool {
                id
                token0 {
                    symbol
                }
                token1 {
                    symbol
                }
            }
            sender
            recipient
            amount0
            amount1
            amountUSD
        }
    }
    """,

    "pool_day_data": """
    query GetPoolDayData($first: Int!, $skip: Int!, $poolId: String!) {
        poolDayDatas(
            first: $first
            skip: $skip
            where: { pool: $poolId }
            orderBy: date
            orderDirection: desc
        ) {
            id
            date
            pool {
                id
                token0 {
                    symbol
                }
                token1 {
                    symbol
                }
            }
            liquidity
            sqrtPrice
            token0Price
            token1Price
            volumeToken0
            volumeToken1
            volumeUSD
            feesUSD
            txCount
            open
            high
            low
            close
        }
    }
    """,

    "tokens": """
    query GetTokens($first: Int!, $skip: Int!, $orderBy: String!, $orderDirection: String!) {
        tokens(
            first: $first
            skip: $skip
            orderBy: $orderBy
            orderDirection: $orderDirection
        ) {
            id
            symbol
            name
            decimals
            totalSupply
            volume
            volumeUSD
            untrackedVolumeUSD
            feesUSD
            txCount
            poolCount
            totalValueLocked
            totalValueLockedUSD
            derivedETH
        }
    }
    """,
}

# -----------------------------------------------------------------------------
# V2 QUERIES (SushiSwap V2, PancakeSwap V2)
# These use "pairs" instead of "pools" and have different field names
# -----------------------------------------------------------------------------
QUERIES_V2 = {
    "pools": """
    query GetPairs($first: Int!, $skip: Int!, $orderBy: String!, $orderDirection: String!) {
        pairs(
            first: $first
            skip: $skip
            orderBy: $orderBy
            orderDirection: $orderDirection
        ) {
            id
            token0 {
                id
                symbol
                name
                decimals
            }
            token1 {
                id
                symbol
                name
                decimals
            }
            reserve0
            reserve1
            reserveUSD
            token0Price
            token1Price
            volumeUSD
            txCount
            createdAtTimestamp
        }
    }
    """,

    "swaps": """
    query GetSwaps($first: Int!, $skip: Int!, $pairId: String, $minAmountUSD: String!, $startTime: BigInt, $endTime: BigInt) {
        swaps(
            first: $first
            skip: $skip
            where: {
                pair_: { id: $pairId }
                amountUSD_gte: $minAmountUSD
                timestamp_gte: $startTime
                timestamp_lte: $endTime
            }
            orderBy: timestamp
            orderDirection: desc
        ) {
            id
            transaction {
                id
                blockNumber
                timestamp
            }
            timestamp
            pair {
                id
                token0 {
                    symbol
                }
                token1 {
                    symbol
                }
            }
            sender
            to
            amount0In
            amount1In
            amount0Out
            amount1Out
            amountUSD
            logIndex
        }
    }
    """,

    "swaps_all": """
    query GetSwapsAll($first: Int!, $skip: Int!, $minAmountUSD: String!, $startTime: BigInt, $endTime: BigInt) {
        swaps(
            first: $first
            skip: $skip
            where: {
                amountUSD_gte: $minAmountUSD
                timestamp_gte: $startTime
                timestamp_lte: $endTime
            }
            orderBy: timestamp
            orderDirection: desc
        ) {
            id
            transaction {
                id
                blockNumber
                timestamp
            }
            timestamp
            pair {
                id
                token0 {
                    symbol
                }
                token1 {
                    symbol
                }
            }
            sender
            to
            amount0In
            amount1In
            amount0Out
            amount1Out
            amountUSD
        }
    }
    """,

    "pair_day_data": """
    query GetPairDayData($first: Int!, $skip: Int!, $pairId: String!) {
        pairDayDatas(
            first: $first
            skip: $skip
            where: { pairAddress: $pairId }
            orderBy: date
            orderDirection: desc
        ) {
            id
            date
            pairAddress
            token0 {
                symbol
            }
            token1 {
                symbol
            }
            reserve0
            reserve1
            reserveUSD
            dailyVolumeToken0
            dailyVolumeToken1
            dailyVolumeUSD
            dailyTxns
        }
    }
    """,

    "tokens": """
    query GetTokens($first: Int!, $skip: Int!, $orderBy: String!, $orderDirection: String!) {
        tokens(
            first: $first
            skip: $skip
            orderBy: $orderBy
            orderDirection: $orderDirection
        ) {
            id
            symbol
            name
            decimals
            tradeVolume
            tradeVolumeUSD
            untrackedVolumeUSD
            txCount
            totalLiquidity
            derivedETH
        }
    }
    """,
}


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def get_queries_for_dex_type(dex_type: str) -> dict[str, str]:
    """
    Get the appropriate query set for a DEX type.

    Args:
        dex_type: The DEX type (e.g., "uniswap_v3", "sushiswap_v2")

    Returns:
        Dictionary of query templates for the DEX type
    """
    if dex_type.endswith("_v2"):
        return QUERIES_V2
    return QUERIES_V3


def is_v2_dex(dex_type: str) -> bool:
    """Check if the DEX type uses V2 schema (pairs instead of pools)."""
    return dex_type.endswith("_v2")


# Legacy alias for backwards compatibility
QUERIES = QUERIES_V3
