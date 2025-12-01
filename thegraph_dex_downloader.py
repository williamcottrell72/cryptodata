#!/usr/bin/env python3
"""
TheGraph DEX Data Downloader
============================

A configurable script for downloading decentralized exchange (DEX) trading data
from The Graph protocol. This example focuses on Uniswap V3, but the patterns
shown here can be adapted for other DEXes like SushiSwap and PancakeSwap.

The Graph Overview
------------------
The Graph is a decentralized indexing protocol that allows you to query blockchain
data using GraphQL. Instead of running your own indexing infrastructure, you can
query "subgraphs" - open APIs that index specific smart contract data.

Key concepts:
- Subgraph: An open API that indexes data from specific smart contracts
- GraphQL: Query language used to request exactly the data you need
- Entities: Data types defined in the subgraph schema (e.g., Pool, Swap, Token)

Uniswap V3 Subgraph Entities
----------------------------
Common entities you can query:
- Pool: Liquidity pools containing two tokens
- Swap: Individual swap transactions
- Token: ERC20 token metadata and statistics
- PoolDayData: Aggregated daily statistics per pool
- PoolHourData: Aggregated hourly statistics per pool

Usage
-----
    # Basic usage - download recent swaps
    python thegraph_dex_downloader.py

    # Download pool data
    python thegraph_dex_downloader.py --query-type pools --limit 50

    # Download swaps for a specific pool
    python thegraph_dex_downloader.py --query-type swaps --pool-id <pool_address>

Requirements
------------
    pip install requests

Author: Generated with Claude Code
"""

import argparse
import json
import os
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional

import requests


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


# =============================================================================
# GRAPH CLIENT
# =============================================================================

class TheGraphClient:
    """
    A client for querying The Graph Protocol subgraphs.

    This client handles:
    - Sending GraphQL queries to subgraph endpoints
    - Pagination for large result sets
    - Rate limiting to avoid overwhelming the API
    - Error handling and retries

    Attributes:
        config: SubgraphConfig containing the endpoint URL and metadata
        rate_limit_delay: Seconds to wait between paginated requests

    Example:
        >>> client = TheGraphClient(SUBGRAPH_CONFIGS["uniswap_v3_ethereum"])
        >>> pools = client.query("pools", {"first": 10, "skip": 0})
    """

    def __init__(self, config: SubgraphConfig, api_key: Optional[str] = None, rate_limit_delay: float = 0.5):
        """
        Initialize the Graph client.

        Args:
            config: Subgraph configuration with endpoint URL
            api_key: The Graph API key (from https://thegraph.com/studio/)
            rate_limit_delay: Delay between requests to respect rate limits
        """
        self.config = config
        self.api_key = api_key or os.environ.get("GRAPH_API_KEY")
        self.rate_limit_delay = rate_limit_delay

        if not self.api_key:
            raise ValueError(
                "API key required. Set GRAPH_API_KEY environment variable or pass api_key parameter.\n"
                "Get a free API key at https://thegraph.com/studio/"
            )

        # Replace {api_key} placeholder in URL
        self.url = config.url.format(api_key=self.api_key)

        self.session = requests.Session()
        self.session.headers.update({
            "Content-Type": "application/json",
            "Accept": "application/json",
        })

    def query(self, query: str, variables: dict[str, Any]) -> dict[str, Any]:
        """
        Execute a GraphQL query against the subgraph.

        This method sends a POST request to the subgraph endpoint with the
        GraphQL query and variables, then returns the parsed response.

        Args:
            query: GraphQL query string
            variables: Dictionary of variables to pass to the query

        Returns:
            Dictionary containing the query response data

        Raises:
            requests.RequestException: If the HTTP request fails
            ValueError: If the response contains GraphQL errors

        Example:
            >>> response = client.query(
            ...     QUERIES["pools"],
            ...     {"first": 10, "skip": 0, "orderBy": "volumeUSD", "orderDirection": "desc"}
            ... )
            >>> pools = response["pools"]
        """
        payload = {
            "query": query,
            "variables": variables,
        }

        response = self.session.post(self.url, json=payload)
        response.raise_for_status()

        result = response.json()

        # Check for GraphQL errors
        if "errors" in result:
            error_messages = [e.get("message", str(e)) for e in result["errors"]]
            raise ValueError(f"GraphQL errors: {'; '.join(error_messages)}")

        return result.get("data", {})

    def query_with_pagination(
        self,
        query_name: str,
        variables: dict[str, Any],
        entity_name: str,
        max_items: Optional[int] = None,
        page_size: int = 100,
    ) -> list[dict[str, Any]]:
        """
        Execute a paginated query to retrieve large datasets.

        The Graph limits queries to 1000 items per request (often 100 for
        complex queries). This method handles pagination automatically,
        making multiple requests and combining the results.

        Args:
            query_name: Key in QUERIES dict for the query template
            variables: Base variables for the query (first/skip will be set)
            entity_name: Name of the entity list in the response (e.g., "pools")
            max_items: Maximum total items to retrieve (None for all available)
            page_size: Number of items per page (max 1000, often 100 is safer)

        Returns:
            List of all retrieved entities

        Example:
            >>> all_swaps = client.query_with_pagination(
            ...     "swaps_all",
            ...     {"minAmountUSD": "1000"},
            ...     entity_name="swaps",
            ...     max_items=500
            ... )
        """
        all_items = []
        skip = 0
        queries = get_queries_for_dex_type(self.config.dex_type)
        query = queries[query_name]

        while True:
            # Determine how many items to request this page
            if max_items is not None:
                remaining = max_items - len(all_items)
                if remaining <= 0:
                    break
                current_page_size = min(page_size, remaining)
            else:
                current_page_size = page_size

            # Update pagination variables
            page_variables = {**variables, "first": current_page_size, "skip": skip}

            print(f"  Fetching items {skip} to {skip + current_page_size}...")

            try:
                data = self.query(query, page_variables)
                items = data.get(entity_name, [])

                if not items:
                    # No more items available
                    break

                all_items.extend(items)
                skip += len(items)

                # If we got fewer items than requested, we've reached the end
                if len(items) < current_page_size:
                    break

                # Rate limiting
                time.sleep(self.rate_limit_delay)

            except Exception as e:
                print(f"  Error during pagination: {e}")
                break

        return all_items


# =============================================================================
# DATA PROCESSING
# =============================================================================

def format_swap_v3(swap: dict[str, Any]) -> dict[str, Any]:
    """
    Format a raw V3 swap record into a more readable structure.

    This function transforms the raw GraphQL response into a cleaner format
    with human-readable timestamps and computed fields.

    Args:
        swap: Raw swap data from a V3 subgraph (Uniswap V3, SushiSwap V3, PancakeSwap V3)

    Returns:
        Formatted swap dictionary with additional computed fields
    """
    timestamp = int(swap.get("timestamp", 0))
    pool = swap.get("pool", {})

    return {
        "id": swap.get("id"),
        "tx_hash": swap.get("transaction", {}).get("id"),
        "block_number": swap.get("transaction", {}).get("blockNumber"),
        "timestamp": timestamp,
        "datetime": datetime.fromtimestamp(timestamp).isoformat() if timestamp else None,
        "pool_id": pool.get("id"),
        "pair": f"{pool.get('token0', {}).get('symbol')}/{pool.get('token1', {}).get('symbol')}",
        "amount0": float(swap.get("amount0", 0)),
        "amount1": float(swap.get("amount1", 0)),
        "amount_usd": float(swap.get("amountUSD", 0)),
        "sender": swap.get("sender"),
        "recipient": swap.get("recipient"),
    }


def format_swap_v2(swap: dict[str, Any]) -> dict[str, Any]:
    """
    Format a raw V2 swap record into a more readable structure.

    V2 swaps use "pair" instead of "pool" and have amount0In/amount0Out fields
    instead of signed amount0/amount1.

    Args:
        swap: Raw swap data from a V2 subgraph (SushiSwap V2, PancakeSwap V2)

    Returns:
        Formatted swap dictionary with additional computed fields
    """
    timestamp = int(swap.get("timestamp", 0))
    pair = swap.get("pair", {})

    # V2 uses separate in/out fields - compute net amounts
    amount0_in = float(swap.get("amount0In", 0))
    amount0_out = float(swap.get("amount0Out", 0))
    amount1_in = float(swap.get("amount1In", 0))
    amount1_out = float(swap.get("amount1Out", 0))

    return {
        "id": swap.get("id"),
        "tx_hash": swap.get("transaction", {}).get("id"),
        "block_number": swap.get("transaction", {}).get("blockNumber"),
        "timestamp": timestamp,
        "datetime": datetime.fromtimestamp(timestamp).isoformat() if timestamp else None,
        "pool_id": pair.get("id"),
        "pair": f"{pair.get('token0', {}).get('symbol')}/{pair.get('token1', {}).get('symbol')}",
        "amount0_in": amount0_in,
        "amount0_out": amount0_out,
        "amount1_in": amount1_in,
        "amount1_out": amount1_out,
        "amount0": amount0_in - amount0_out,  # Net amount for compatibility
        "amount1": amount1_in - amount1_out,
        "amount_usd": float(swap.get("amountUSD", 0)),
        "sender": swap.get("sender"),
        "recipient": swap.get("to"),  # V2 uses "to" instead of "recipient"
    }


def format_swap(swap: dict[str, Any], dex_type: str) -> dict[str, Any]:
    """
    Format a raw swap record based on DEX type.

    Args:
        swap: Raw swap data from the subgraph
        dex_type: The DEX type to determine formatting

    Returns:
        Formatted swap dictionary
    """
    if is_v2_dex(dex_type):
        return format_swap_v2(swap)
    return format_swap_v3(swap)


def format_pool_v3(pool: dict[str, Any]) -> dict[str, Any]:
    """
    Format a raw V3 pool record into a more readable structure.

    Args:
        pool: Raw pool data from a V3 subgraph

    Returns:
        Formatted pool dictionary
    """
    token0 = pool.get("token0", {})
    token1 = pool.get("token1", {})

    return {
        "id": pool.get("id"),
        "pair": f"{token0.get('symbol')}/{token1.get('symbol')}",
        "token0": {
            "address": token0.get("id"),
            "symbol": token0.get("symbol"),
            "name": token0.get("name"),
            "decimals": token0.get("decimals"),
        },
        "token1": {
            "address": token1.get("id"),
            "symbol": token1.get("symbol"),
            "name": token1.get("name"),
            "decimals": token1.get("decimals"),
        },
        "fee_tier": int(pool.get("feeTier", 0)) / 10000,  # Convert to percentage
        "token0_price": float(pool.get("token0Price", 0)),
        "token1_price": float(pool.get("token1Price", 0)),
        "volume_usd": float(pool.get("volumeUSD", 0)),
        "tvl_usd": float(pool.get("totalValueLockedUSD", 0)),
        "tx_count": int(pool.get("txCount", 0)),
    }


def format_pool_v2(pair: dict[str, Any]) -> dict[str, Any]:
    """
    Format a raw V2 pair record into a more readable structure.

    V2 uses "pairs" with reserves instead of "pools" with TVL.

    Args:
        pair: Raw pair data from a V2 subgraph

    Returns:
        Formatted pair dictionary (using "pool" naming for consistency)
    """
    token0 = pair.get("token0", {})
    token1 = pair.get("token1", {})

    return {
        "id": pair.get("id"),
        "pair": f"{token0.get('symbol')}/{token1.get('symbol')}",
        "token0": {
            "address": token0.get("id"),
            "symbol": token0.get("symbol"),
            "name": token0.get("name"),
            "decimals": token0.get("decimals"),
        },
        "token1": {
            "address": token1.get("id"),
            "symbol": token1.get("symbol"),
            "name": token1.get("name"),
            "decimals": token1.get("decimals"),
        },
        "fee_tier": 0.003,  # V2 uses fixed 0.3% fee
        "reserve0": float(pair.get("reserve0", 0)),
        "reserve1": float(pair.get("reserve1", 0)),
        "token0_price": float(pair.get("token0Price", 0)),
        "token1_price": float(pair.get("token1Price", 0)),
        "volume_usd": float(pair.get("volumeUSD", 0)),
        "tvl_usd": float(pair.get("reserveUSD", 0)),  # V2 uses reserveUSD
        "tx_count": int(pair.get("txCount", 0)),
    }


def format_pool(pool: dict[str, Any], dex_type: str) -> dict[str, Any]:
    """
    Format a raw pool/pair record based on DEX type.

    Args:
        pool: Raw pool/pair data from the subgraph
        dex_type: The DEX type to determine formatting

    Returns:
        Formatted pool dictionary
    """
    if is_v2_dex(dex_type):
        return format_pool_v2(pool)
    return format_pool_v3(pool)


# =============================================================================
# MAIN EXECUTION
# =============================================================================

def download_pools(
    client: TheGraphClient,
    limit: int = 100,
    order_by: str = "volumeUSD",
    order_direction: str = "desc",
) -> list[dict[str, Any]]:
    """
    Download liquidity pool data from the DEX.

    Works with both V3 (pools) and V2 (pairs) DEXes.

    Args:
        client: TheGraphClient instance
        limit: Maximum number of pools to retrieve
        order_by: Field to sort by (volumeUSD, reserveUSD for V2, txCount)
        order_direction: Sort direction (asc or desc)

    Returns:
        List of formatted pool dictionaries
    """
    dex_type = client.config.dex_type
    entity_name = "pairs" if is_v2_dex(dex_type) else "pools"

    print(f"\nDownloading top {limit} {entity_name} by {order_by}...")

    variables = {
        "orderBy": order_by,
        "orderDirection": order_direction,
    }

    pools = client.query_with_pagination(
        "pools",  # Query key is always "pools", but actual GraphQL entity differs
        variables,
        entity_name=entity_name,
        max_items=limit,
    )

    return [format_pool(p, dex_type) for p in pools]


def download_swaps(
    client: TheGraphClient,
    limit: int = 100,
    pool_id: Optional[str] = None,
    min_amount_usd: float = 0,
    start_time: Optional[int] = None,
    end_time: Optional[int] = None,
) -> list[dict[str, Any]]:
    """
    Download swap transaction data from the DEX.

    Works with both V3 and V2 DEXes.

    Args:
        client: TheGraphClient instance
        limit: Maximum number of swaps to retrieve
        pool_id: Optional pool/pair address to filter swaps
        min_amount_usd: Minimum swap amount in USD
        start_time: Optional start timestamp (Unix seconds) - inclusive
        end_time: Optional end timestamp (Unix seconds) - inclusive

    Returns:
        List of formatted swap dictionaries
    """
    dex_type = client.config.dex_type
    is_v2 = is_v2_dex(dex_type)

    # Default time range if not specified (The Graph doesn't like null values for >= filters)
    # Use timestamp 0 (1970-01-01) as default start, and far future as default end
    effective_start = start_time if start_time is not None else 0
    effective_end = end_time if end_time is not None else 9999999999  # Year 2286

    # Build time filter description for display
    time_filter = ""
    if start_time is not None or end_time is not None:
        if start_time is not None and end_time is not None:
            time_filter = f" from {datetime.fromtimestamp(start_time)} to {datetime.fromtimestamp(end_time)}"
        elif start_time is not None:
            time_filter = f" after {datetime.fromtimestamp(start_time)}"
        else:
            time_filter = f" before {datetime.fromtimestamp(end_time)}"

    if pool_id:
        print(f"\nDownloading {limit} swaps for {'pair' if is_v2 else 'pool'} {pool_id}{time_filter}...")
        query_name = "swaps"
        # V2 uses pairId, V3 uses poolId
        variables = {
            "pairId" if is_v2 else "poolId": pool_id,
            "minAmountUSD": str(min_amount_usd),
            "startTime": str(effective_start),
            "endTime": str(effective_end),
        }
    else:
        print(f"\nDownloading {limit} recent swaps (min ${min_amount_usd}){time_filter}...")
        query_name = "swaps_all"
        variables = {
            "minAmountUSD": str(min_amount_usd),
            "startTime": str(effective_start),
            "endTime": str(effective_end),
        }

    swaps = client.query_with_pagination(
        query_name,
        variables,
        entity_name="swaps",
        max_items=limit,
    )

    return [format_swap(s, dex_type) for s in swaps]


def download_tokens(
    client: TheGraphClient,
    limit: int = 100,
    order_by: str = "volumeUSD",
    order_direction: str = "desc",
) -> list[dict[str, Any]]:
    """
    Download token data from the DEX.

    Args:
        client: TheGraphClient instance
        limit: Maximum number of tokens to retrieve
        order_by: Field to sort by
        order_direction: Sort direction

    Returns:
        List of token dictionaries
    """
    print(f"\nDownloading top {limit} tokens by {order_by}...")

    variables = {
        "orderBy": order_by,
        "orderDirection": order_direction,
    }

    tokens = client.query_with_pagination(
        "tokens",
        variables,
        entity_name="tokens",
        max_items=limit,
    )

    return tokens


def parse_timestamp(time_str: Optional[str]) -> Optional[int]:
    """
    Parse a time string into a Unix timestamp.

    Accepts:
    - Unix timestamp (integer string): "1704067200"
    - ISO date: "2024-01-01"
    - ISO datetime: "2024-01-01T12:00:00"

    Args:
        time_str: Time string to parse, or None

    Returns:
        Unix timestamp as integer, or None if input is None
    """
    if time_str is None:
        return None

    # Try parsing as Unix timestamp first
    try:
        return int(time_str)
    except ValueError:
        pass

    # Try parsing as ISO format
    for fmt in ["%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"]:
        try:
            dt = datetime.strptime(time_str, fmt)
            return int(dt.timestamp())
        except ValueError:
            continue

    raise ValueError(
        f"Cannot parse time '{time_str}'. Use Unix timestamp, YYYY-MM-DD, or YYYY-MM-DDTHH:MM:SS"
    )


def get_data_directory(subgraph: str, query_type: str) -> str:
    """
    Get the data directory path for a given subgraph and query type.

    Creates the directory structure: data/<subgraph>/<query_type>/

    Args:
        subgraph: The subgraph name (e.g., "uniswap_v3_ethereum")
        query_type: The query type (e.g., "pools", "swaps", "tokens")

    Returns:
        Path to the data directory
    """
    # Get the script's directory to create data folder relative to it
    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(script_dir, "data", subgraph, query_type)

    # Create directory if it doesn't exist
    os.makedirs(data_dir, exist_ok=True)

    return data_dir


def save_to_json(data: list[dict], filepath: str) -> None:
    """
    Save data to a JSON file.

    Args:
        data: List of dictionaries to save
        filepath: Full path to output file
    """
    with open(filepath, "w") as f:
        json.dump(data, f, indent=2, default=str)
    print(f"Saved {len(data)} records to {filepath}")


def main():
    """
    Main entry point for the script.

    Parses command line arguments and executes the appropriate query.
    """
    parser = argparse.ArgumentParser(
        description="Download DEX trading data from The Graph",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Download top 50 pools by volume
  python thegraph_dex_downloader.py --query-type pools --limit 50

  # Download recent swaps with minimum $1000 value
  python thegraph_dex_downloader.py --query-type swaps --limit 100 --min-amount-usd 1000

  # Download swaps for a specific USDC/WETH pool
  python thegraph_dex_downloader.py --query-type swaps --pool-id 0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640

  # Use a different DEX
  python thegraph_dex_downloader.py --subgraph sushiswap_ethereum --query-type pools
        """,
    )

    parser.add_argument(
        "--subgraph",
        type=str,
        default="uniswap_v3_ethereum",
        choices=list(SUBGRAPH_CONFIGS.keys()),
        help="Subgraph to query (default: uniswap_v3_ethereum)",
    )

    parser.add_argument(
        "--query-type",
        type=str,
        default="swaps",
        choices=["pools", "swaps", "tokens"],
        help="Type of data to download (default: swaps)",
    )

    parser.add_argument(
        "--limit",
        type=int,
        default=100,
        help="Maximum number of records to download (default: 100)",
    )

    parser.add_argument(
        "--pool-id",
        type=str,
        default=None,
        help="Pool address to filter swaps (optional)",
    )

    parser.add_argument(
        "--min-amount-usd",
        type=float,
        default=0,
        help="Minimum swap amount in USD (default: 0)",
    )

    parser.add_argument(
        "--start-time",
        type=str,
        default=None,
        help="Start time for swaps query (ISO format: 2024-01-01 or 2024-01-01T12:00:00, or Unix timestamp)",
    )

    parser.add_argument(
        "--end-time",
        type=str,
        default=None,
        help="End time for swaps query (ISO format: 2024-01-01 or 2024-01-01T12:00:00, or Unix timestamp)",
    )

    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="Output JSON filename (default: auto-generated)",
    )

    parser.add_argument(
        "--list-subgraphs",
        action="store_true",
        help="List available subgraph configurations and exit",
    )

    parser.add_argument(
        "--api-key",
        type=str,
        default=None,
        help="The Graph API key (or set GRAPH_API_KEY env var)",
    )

    args = parser.parse_args()

    # Handle --list-subgraphs
    if args.list_subgraphs:
        print("\nAvailable Subgraph Configurations:")
        print("-" * 60)
        for key, config in SUBGRAPH_CONFIGS.items():
            print(f"\n  {key}")
            print(f"    Name: {config.name}")
            print(f"    Description: {config.description}")
            print(f"    URL: {config.url[:60]}...")
        print()
        return

    # Initialize client
    config = SUBGRAPH_CONFIGS[args.subgraph]
    print(f"\nConnecting to {config.name}...")

    try:
        client = TheGraphClient(config, api_key=args.api_key)
    except ValueError as e:
        print(f"\nError: {e}")
        return

    print(f"Endpoint: {client.url[:60]}...")

    # Parse time arguments
    try:
        start_time = parse_timestamp(args.start_time)
        end_time = parse_timestamp(args.end_time)
    except ValueError as e:
        print(f"\nError: {e}")
        return

    # Execute query based on type
    if args.query_type == "pools":
        data = download_pools(client, limit=args.limit)
    elif args.query_type == "swaps":
        data = download_swaps(
            client,
            limit=args.limit,
            pool_id=args.pool_id,
            min_amount_usd=args.min_amount_usd,
            start_time=start_time,
            end_time=end_time,
        )
    elif args.query_type == "tokens":
        data = download_tokens(client, limit=args.limit)
    else:
        print(f"Unknown query type: {args.query_type}")
        return

    # Print summary
    print(f"\nDownloaded {len(data)} {args.query_type}")

    if data:
        print("\nSample record:")
        print(json.dumps(data[0], indent=2, default=str))

    # Determine output path
    if args.output:
        # User specified a custom output path
        output_file = args.output
    else:
        # Use default directory structure: data/<subgraph>/<query_type>/data.json
        data_dir = get_data_directory(args.subgraph, args.query_type)
        output_file = os.path.join(data_dir, "data.json")

    save_to_json(data, output_file)

    print("\nDone!")


if __name__ == "__main__":
    main()
