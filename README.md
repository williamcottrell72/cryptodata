# cryptodata

Tools for downloading cryptocurrency and DeFi data.

## TheGraph DEX Downloader

A configurable Python script for downloading decentralized exchange (DEX) trading data from [The Graph](https://thegraph.com/) protocol.

### Supported DEXes

| DEX | Version | Chains |
|-----|---------|--------|
| **Uniswap** | V3 | Ethereum, Arbitrum, Polygon, Base, Optimism, Celo, Avalanche, BSC |
| **PancakeSwap** | V3 | BSC, Ethereum, Arbitrum, Polygon zkEVM, zkSync, Linea, Base |
| **PancakeSwap** | V2 | Ethereum, Arbitrum, Polygon zkEVM, zkSync, Linea, Base |

### Requirements

```bash
pip install requests
```

### Setup

1. Get a free API key from https://thegraph.com/studio/
2. Set the environment variable:
   ```bash
   export GRAPH_API_KEY="your-api-key-here"
   ```

### Usage

```bash
# List all available subgraphs
python thegraph_dex_downloader.py --list-subgraphs

# Download top 50 pools by volume (Uniswap V3 Ethereum by default)
python thegraph_dex_downloader.py --query-type pools --limit 50

# Download recent swaps (minimum $1000 value)
python thegraph_dex_downloader.py --query-type swaps --limit 100 --min-amount-usd 1000

# Download swaps for a specific pool
python thegraph_dex_downloader.py --query-type swaps --pool-id 0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640

# Download token data
python thegraph_dex_downloader.py --query-type tokens --limit 50

# Pass API key directly (alternative to env var)
python thegraph_dex_downloader.py --api-key YOUR_KEY --query-type pools
```

#### Querying Different DEXes

```bash
# Uniswap V3 on different chains
python thegraph_dex_downloader.py --subgraph uniswap_v3_arbitrum --query-type swaps
python thegraph_dex_downloader.py --subgraph uniswap_v3_polygon --query-type pools
python thegraph_dex_downloader.py --subgraph uniswap_v3_base --query-type tokens

# PancakeSwap V3
python thegraph_dex_downloader.py --subgraph pancakeswap_v3_bsc --query-type pools
python thegraph_dex_downloader.py --subgraph pancakeswap_v3_ethereum --query-type swaps

# PancakeSwap V2
python thegraph_dex_downloader.py --subgraph pancakeswap_v2_arbitrum --query-type pools
```

### Output

Data is saved to JSON files (`pools.json`, `swaps.json`, or `tokens.json` by default). Use `--output` to specify a custom filename.

### V3 vs V2 Schema Differences

The script automatically handles schema differences between V3 and V2 DEXes:

| Feature | V3 (Uniswap V3, PancakeSwap V3) | V2 (PancakeSwap V2) |
|---------|----------------------------------------------|-----------------------------------|
| Liquidity entity | `pools` | `pairs` |
| Fee structure | Variable fee tiers (0.01%, 0.05%, 0.3%, 1%) | Fixed 0.3% fee |
| TVL field | `totalValueLockedUSD` | `reserveUSD` |
| Swap amounts | Signed `amount0`, `amount1` | Separate `amount0In`, `amount0Out`, etc. |

### Available Subgraphs

Run `python thegraph_dex_downloader.py --list-subgraphs` to see all available subgraph configurations.
