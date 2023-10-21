import requests
import pandas as pd
from datetime import datetime

ANALYSIS_START_TIME = datetime(2023, 10, 21, 12, 0, 0).timestamp()
BLOCKFROST_BASE_URL = "https://cardano-mainnet.blockfrost.io/api/v0"
BLOCKFROST_PROJECT_ID = "<INSERT YOUR PROJECT ID HERE>"
SPECTRUM_ORDER_CONTRACT_ADDRESS = "addr1wynp362vmvr8jtc946d3a3utqgclfdl5y9d3kn849e359hsskr20n"
SPECTRUM_POOL_ADDRESSES = [
    "addr1x8nz307k3sr60gu0e47cmajssy4fmld7u493a4xztjrll0aj764lvrxdayh2ux30fl0ktuh27csgmpevdu89jlxppvrswgxsta",
    "addr1x94ec3t25egvhqy2n265xfhq882jxhkknurfe9ny4rl9k6dj764lvrxdayh2ux30fl0ktuh27csgmpevdu89jlxppvrst84slu"
]
MAX_PAGES_TO_FETCH = 100

def calculate_spectrum_batcher_distribution():
    page, min_tx_block_time, spectrum_tx_hashes = 1, ANALYSIS_START_TIME + 1, []
    while page <= MAX_PAGES_TO_FETCH and min_tx_block_time > ANALYSIS_START_TIME:
        spectrum_order_contract_txs = requests.get(
            url = f"{BLOCKFROST_BASE_URL}/addresses/{SPECTRUM_ORDER_CONTRACT_ADDRESS}/transactions?page={page}&order=desc",
            headers={"project_id": BLOCKFROST_PROJECT_ID}
        ).json()
        min_tx_block_time = min([x.get("block_time") for x in spectrum_order_contract_txs])
        spectrum_tx_hashes.extend([x.get("tx_hash") for x in spectrum_order_contract_txs if x.get("block_time") > ANALYSIS_START_TIME])
        page += 1

    print(f"Need to query detailed data for {len(spectrum_tx_hashes)} transactions.")
    k, batcher_stats = 0, []
    for tx_hash in spectrum_tx_hashes:
        tx_utxos = requests.get(
            url = f"{BLOCKFROST_BASE_URL}/txs/{tx_hash}/utxos",
            headers={"project_id": BLOCKFROST_PROJECT_ID}
        ).json()

        if any([tx_input.get("address") in SPECTRUM_POOL_ADDRESSES for tx_input in tx_utxos.get("inputs")]) and any([tx_input.get("address") == SPECTRUM_ORDER_CONTRACT_ADDRESS for tx_input in tx_utxos.get("inputs")]):
            possible_batcher_outputs = [utxo for utxo in tx_utxos.get("outputs") if len(utxo.get("amount")) == 1]
            if len(possible_batcher_outputs) > 0:
                assumed_batcher_output = possible_batcher_outputs[-1]
                batcher_stats.append({
                    "batcher_address": assumed_batcher_output.get("address"),
                    "fee_lovelace": int(assumed_batcher_output.get("amount")[0].get("quantity"))
                })
        
        k += 1
        if k % 100 == 0 :
            print(f"Detailed data processed for {k} transactions.")

    batcher_stats_df = pd.DataFrame(batcher_stats) \
        .groupby("batcher_address") \
        .agg({
            "fee_lovelace": ["count", "sum"]
        }) \
        .reset_index()
    
    batcher_stats_df.columns = ["batcher_address", "batched_txs_count", "accumulated_fees_lovelace"]
    batcher_stats_df = batcher_stats_df.sort_values(by=["batched_txs_count", "accumulated_fees_lovelace"], ascending=False)
    batcher_stats_df["batched_txs_pct"] = batcher_stats_df["batched_txs_count"] / batcher_stats_df["batched_txs_count"].sum()
    
    print(batcher_stats_df.to_string())

if __name__ == "__main__":
    calculate_spectrum_batcher_distribution()
