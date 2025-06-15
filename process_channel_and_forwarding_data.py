import json
from datetime import datetime, timezone
import requests # Required for making API calls to mempool.space
import time     # Required for time.sleep in get_tx_timestamp
from tqdm import tqdm # Required for the progress bar

# Global cache for transaction timestamps to avoid redundant API calls
tx_timestamp_cache = {}

def get_tx_timestamp(txid):
    """
    Fetches the confirmation timestamp for a given Bitcoin transaction ID from mempool.space API.
    Returns a datetime object in UTC, or None if the timestamp cannot be retrieved.
    """
    if txid in tx_timestamp_cache:
        return tx_timestamp_cache[txid]

    # mempool.space API endpoint for transaction details
    api_url = f"https://mempool.space/api/tx/{txid}"
    
    max_retries = 3
    for attempt in range(max_retries):
        try:
            response = requests.get(api_url, timeout=10) # 10 second timeout
            response.raise_for_status() # Raise an exception for HTTP errors (4xx or 5xx)
            tx_data = response.json()

            # The 'status' field contains 'block_time' if confirmed
            if tx_data and 'status' in tx_data and tx_data['status'].get('confirmed'):
                timestamp_unix = tx_data['status']['block_time']
                # Convert Unix timestamp to datetime object (UTC)
                dt_object = datetime.fromtimestamp(timestamp_unix, tz=timezone.utc)
                tx_timestamp_cache[txid] = dt_object
                return dt_object
            else:
                print(f"Warning: Transaction {txid} not confirmed or status missing (Attempt {attempt+1}/{max_retries}).")
                # If not confirmed, we don't have a timestamp, so no retries will help.
                return None # Just return None and don't retry.

        except requests.exceptions.RequestException as e:
            print(f"Error fetching timestamp for {txid} (Attempt {attempt+1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt) # Exponential backoff
            else:
                print(f"Failed to retrieve timestamp for {txid} after {max_retries} attempts.")
                return None # Return None after all retries fail
    return None # Should not be reached, but as a safeguard

def process_channel_and_forwarding_data(list_channels_file, fwding_history_file, output_file):
    """
    Processes channel and forwarding history data to generate a report on peer activity and fees.
    Now includes true channel age by querying a blockchain explorer API and local balance,
    plus new metrics "Fees/Days Sats" and "Swap Maturity".

    Args:
        list_channels_file (str): Path to the file containing channel data.
        fwding_history_file (str): Path to the file containing forwarding history data.
        output_file (str): Path to the output file for the generated report.
    """

    # Stores {peer_alias: {'total_channel_age_seconds': int, 'forwards': int, 'total_fees_msat': int, 'is_open': bool, 'local_balance': int, 'fees_per_day_sats': float, 'swap_maturity': str}}
    peer_data = {}

    # 1. Process listChannels to get peer channel age, local balance, and initial 'is_open' status
    try:
        with open(list_channels_file, 'r') as f:
            list_channels_response = json.load(f)

        if "channels" in list_channels_response:
            current_time_utc = datetime.now(timezone.utc) # Get current time in UTC
            
            # Wrap the channel iteration with tqdm for a progress bar (this one remains)
            for i, channel in enumerate(tqdm(list_channels_response["channels"], desc="Fetching Channel Timestamps")):
                peer_alias = channel.get("peer_alias")
                channel_point = channel.get("channel_point")
                
                # Ensure local_balance is an integer
                local_balance = 0
                try:
                    local_balance = int(channel.get("local_balance", 0))
                except (ValueError, TypeError):
                    print(f"Warning: Could not convert local_balance '{channel.get('local_balance')}' to int for channel_point: {channel.get('channel_point', 'N/A')}. Defaulting to 0.")
                    local_balance = 0

                channel_age_seconds = 0
                if channel_point:
                    txid = channel_point.split(':')[0]
                    # Call the actual get_tx_timestamp function
                    tx_timestamp = get_tx_timestamp(txid) 
                    
                    if tx_timestamp:
                        # Calculate age in seconds
                        age_delta = current_time_utc - tx_timestamp
                        channel_age_seconds = age_delta.total_seconds()
                    else:
                        print(f"Could not get timestamp for channel_point: {channel_point}")
                else:
                    print(f"Warning: Channel missing 'channel_point': {channel}")


                if peer_alias:
                    if peer_alias not in peer_data:
                        peer_data[peer_alias] = {
                            'total_channel_age_seconds': channel_age_seconds,
                            'forwards': 0,
                            'total_fees_msat': 0,
                            'is_open': True,
                            'local_balance': local_balance,
                            'fees_per_day_sats': 0.0,
                            'swap_maturity': 'null'
                        }
                    else:
                        # For peers with multiple channels, aggregate relevant data
                        # We use the maximum channel age if a peer has multiple channels
                        if channel_age_seconds > peer_data[peer_alias]['total_channel_age_seconds']:
                            peer_data[peer_alias]['total_channel_age_seconds'] = channel_age_seconds
                        peer_data[peer_alias]['is_open'] = True
                        # For local_balance, sum it up if a peer has multiple channels
                        peer_data[peer_alias]['local_balance'] += local_balance
                
                # To be polite to the API, add a small delay between requests for multiple channels
                # if i < len(list_channels_response["channels"]) - 1: # No delay after the last one
                #    time.sleep(0.1) # 100 ms delay - adjust as needed based on rate limits


        else:
            print(f"Warning: 'channels' key not found in {list_channels_file}. Ensure the file format is correct.")

    except FileNotFoundError:
        print(f"Error: {list_channels_file} not found.")
        return
    except json.JSONDecodeError:
        print(f"Error: Could not decode JSON from {list_channels_file}. Please ensure it's valid JSON.")
        return

    # 2. Process fwdingHistory to tally forwards and fees
    try:
        with open(fwding_history_file, 'r') as f:
            fwding_history_response = json.load(f)

        if "forwarding_events" in fwding_history_response:
            for event in fwding_history_response["forwarding_events"]:
                peer_alias_in = event.get("peer_alias_in")
                peer_alias_out = event.get("peer_alias_out")
                
                try:
                    fee_msat = int(event.get("fee_msat", 0))
                except (ValueError, TypeError):
                    fee_msat = 0

                if peer_alias_in:
                    if peer_alias_in not in peer_data:
                        peer_data[peer_alias_in] = {'total_channel_age_seconds': 0, 'forwards': 0, 'total_fees_msat': 0, 'is_open': False, 'local_balance': 0, 'fees_per_day_sats': 0.0, 'swap_maturity': 'null'}
                    peer_data[peer_alias_in]['forwards'] += 1
                    peer_data[peer_alias_in]['total_fees_msat'] += fee_msat

                if peer_alias_out and peer_alias_out != peer_alias_in:
                    if peer_alias_out not in peer_data:
                        peer_data[peer_alias_out] = {'total_channel_age_seconds': 0, 'forwards': 0, 'total_fees_msat': 0, 'is_open': False, 'local_balance': 0, 'fees_per_day_sats': 0.0, 'swap_maturity': 'null'}
                    peer_data[peer_alias_out]['forwards'] += 1
                    peer_data[peer_alias_out]['total_fees_msat'] += fee_msat

        else:
            print(f"Warning: 'forwarding_events' key not found in {fwding_history_file}. Ensure the file format is correct.")

    except FileNotFoundError:
        print(f"Error: {fwding_history_file} not found.")
        return
    except json.JSONDecodeError:
        print(f"Error: Could not decode JSON from {fwding_history_file}. Please ensure it's valid JSON.")
        return

    # ---
    ## Calculate Metrics and Prepare Output
    
    # Updated header to include the new columns and renamed "Fees/Days x Sats"
    output_lines = ["PeerAlias,LocalBalance,#Forwards,TotalFeesEarnt,Age(Days),Fees/Days,Fees/Days Sats,Open,Swap Maturity"]
    sorted_peers = []

    for alias, data in peer_data.items():
        # Defensive type conversion for local_balance before calculation
        peer_local_balance = int(data.get('local_balance', 0))
        
        # Convert total_channel_age_seconds to days
        channel_age_days = data['total_channel_age_seconds'] / (60 * 60 * 24)
        
        total_fees_earnt = data['total_fees_msat'] / 1000  # Convert msat to satoshis
        
        fees_per_day = 0
        if channel_age_days > 0:
            fees_per_day = total_fees_earnt / channel_age_days
        
        # Calculate new metric: Fees/Days Sats (Fees per day divided by local balance)
        fees_per_day_sats = 0
        if peer_local_balance > 0:
            fees_per_day_sats = fees_per_day / peer_local_balance

        sorted_peers.append({
            'alias': alias,
            'local_balance': peer_local_balance,
            'forwards': data['forwards'],
            'total_fees_earnt': total_fees_earnt,
            'age_days': channel_age_days,
            'fees_per_day': fees_per_day,
            'fees_per_day_sats': fees_per_day_sats,
            'is_open': "True" if data['is_open'] else "False",
            'swap_maturity': 'null'
        })

    # Sort peers by Fees/Days Sats in descending order (changed sorting key)
    sorted_peers.sort(key=lambda x: x['fees_per_day_sats'], reverse=True)

    for peer in sorted_peers:
        output_lines.append(
            f"{peer['alias']},{peer['local_balance']},{peer['forwards']},{peer['total_fees_earnt']:.4f},{peer['age_days']:.4f},{peer['fees_per_day']:.4f},{peer['fees_per_day_sats']:.10f},{peer['is_open']},{peer['swap_maturity']}"
        )

    # ---
    ## Write Report to File

    try:
        with open(output_file, 'w') as f:
            for line in output_lines:
                f.write(line + '\n')
        print(f"Report successfully generated and saved to {output_file}")
    except IOError:
        print(f"Error: Could not write to {output_file}.")


if __name__ == "__main__":
    list_channels_file = "listChannels"
    fwding_history_file = "fwdingHistory"
    output_file = "peer_activity_report.csv"

    process_channel_and_forwarding_data(list_channels_file, fwding_history_file, output_file)
