from web3 import Web3
from dotenv import load_dotenv
import os
import requests
from collections import defaultdict
import time

load_dotenv()
ALCHEMY_API_KEY = os.getenv("ALCHEMY_API_KEY")
ETHERSCAN_API_KEY = os.getenv("ETHERSCAN_API_KEY")

w3 = Web3(Web3.HTTPProvider(f'https://eth-mainnet.alchemyapi.io/v2/{ALCHEMY_API_KEY}'))

def get_token_decimals(contract):
    try:
        return contract.functions.decimals().call()
    except Exception as e:
        print(f"Failed to get token decimals for contract {contract_address}, error: {e}")
        return 18  # default to 18 if cannot fetch decimals

def send_request(url):
    while True:
        try:
            response = requests.get(url)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as err:
            if response.status_code == 429:  # status code for rate limit error
                print("Rate limit exceeded. Sleeping for a minute...")
                time.sleep(60)  # Sleep for a minute
            else:
                raise err  # Re-raise the exception if it's not rate limit error

volume = defaultdict(int)
contract_first_seen = {}
prevBlock = None
cache_abi = {}

while True:
    try:
        block = w3.eth.getBlock('latest')
        if prevBlock != block:
            for tx in block.transactions:
                transaction = w3.eth.getTransaction(tx)
                contract_address = transaction['to']
                value = transaction['value']

                # Fetch ABI only if it's not in the cache
                if contract_address not in cache_abi:
                    try:
                        response = send_request(f'https://api.etherscan.io/api?module=contract&action=getabi&address={contract_address}&apikey={ETHERSCAN_API_KEY}')
                        abi = response['result']
                        cache_abi[contract_address] = abi
                    except Exception as e:
                        print(f"Failed to fetch ABI for contract {contract_address}, error: {e}")
                        continue

                # Exclude non-token/NFT transactions
                contract = w3.eth.contract(address=contract_address, abi=cache_abi[contract_address])

                try:
                    # Decode function input to get function name
                    input_data = transaction['input']
                    try:
                        function_name, _ = contract.decode_function_input(input_data)
                    except Exception as e:
                        print(f"Failed to decode input data for contract {contract_address}, error: {e}")
                        function_name = None

                    if function_name not in ['transfer', 'transferFrom']:
                        continue
                except Exception as e:
                    print(f"Failed to decode input data for contract {contract_address}, error: {e}")
                    continue

                # Get token decimals
                decimals = get_token_decimals(contract)

                # Adjust the value accordingly
                value_adjusted = value / 10 ** decimals

                # Record first seen time if contract is new
                if contract_address not in contract_first_seen:
                    contract_first_seen[contract_address] = time.time()

                # Update value if contract was first seen less than 24 hours ago
                current_time = time.time()
                if current_time - contract_first_seen[contract_address] <= 24 * 60 * 60:
                    volume[contract_address] += value_adjusted

            # Delete data for contracts first seen more than 24 hours ago
            current_time = time.time()
            for contract in list(volume.keys()):
                if current_time - contract_first_seen[contract] > 24 * 60 * 60:
                    del volume[contract]

            prevBlock = block
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
