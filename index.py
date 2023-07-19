from web3 import Web3
from dotenv import load_dotenv
import aiohttp
import os
import requests
import asyncio
import ssl
from collections import defaultdict
from classes import CurrentToken, OldToken
import time
from sqlalchemy import create_engine, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from classes import Base, CurrentToken, OldToken
from datetime import datetime, timedelta

load_dotenv()
ALCHEMY_API_KEY = os.getenv("ALCHEMY_API_KEY")
ETHERSCAN_API_KEY = os.getenv("ETHERSCAN_API_KEY")
DB_URL = os.getenv("DB_URL")

engine = create_async_engine(DB_URL)

# Create all tables in the database which are defined by Base's subclasses

async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

AsyncSession = sessionmaker(
    engine, expire_on_commit=False, class_=AsyncSession)

w3 = Web3(Web3.HTTPProvider(
    f'https://eth-mainnet.alchemyapi.io/v2/{ALCHEMY_API_KEY}'))


async def create_symbol_id_mapping():
    response = await send_request('https://api.coingecko.com/api/v3/coins/list')
    symbol_id_map = {coin['symbol']: coin['id'] for coin in response}
    return symbol_id_map


async def add_token(session, contract_address, first_seen, volume):
    new_token = CurrentToken(contract_address=contract_address,
                             first_seen=datetime.fromtimestamp(first_seen), volume=volume)
    session.add(new_token)
    print(f'${new_token}')
    await session.commit()


async def update_token(session, contract_address, volume):
    stmt = select(CurrentToken).filter_by(contract_address=contract_address)
    result = await session.execute(stmt)
    token = result.scalars().first()
    token.volume += volume
    print(f'${token}')
    await session.commit()


async def try_add_token(session, contract_address, first_seen, volume):
    try:
        new_token = CurrentToken(contract_address=contract_address,
                                 first_seen=datetime.fromtimestamp(first_seen), volume=volume)
        session.add(new_token)
        await session.commit()
        print(f'Added new token: {new_token}')
    except IntegrityError:
        await session.rollback()  # important: reset the session
        await update_token(session, contract_address, volume)


async def consolidate_old_tokens(session):
    stmt = select(CurrentToken).filter(CurrentToken.first_seen <
                                       datetime.now() - timedelta(hours=24))
    result = await session.execute(stmt)
    tokens = result.scalars().all()
    for token in tokens:
        old_token = OldToken(contract_address=token.contract_address,
                             first_seen=token.first_seen, volume=token.volume)
        session.delete(token)
        session.add(old_token)
    await session.commit()


async def was_seen_before(session, contract_address):
    stmt_old_token = select(OldToken).filter_by(
        contract_address=contract_address)
    stmt_current_token = select(CurrentToken).filter_by(
        contract_address=contract_address)

    old_token_result = await session.execute(stmt_old_token)
    current_token_result = await session.execute(stmt_current_token)

    return old_token_result.scalars().first() is not None or current_token_result.scalars().first() is not None


async def get_token_usd_price(token_symbol, symbol_id_map):
    # use the API of your choice to get the USD price
    # ensure the token_symbol is url encoded
    token_id = symbol_id_map.get(token_symbol)
    if token_id is None:
        print(f"Token {token_symbol} is not found in the API response")
        return None  # return a default value or handle this case as needed
    token_id_encoded = requests.utils.quote(token_id)
    response = await send_request(f'https://api.coingecko.com/api/v3/simple/price?ids={token_id_encoded}&vs_currencies=usd')
    print(f"{response}")
    return response[token_id]['usd']


def get_token_decimals(contract):
    try:
        return contract.functions.decimals().call()
    except Exception as e:
        print(f"Failed to get token decimals for contract {contract_address}, error: {e}")
        return 18  # default to 18 if cannot fetch decimals


def get_token_symbol(contract):
    try:
        # The function call 'symbol' is a common interface for tokens
        return contract.functions.symbol().call().lower()
    except Exception as e:
        print(
            f"Could not fetch symbol for contract {contract.address}, error: {e}")
        return None


async def send_request(url):
    sslcontext = ssl.create_default_context()
    sslcontext.check_hostname = False
    sslcontext.verify_mode = ssl.CERT_NONE
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                async with session.get(url, ssl=sslcontext) as response:
                    if response.status == 429:
                        print("Rate limit exceeded. Sleeping for a minute...")
                        await asyncio.sleep(60)
                        continue
                    response.raise_for_status()
                    json_response = await response.json()
                    return json_response
            except aiohttp.ClientResponseError as err:
                print(f"HTTP request failed with error: {err}")
            except Exception as e:
                print(f"An unexpected error occurred: {e}")

volume = defaultdict(int)
contract_first_seen = {}
prevBlock = None
cache_abi = {}


async def main():
  global prevBlock
  await init_db()
  symbol_id_map = await create_symbol_id_mapping()

  while True:
      try:
          block = w3.eth.get_block('latest')
          if prevBlock != block.number:
              for tx in block.transactions:
                  transaction = w3.eth.get_transaction(tx)
                  contract_address = transaction['to']
                  value = transaction['value']
  
                  # Fetch ABI only if it's not in the cache
                  if contract_address not in cache_abi:
                      try:
                          response = await send_request(
                              f'https://api.etherscan.io/api?module=contract&action=getabi&address={contract_address}&apikey={ETHERSCAN_API_KEY}')
                          abi = response['result']
                          if abi == 'Contract source code not verified':
                              print(
                                  f"Cannot verify contract source code: {contract_address}")
                              cache_abi[contract_address] = 'unverified'
                              continue
                          cache_abi[contract_address] = abi
                      except Exception as e:
                          print(
                              f"Failed to fetch ABI for contract {contract_address}, error: {e}")
                          continue
                        
                  # Exclude non-token/NFT transactions
                  if cache_abi[contract_address] == 'unverified':
                      continue
                    
                  contract = w3.eth.contract(
                      address=contract_address, abi=cache_abi[contract_address])
  
                  try:
                      # Decode function input to get function name
                      input_data = transaction['input']
                      try:
                          function_call, _ = contract.decode_function_input(
                              input_data)
                          function_name = function_call.fn_name

                      except Exception as e:
                          print(
                              f"Failed to decode input data for contract {contract_address}, error: {e} Ignoring this transaction.")
                          function_name = None
  
                      if function_name not in ['transfer', 'transferFrom']:
                        print(
                            f"The function {function_name} for contract {contract_address} is not transfer or transferFrom. Ignoring this transaction.")
                        continue
                  except Exception as e:
                      print(
                          f"Failed to decode input data for contract {contract_address}, error: {e} Ignoring this transaction.")
                      continue
                    
                  # Get token decimals
                  decimals = get_token_decimals(contract)

                  # Adjust the value accordingly
                  value_adjusted = value / 10 ** decimals

                  # Get the token symbol or ticker
                  token_symbol = get_token_symbol(contract)

                  # Get the token's current USD price
                  usd_price = await get_token_usd_price(token_symbol, symbol_id_map)

                  # Calculate the transfer value in USD
                  usd_value = value_adjusted * usd_price
  
                  # Interact with database
                  async with AsyncSession() as session:
                      if not await was_seen_before(session, contract_address):
                          contract_first_seen[contract_address] = time.time()
                          await try_add_token(session, contract_address, contract_first_seen[contract_address], usd_value)
                      else:
                          await update_token(session, contract_address, usd_value)

              # After processing all transactions in the block
              async with AsyncSession() as session:
                  await consolidate_old_tokens(session)
  
              prevBlock = block.number
          await asyncio.sleep(15)  # sleeps for 15 seconds
      except Exception as e:
          print(f"An unexpected error occurred: {e}")

          
asyncio.run(main())
