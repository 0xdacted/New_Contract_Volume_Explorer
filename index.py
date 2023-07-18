from web3 import Web3
from dotenv import load_dotenv
import os
import requests
import asyncio
from collections import defaultdict
from classes import CurrentToken, OldToken
import time
from sqlalchemy import create_engine
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

async def add_token(session, contract_address, first_seen, volume):
    new_token = CurrentToken(contract_address=contract_address,
                             first_seen=datetime.fromtimestamp(first_seen), volume=volume)
    session.add(new_token)
    await session.commit()


async def update_token(session, contract_address, volume):
    token = await session.query(CurrentToken).filter_by(contract_address=contract_address).first()
    token.volume += volume
    await session.commit()


async def consolidate_old_tokens(session):
    tokens = await session.query(CurrentToken).filter(CurrentToken.first_seen < datetime.now() - timedelta(hours=24)).all()
    for token in tokens:
        old_token = OldToken(contract_address=token.contract_address,
                             first_seen=token.first_seen, volume=token.volume)
        session.delete(token)
        session.add(old_token)
    await session.commit()


async def was_seen_before(session, contract_address):
    token = await session.query(OldToken).filter_by(contract_address=contract_address).first()
    return token is not None

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


async def main():
  global prevBlock
  await init_db()

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
                          response = send_request(
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
                          function_name, _ = contract.decode_function_input(
                              input_data)
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
  
                  # Interact with database
                  async with AsyncSession() as session:
                      if not await was_seen_before(session, contract_address):
                          contract_first_seen[contract_address] = time.time()
                          await add_token(session, contract_address, contract_first_seen[contract_address], value_adjusted)
                      else:
                          await update_token(session, contract_address, value_adjusted)
  
              # After processing all transactions in the block
              async with AsyncSession() as session:
                  await consolidate_old_tokens(session)
  
              prevBlock = block.number
          await asyncio.sleep(15)  # sleeps for 15 seconds
      except Exception as e:
          print(f"An unexpected error occurred: {e}")

          
asyncio.run(main())
