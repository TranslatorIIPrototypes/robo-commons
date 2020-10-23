import aiohttp
import logging

logger = logging.getLogger(name = __name__)

async def async_get_json(url, headers = {},  retry_opts={'retry_attempts':3}):
    """
        Gets json response from url asyncronously.
    """
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url, headers=headers) as response:
                if response.status != 200:
                    logger.error(f"Failed to get response from {url}. Status code {response.status}")
                    return {}
                return await response.json()
        except Exception as e:
            logger.error(f"Failed to get response from {url}. Failed with client opts {retry_opts}. Exception: {e}" )
            return {}

async def async_get_text(url,headers = {}):
    """
        Gets text response from url asyncronously
    """
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers= headers) as response:
            if response.status != 200:
                logger.error(f'Failed to get response from {url}, returned status : {response.status}')
                return ''
            return await response.text()

async def async_get_response(url, headers= {}, retry_opts={'retry_attempts':3}):
    """
    Returns the whole reponse object
    """
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers) as response:
            json = await response.json()
            text = await response.text()
            raw = await response.read()
            return {
                'headers' : response.headers,
                'json': json,
                'text': text,
                'raw': raw,
                'status': response.status
            }
