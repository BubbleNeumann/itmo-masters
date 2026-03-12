import aiohttp

_session_ids = {}


async def get_session_id(url: str) -> str:
    if url in _session_ids:
        return _session_ids[url]

    async with aiohttp.ClientSession() as session:
        async with session.options(url) as resp:
            session_id = resp.headers.get('mcp-session-id')
            if session_id:
                _session_ids[url] = session_id
                return session_id
            raise Exception("No session ID in OPTIONS response")


async def call_mcp(url: str, tool: str, args: dict, request_id: str):
    session_id = await get_session_id(url)
    payload = {
        'jsonrpc': '2.0',
        'method': 'tools/call',
        'params': {
            'name': tool,
            'arguments': args
        },
        'id': request_id
    }
    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'mcp-session-id': session_id,
        'X-Request-ID': request_id  # for idempotency
    }
    async with aiohttp.ClientSession() as session:
        async with session.post(url, json=payload, headers=headers) as resp:
            result = await resp.json()
            if 'error' in result:
                raise Exception(f"RPC error: {result['error']}")
            return result.get('result')


async def execute_action(action, request_id):
    tool = action['tool']
    args = action['args']

    if tool.startswith('kafka_'):
        url = "http://mcp-kafka-mcp-kafka:8000/mcp"
        tool_name = tool.replace('kafka_', '', 1)
    elif tool.startswith('spark_'):
        url = "http://mcp-spark-mcp-spark:8000/mcp"
        tool_name = tool.replace('spark_', '', 1)
    elif tool.startswith('clickhouse_'):
        url = "http://mcp-clickhouse-mcp-clickhouse:8000/mcp"
        tool_name = tool.replace('clickhouse_', '', 1)
    else:
        raise ValueError(f"Unknown tool: {tool}")

    print(f"call {tool_name} on {url} with args {args}", flush=True)
    return await call_mcp(url, tool_name, args, request_id)