from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field



import hashlib

import json

import os

import re

import threading

from pathlib import Path

from urllib.parse import quote



import pandas as pd

from apscheduler.schedulers.background import BackgroundScheduler

from dotenv import load_dotenv

from hdbcli import dbapi

from gen_ai_hub.proxy.langchain.openai import ChatOpenAI

from destination import proxy_client



load_dotenv()

SCHEMA = os.getenv('HANA_SCHEMA', 'FB90EF0C35E94D95831E095D724458A6')

GRAPH_NAME = os.getenv('GRAPH_NAME', 'bpp_kg')

BASE_IRI = os.getenv('BASE_IRI', 'https://example.org/excel-kg/').rstrip('/') + '/'

EXCEL_DIR = Path(os.getenv('EXCEL_DIR', 'input_data'))

SYNC_MINUTES = int(os.getenv('SYNC_MINUTES', '5'))

BATCH_SIZE = int(os.getenv('BATCH_SIZE', '100'))

MAX_ROWS = int(os.getenv('MAX_ROWS_PER_SHEET', '100000'))

RDF_TYPE = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type'

RDFS_CLASS = 'http://www.w3.org/2000/01/rdf-schema#Class'

RDFS_LABEL = 'http://www.w3.org/2000/01/rdf-schema#label'



llm = ChatOpenAI(deployment_id=os.environ['DEPLOYMENT_ID'], proxy_client=proxy_client, temperature=0)

lock = threading.Lock()

last_digest = None

schema_description = ''
last_sync = None
last_triple_count = 0
last_sheet_count = 0
graph_initialized = os.getenv('GRAPH_ALREADY_EXISTS', 'false').lower() == 'true'





def connect_hana():

    return dbapi.connect(address=os.environ['HANA_ADDRESS'],

                         port=int(os.getenv('HANA_PORT', '443')),

                         user=os.environ['HANA_USER'],

                         password=os.environ['HANA_PASSWORD'],

                         currentSchema=SCHEMA,

                         sslValidateCertificate=os.getenv('HANA_SSL_VALIDATE', 'true').lower() == 'true',

                         autocommit=True)





def sparql_update(connection, query):

    cursor = connection.cursor()

    try:

        cursor.execute("CALL SPARQL_EXECUTE(?, 'Accept: text/plain', ?, ?)", (query, None, None))

    finally:

        cursor.close()





def iri(*parts):

    return '<' + BASE_IRI + '/'.join(quote(str(x), safe='') for x in parts) + '>'





def literal(value):

    # JSON string escaping is compatible with ordinary SPARQL quoted literals.

    return json.dumps(str(value), ensure_ascii=False)





def table_name(file, sheet):

    return f'{file.stem}__{sheet}'





def read_excel():

    files = sorted(p for p in EXCEL_DIR.glob('*.xlsx') if not p.name.startswith('~$'))

    if not files:

        raise RuntimeError(f'No .xlsx files in {EXCEL_DIR.resolve()}')

    digest = hashlib.sha256()

    tables = {}

    for file in files:

        digest.update(file.name.encode())

        digest.update(file.read_bytes())

        for sheet, frame in pd.read_excel(file, sheet_name=None, dtype=str, keep_default_na=False).items():

            if len(frame) > MAX_ROWS:

                raise ValueError(f'{file.name}/{sheet} exceeds MAX_ROWS_PER_SHEET')

            name = table_name(file, sheet)

            tables[name] = frame.fillna('')

    return digest.hexdigest(), tables





def build_triples(tables):

    lines = set()

    descriptions = []

    for table, df in tables.items():

        cls = iri('class', table)

        lines.add(f'{cls} <{RDF_TYPE}> <{RDFS_CLASS}> .')

        descriptions.append(f'Table {table}: columns {list(map(str, df.columns))}')

        for number, (_, record) in enumerate(df.iterrows(), 1):

            # Stable for unchanged row order; re-sync rebuilds after edits.

            subject = iri('row', table, number)

            lines.add(f'{subject} <{RDF_TYPE}> {cls} .')

            for col, value in record.items():

                value = str(value).strip()

                if value:

                    lines.add(f'{subject} {iri("property", table, col)} {literal(value)} .')

    # Relationships are added only when explicitly mapped, not guessed.

    mapping_file = Path(os.getenv('MAPPINGS_FILE', 'relationships.json'))

    if mapping_file.exists():

        mappings = json.loads(mapping_file.read_text(encoding='utf-8'))

        if isinstance(mappings, dict):

            mappings = mappings.get('relationships', [])

        for rel in mappings:

            left, right = rel['from_table'], rel['to_table']

            left_col, right_col = rel['from_column'], rel['to_column']

            predicate = rel.get('predicate', 'relatedTo')

            if left not in tables or right not in tables:

                raise ValueError(f'Mapping refers to unknown table: {left} or {right}')

            if left_col not in tables[left] or right_col not in tables[right]:

                raise ValueError(f'Mapping refers to unknown column: {left_col} or {right_col}')

            index = {}

            for n, value in enumerate(tables[right][right_col].astype(str), 1):

                if value.strip():

                    index.setdefault(value.strip(), []).append(n)

            for n, value in enumerate(tables[left][left_col].astype(str), 1):

                for target in index.get(value.strip(), []) if value.strip() else []:

                    lines.add(f'{iri("row", left, n)} {iri("relationship", predicate)} {iri("row", right, target)} .')

            descriptions.append(f'Relationship {left}.{left_col} -> {right}.{right_col} ({predicate})')

    return sorted(lines), '\n'.join(descriptions)





def sync_graph(force=False):

    global last_digest, schema_description, last_sync, last_triple_count, last_sheet_count, graph_initialized

    with lock:

        digest, tables = read_excel()

        if digest == last_digest and not force:

            print('Excel unchanged; graph is up to date.')

            return

        triples, description = build_triples(tables)

        if not triples:

            raise RuntimeError('No triples generated; refusing to replace graph.')

        connection = connect_hana()

        try:

            # A named RDF graph is identified by GRAPH_NAME, not by HANA_SCHEMA.
            # CREATE SILENT can require Replace Existing Graph permission when it already exists.
            # First-run mode creates a genuinely new graph; later syncs clear/reload it.
            # This simple refresh is not atomic; avoid concurrent external graph reads.
            if not graph_initialized:
                try:
                    sparql_update(connection, f'CREATE GRAPH <{GRAPH_NAME}>')
                except Exception as exc:
                    raise RuntimeError(
                        f'Cannot create new named RDF graph <{GRAPH_NAME}> as user '
                        f'{os.environ.get("HANA_USER", "unknown")}. '
                        'Verify that the graph name is unused and the user has RDF graph '
                        f'creation privileges. Changing HANA_SCHEMA alone is insufficient: {exc}'
                    ) from exc
                graph_initialized = True
            else:
                sparql_update(connection, f'CLEAR GRAPH <{GRAPH_NAME}>')

            for start in range(0, len(triples), BATCH_SIZE):

                chunk = '\n'.join(triples[start:start + BATCH_SIZE])

                sparql_update(connection, f'INSERT DATA {{ GRAPH <{GRAPH_NAME}> {{\n{chunk}\n}} }}')

            last_digest = digest
            last_triple_count = len(triples)
            last_sheet_count = len(tables)
            last_sync = __import__("datetime").datetime.now().isoformat()

            schema_description = description

            print(f'Graph updated: {GRAPH_NAME} | {len(triples)} triples | {len(tables)} sheets')

        finally:

            connection.close()





def execute_sparql(query):

    if not re.search(r'\bSELECT\s+\*\s+WHERE\b', query, re.I | re.S):

        raise ValueError('Only SELECT * WHERE queries are accepted.')

    forbidden = r'\b(INSERT|DELETE|DROP|CLEAR|CREATE|LOAD|MOVE|COPY|ADD|SERVICE|CONSTRUCT|DESCRIBE|ASK|UPDATE)\b'

    if re.search(forbidden, query, re.I):

        raise ValueError('Unsafe or unsupported SPARQL operation.')

    if f'GRAPH <{GRAPH_NAME}>' not in query:

        raise ValueError('Query must use the configured RDF graph.')

    connection = connect_hana()

    cursor = connection.cursor()

    try:

        # HANA SPARQL_TABLE is a SQL table function; escape quotes for SQL.

        escaped = query.replace("'", "''")

        cursor.execute(f"SELECT * FROM SPARQL_TABLE('{escaped}')")

        names = [col[0] for col in cursor.description]

        return [dict(zip(names, [str(v) if v is not None else None for v in row]))

                for row in cursor.fetchmany(100)]

    finally:

        cursor.close()

        connection.close()





# Real tool-calling agent: model chooses tools and can retry a failed query.
from langchain_core.messages import HumanMessage, SystemMessage, AIMessage, ToolMessage
from langchain_core.tools import tool
from langgraph.graph import StateGraph, MessagesState, START, END
from langgraph.prebuilt import ToolNode


@tool
def inspect_graph_schema() -> str:
    """Inspect available Excel tables, columns and mapped relationships in the RDF graph. Call before writing SPARQL."""
    if not schema_description:
        return "Graph not synchronized yet. Ask administrator to run POST /sync."
    return schema_description


@tool
def search_rdf_graph(sparql: str) -> str:
    """Execute a READ-ONLY SPARQL SELECT * WHERE query on the configured HANA RDF graph. Return matching rows or a query error for self-correction."""
    try:
        query = sparql.strip().replace('```sparql', '').replace('```', '').strip()
        if not re.search(r'\bLIMIT\s+\d+\b', query, re.I):
            query += '\nLIMIT 100'
        rows = execute_sparql(query)
        return json.dumps({'sparql': query, 'row_count': len(rows), 'rows': rows}, ensure_ascii=False)
    except Exception as exc:
        return json.dumps({'error': str(exc), 'sparql': sparql, 'instruction': 'Correct the SPARQL and call search_rdf_graph again.'})


TOOLS = [inspect_graph_schema, search_rdf_graph]
try:
    agent_model = llm.bind_tools(TOOLS)
except Exception as exc:
    raise RuntimeError('The selected SAP AI Core deployment must support tool calling.') from exc


def agent_node(state: MessagesState):
    system = SystemMessage(content=f"""You are a SAP HANA RDF Knowledge Graph agent, not a general chatbot.
Your job is to answer the user's question by independently choosing and calling tools.
1. Call inspect_graph_schema first to learn the real tables and column names.
2. Build a read-only SPARQL query and call search_rdf_graph.
3. If the tool returns an error, fix your query and retry (at most twice).
4. If no matching rows, do not invent facts. Explain no matching data was found.
5. Answer only from actual tool results, not prior knowledge.
Use ONLY SELECT * WHERE {{ GRAPH <{GRAPH_NAME}> {{ ... }} }} LIMIT 100.
Use SELECT * (not projected variables), and do not use UPDATE, DELETE or INSERT.
RDF class: <{BASE_IRI}class/TABLE>
RDF property: <{BASE_IRI}property/TABLE/COLUMN>
RDF relationship: <{BASE_IRI}relationship/NAME>
RDF property values are quoted string literals; column names are URL encoded in IRIs.
Do not claim an answer unless search_rdf_graph returned supporting rows.""")
    response = agent_model.invoke([system] + state['messages'])
    return {'messages': [response]}


def route_agent(state: MessagesState):
    last = state['messages'][-1]
    return 'tools' if isinstance(last, AIMessage) and last.tool_calls else END


workflow = StateGraph(MessagesState)
workflow.add_node('agent', agent_node)
workflow.add_node('tools', ToolNode(TOOLS))
workflow.add_edge(START, 'agent')
workflow.add_conditional_edges('agent', route_agent, {'tools': 'tools', END: END})
workflow.add_edge('tools', 'agent')
kg_agent = workflow.compile()


def answer_question(question):
    if not schema_description:
        raise RuntimeError('Graph not ready. Run POST /sync first.')
    state = kg_agent.invoke({'messages': [HumanMessage(content=question)]},
                            config={'recursion_limit': 12})
    tool_history = []
    last_sparql = None
    last_rows = []
    for message in state['messages']:
        if isinstance(message, AIMessage):
            for call in message.tool_calls:
                tool_history.append({'tool': call['name'], 'input': call['args']})
        elif isinstance(message, ToolMessage) and message.name == 'search_rdf_graph':
            try:
                result = json.loads(message.content)
                if 'rows' in result:
                    last_sparql = result['sparql']
                    last_rows = result['rows']
            except (TypeError, ValueError):
                pass
    if last_sparql is None:
        raise RuntimeError('Agent did not execute the RDF query tool. Check that your SAP AI Core model supports tool calling.')
    return {'graph': GRAPH_NAME, 'answer': str(state['messages'][-1].content),
            'sparql': last_sparql, 'results': last_rows,
            'tool_calls': tool_history, 'agent_type': 'LangGraph tool-calling agent'}


# FastAPI agent endpoints
class ChatRequest(BaseModel):
    question: str = Field(min_length=1)

@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        sync_graph()
    except Exception as exc:
        print('Initial sync failed:', exc)
    scheduler = BackgroundScheduler(timezone=os.getenv('TIMEZONE', 'Asia/Kolkata'))
    scheduler.add_job(scheduled_sync, 'interval', minutes=SYNC_MINUTES, max_instances=1)
    scheduler.start()
    print(f'Excel scheduler started: every {SYNC_MINUTES} minutes')
    try:
        yield
    finally:
        scheduler.shutdown(wait=False)

def scheduled_sync():
    try:
        sync_graph()
    except Exception as exc:
        print('Scheduled sync failed:', exc)

app = FastAPI(title='SAP HANA RDF Knowledge Graph Agent', lifespan=lifespan)

@app.get('/graph')
def graph_status():
    return {'graph': GRAPH_NAME, 'triples': last_triple_count,
            'sheets': last_sheet_count, 'last_sync': last_sync,
            'sync_minutes': SYNC_MINUTES, 'schema': SCHEMA, 'ready': bool(schema_description)}

@app.post('/sync')
def manual_sync():
    try:
        sync_graph(force=True)
        return graph_status()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))

@app.post('/chat')
def chat(request: ChatRequest):
    try:
        with lock:
            return answer_question(request.question)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))

if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host='127.0.0.1', port=8000)
