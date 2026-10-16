
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
GRAPH_NAME = os.getenv('GRAPH_NAME', 'urn:excel-kg:central')
BASE_IRI = os.getenv('BASE_IRI', 'https://example.org/excel-kg/').rstrip('/') + '/'
EXCEL_DIR = Path(os.getenv('EXCEL_DIR', 'input_data'))
SYNC_MINUTES = int(os.getenv('SYNC_MINUTES', '1'))
BATCH_SIZE = int(os.getenv('BATCH_SIZE', '100'))
MAX_ROWS = int(os.getenv('MAX_ROWS_PER_SHEET', '100000'))
RDF_TYPE = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type'
RDFS_CLASS = 'http://www.w3.org/2000/01/rdf-schema#Class'
RDFS_LABEL = 'http://www.w3.org/2000/01/rdf-schema#label'

llm = ChatOpenAI(deployment_id=os.environ['DEPLOYMENT_ID'], proxy_client=proxy_client, temperature=0)
lock = threading.Lock()
last_digest = None
schema_description = ''


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
    global last_digest, schema_description
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
            # Fixed graph name, visible in HANA; do not silently ignore permissions errors.
            # Note: this simple refresh is not atomic; do not use for concurrent production reads.
            try:
                sparql_update(connection, f'CREATE SILENT GRAPH <{GRAPH_NAME}>')
            except Exception as exc:
                raise RuntimeError(f'Cannot create RDF graph; check HANA SPARQL privileges: {exc}') from exc
            sparql_update(connection, f'CLEAR GRAPH <{GRAPH_NAME}>')
            for start in range(0, len(triples), BATCH_SIZE):
                chunk = '\n'.join(triples[start:start + BATCH_SIZE])
                sparql_update(connection, f'INSERT DATA {{ GRAPH <{GRAPH_NAME}> {{\n{chunk}\n}} }}')
            last_digest = digest
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


def answer_question(question):
    if not schema_description:
        raise RuntimeError('First synchronize the Excel files to create the graph.')
    prompt = f'''You generate SAP HANA RDF SPARQL queries. Return only SPARQL, no markdown.
Use this exact graph: <{GRAPH_NAME}>.
Only use SELECT * WHERE {{ GRAPH <{GRAPH_NAME}> {{ ... }} }} LIMIT 100.
Never use SELECT ?variable; use SELECT * (HANA compatibility).
Never use INSERT, DELETE, SERVICE or updates.
All row IRIs have the form <{BASE_IRI}row/TABLE/ROW_NUMBER>.
Property IRIs have the form <{BASE_IRI}property/TABLE/COLUMN>.
Class IRIs have the form <{BASE_IRI}class/TABLE>.
Relationship IRIs have the form <{BASE_IRI}relationship/NAME>.
Values are RDF string literals. Use actual table and column names from this schema:
{schema_description}
If uncertain, use ?s ?p ?o and return relevant triples.
Question: {question}'''
    query = str(llm.invoke(prompt).content).replace('```sparql', '').replace('```', '').strip()
    if 'LIMIT' not in query.upper():
        query += '\nLIMIT 100'
    rows = execute_sparql(query)
    if not rows:
        answer = 'No matching facts found in the RDF graph.'
    else:
        answer = llm.invoke('Answer using ONLY these SPARQL results. If insufficient, say so.\n'
                            + json.dumps(rows, ensure_ascii=False) + '\nQuestion: ' + question).content
    return {'graph': GRAPH_NAME, 'sparql': query, 'results': rows, 'answer': str(answer)}


def main():
    print('SAP HANA RDF Knowledge Graph Agent')
    print('Graph name:', GRAPH_NAME)
    try:
        sync_graph()
    except Exception as exc:
        print('Initial sync failed:', exc)
    scheduler = BackgroundScheduler(timezone=os.getenv('TIMEZONE', 'Asia/Kolkata'))
    scheduler.add_job(sync_graph, 'interval', minutes=SYNC_MINUTES, max_instances=1)
    scheduler.start()
    print(f'Automatic Excel check every {SYNC_MINUTES} minutes. Type exit to stop.')
    try:
        while True:
            question = input('\nAsk your question (or type sync / exit): ').strip()
            if question.lower() == 'exit':
                break
            if question.lower() == 'sync':
                sync_graph(force=True)
            elif question:
                try:
                    result = answer_question(question)
                    print('\nGenerated SPARQL:\n', result['sparql'])
                    print('\nAnswer:\n', result['answer'])
                    print('\nRetrieved rows:', len(result['results']))
                except Exception as exc:
                    print('Agent error:', exc)
    finally:
        scheduler.shutdown(wait=False)


if __name__ == '__main__':
    main()
