
try:
    from flask.ext.sqlalchemy import get_debug_queries, SQLAlchemy
except ImportError:
    sqlalchemy_available = False
    get_debug_queries = SQLAlchemy = None
else:
    sqlalchemy_available = True

from collections import namedtuple, deque
from flask import request, current_app, abort, json_available, g
from flask_debugtoolbar import module
from flask_debugtoolbar.panels import DebugPanel
from flask_debugtoolbar.utils import format_fname, format_sql, is_rendering
import itertools
import itsdangerous
import uuid


_ = lambda x: x


def query_signer():
    return itsdangerous.URLSafeSerializer(current_app.config['SECRET_KEY'],
                                          salt='fdt-sql-query')


def dump_query(statement, params):
    if not params or not statement.lower().strip().startswith('select'):
        return None

    try:
        return query_signer().dumps([statement, params])
    except TypeError:
        return None


def load_query(data):
    try:
        statement, params = query_signer().loads(request.args['query'])
    except (itsdangerous.BadSignature, TypeError):
        abort(406)

    # Make sure it is a select statement
    if not statement.lower().strip().startswith('select'):
        abort(406)

    return statement, params


class SQLAlchemyDebugPanel(DebugPanel):
    """
    Panel that displays the time a response took in milliseconds.
    """
    name = 'SQLAlchemy'

    # save the context for the 5 most recent requests
    query_cache = deque(maxlen=5)

    @classmethod
    def get_cache_for_key(self, key):
        for cache_key, value in self.query_cache:
            if key == cache_key:
                return value
        raise KeyError(key)

    def __init__(self, *args, **kwargs):
        super(self.__class__, self).__init__(*args, **kwargs)
        self.key = str(uuid.uuid4())

    @property
    def has_content(self):
        if not json_available or not sqlalchemy_available:
            return True # will display an error message
        return bool(get_debug_queries())

    def process_request(self, request):
        pass

    def process_response(self, request, response):
        pass

    def nav_title(self):
        return _('SQLAlchemy')

    def nav_subtitle(self):
        if not json_available or not sqlalchemy_available:
            return 'Unavailable'

        if get_debug_queries:
            count = len(get_debug_queries())
            return "%d %s" % (count, "query" if count == 1 else "queries")

    def title(self):
        return _('SQLAlchemy queries')

    def url(self):
        return ''

    def content(self):
        if not json_available or not sqlalchemy_available:
            msg = ['Missing required libraries:', '<ul>']
            if not json_available:
                msg.append('<li>simplejson</li>')
            if not sqlalchemy_available:
                msg.append('<li>Flask-SQLAlchemy</li>')
            msg.append('</ul>')
            return '\n'.join(msg)

        queries = get_debug_queries()

        unique_queries = {}

        for i, query in enumerate(queries):

            try:
                query_data = unique_queries[query.statement]
            except KeyError:
                query_data = {
                  'query_id': len(unique_queries) + 1,
                  'total_duration': 0,
                  'executions': []
                }
                unique_queries[query.statement] = query_data

            formatted_stack = [ format_fname(level) for level in query.stacktrace ]

            query_exec = {
                'exec_nr': i + 1,
                'duration': query.duration,
                'parameters': query.parameters,
                'context_long': query.context,
                'stack': formatted_stack,
                'shortened_stack': [ level for level in formatted_stack if not level.startswith("<") ],
                'signed_query': dump_query(query.statement, query.parameters)
            }

            query_data['total_duration'] += query.duration
            query_data['sql'] = format_sql(query.statement, query.parameters)
            query_data['executions'].append(query_exec)

        # keep only the actual query info, indexed by our id
        data = dict((query_data['query_id'], query_data) for (statement, query_data) in unique_queries.iteritems())

        # store the info about this queries
        self.query_cache.append((self.key, data))

        # total rendering time
        sql_time_while_rendering = sum( execution['duration'] for execution in \
                                        itertools.chain(*[ query['executions'] for query in data.itervalues() ]) \
                                        if is_rendering(execution['stack']) )
        context = {
          'key':                      self.key,
          'queries':                  data.values(),
          'total_sql_time':           sum(q.duration for q in queries),
          'sql_time_while_rendering': sql_time_while_rendering,
          'total_executions_count':   sum(len(query['executions']) for query in data.itervalues())
        }

        return self.render('panels/sqlalchemy.html', context)

# Panel views

@module.route('/sqlalchemy/sql_select', methods=['GET', 'POST'])
def sql_select():
    statement, params = load_query(request.args['query'])
    engine = SQLAlchemy().get_engine(current_app)

    result = engine.execute(statement, params)
    return g.debug_toolbar.render('panels/sqlalchemy_select.html', {
        'result': result.fetchall(),
        'headers': result.keys(),
        'sql': format_sql(statement, params),
        'duration': float(request.args['duration'] or 0),
    })

@module.route('/sqlalchemy/sql_explain', methods=['GET', 'POST'])
def sql_explain():
    statement, params = load_query(request.args['query'])
    engine = SQLAlchemy().get_engine(current_app)

    if engine.driver == 'pysqlite':
        query = 'EXPLAIN QUERY PLAN %s' % statement
    else:
        query = 'EXPLAIN %s' % statement

    result = engine.execute(query, params)
    return g.debug_toolbar.render('panels/sqlalchemy_explain.html', {
        'result': result.fetchall(),
        'headers': result.keys(),
        'sql': format_sql(statement, params),
        'duration': float(request.args['duration'] or 0),
    })

@module.route('/sqlalchemy/sql_query_executions', methods=['GET', 'POST'])
def sql_query_executions():
    data = SQLAlchemyDebugPanel.get_cache_for_key(request.args['key'])
    return g.debug_toolbar.render('panels/sqlalchemy_query_executions.html', {
        'query_data': data[int(request.args['query_id'])],
    })
