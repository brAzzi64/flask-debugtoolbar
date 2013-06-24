
try:
    from flask.ext.sqlalchemy import get_debug_queries, SQLAlchemy
except ImportError:
    sqlalchemy_available = False
    get_debug_queries = SQLAlchemy = None
else:
    sqlalchemy_available = True

from flask import request, current_app, abort, json_available, g
from flask_debugtoolbar import module
from flask_debugtoolbar.panels import DebugPanel
from flask_debugtoolbar.utils import format_fname, format_sql, is_rendering
import itsdangerous


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
        data = []

        unique_queries = {}

        for query in queries:

            formatted_stack = [ format_fname(level) for level in query.stacktrace ]
            full_sql = query.statement % query.parameters
            current = {
                'query_nr': len(data) + 1,
                'duration': query.duration,
                'sql': format_sql(query.statement, query.parameters),
                'signed_query': dump_query(query.statement, query.parameters),
                'context_long': query.context,
                'stack': formatted_stack,
                'shortened_stack': [ level for level in formatted_stack if not level.startswith("<") ],
            }

            data.append(current)

            if full_sql not in unique_queries:
              unique_queries[full_sql] = []
            unique_queries[full_sql].append(current['query_nr'])

        repeated_queries = []
        avoidable_query_time = 0

        for uq in unique_queries:
          query_nrs = unique_queries[uq]
          if len(query_nrs) > 1:
            avg_duration = sum( data[query_nr - 1]['duration'] for query_nr in query_nrs ) / len( query_nrs )
            avoidable_query_time += avg_duration * ( len(query_nrs) - 1 )
            repeated_queries.append(query_nrs)

        context = {
          'queries':                  data,
          'total_sql_time':           sum(q.duration for q in queries),
          'sql_time_while_rendering': sum( q['duration'] for q in data if is_rendering( q['stack'] ) ),
          'repeated_queries':         repeated_queries,
          'avoidable_queries_count':  sum( len(query_nrs) - 1 for query_nrs in repeated_queries ),
          'avoidable_query_time':     avoidable_query_time,
          'queries_by_duration':      [ d['query_nr'] for d in sorted(data, key=lambda x: x['duration'], reverse=True) ]
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
        'duration': float(request.args['duration']),
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
        'duration': float(request.args['duration']),
    })
