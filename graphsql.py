import sys
import logging
import configparser
import vertica_python as db


# This is a logging configuration. It is used to log the output of the program.
logging.basicConfig(filename='graphsql.log', filemode='w',
                    format='%(name)s - %(levelname)s - %(message)s')


# constants
OUTPUT_TABLE_NAME = 'cs44_graphsql_output'


# The Arguments class is used to store the arguments passed to the script.
class Arguments(object):

    def __init__(self):
        try:
            args = sys.argv[1].split(';')
            for arg in args:
                key, value = arg.split('=')
                setattr(self, key, value)
        except:
            print('Invalid arguments')
            print('Expected format: python3 graphsql.py "task=<diameter|clique>;table=<name>;source=<id>;destination=<id>;k=<max threshold>"')
            sys.exit(1)


# The DBConnectionManager class is a class that manages the connection to the database
class DBConnectionManager(object):

    _create_key = object()

    _conn = None
    _tunnel = None

    def __init__(self, create_key, connection_info, tunnelling_info=None):
        assert(create_key == DBConnectionManager._create_key), \
            "DBConnectionManager objects must be created using DBConnectionManager.get_connection"

        if(tunnelling_info):
            try:
                self.__create_ssh_tunnel(tunnelling_info)
                # if SSH tunneling is used, the local port is replaced with the tunneled port
                connection_info['port'] = self._tunnel.local_bind_port
            except Exception as e:
                logging.warning(
                    'Could not create SSH tunnel: {}. Continuing without tunnelling...'.format(e))

        self._conn = db.connect(**connection_info)

    @classmethod
    def get_connection(cls, config, use_tunnel=False):
        """
        Get a connection to the database

        :param cls: The class that is calling the method
        :param config: The configuration dictionary
        :param use_tunnel: If True, the connection will be made via SSH, defaults to False (optional)
        """
        db_connection_info = config['DATABASE']
        ssh_tunnelling_info = config['SSH'] if 'SSH' in config else None

        if cls._conn is None:
            cls = cls(cls._create_key, db_connection_info, ssh_tunnelling_info)

        return cls._conn

    def __create_ssh_tunnel(self, tunnelling_info):
        """
        Create an SSH tunnel to the remote database (use if running from outside the server)

        :param tunnelling_info: A dictionary containing the following keys:
        """
        from sshtunnel import SSHTunnelForwarder

        self._tunnel = SSHTunnelForwarder(
            (tunnelling_info['host'], 22),
            ssh_username=tunnelling_info['ssh_username'],
            ssh_password=tunnelling_info['ssh_password'],
            remote_bind_address=(
                tunnelling_info['localhost'], int(tunnelling_info['port']))
        )
        self._tunnel.start()


# The QueryBuilder class is used to build a query to be executed against the database
class QueryBuilder(object):
    _query = ''

    save_query = False

    def __init__(self, args, save=False):
        self._task = args.task
        self._table = args.table
        self._source = args.source
        self._destination = args.destination
        self._k = int(args.k)
        self.save_query = save or self.save_query

    def build(self, template):
        """
        The function takes a template and replaces all the variables with the values from the variable
        map

        :param template: The template query to be used
        :return: The query with the variables replaced with the values.
        """
        self._query = template
        variable_map = self.map_variables()

        for key, val in variable_map.items():
            self._query = self._query.replace(key, val)

        if self.save_query:
            self.save()

        return self._query

    def map_variables(self):
        """
        # The above function is a dictionary of key-value pairs.
        #
        # The key is the variable name to be found in the string.
        #
        :return: a string that is the query to be sent to the database.
        """
        return {
            # v1, v2, v3... vk
            '%V%': ', '.join(['v{}'.format(i) for i in range(1, self._k+1)]),
            '%SOURCE%': self._source,
            '%DESTINATION%': self._destination,
            # 0's for placeholders for sub-query fields
            '%0%': ', '.join(['0'] * (self._k - 2)),
            '%TABLE%': self._table,
            # solution.v1, solution.v2, solution.v3... solution.vk-2, solution.vk (skip vk-1)
            '%SOLUTION.V%': ', '.join(['solution.v{}'.format(i) for i in range(1, self._k-1)] + ['solution.v{}'.format(i) for i in range(self._k, self._k+1)]),
            # v1 < v2 AND v2 < v3 AND... vk-1 < vk
            '%V<V%': ' and '.join(['v{} < v{}'.format(i, i+1) for i in range(1, self._k)]),
            '%K%': str(self._k),
        }

    def get_text(self):
        return self._query

    def save(self):
        """
        Save the query to a file
        """
        try:
            with open('pygraph.sql', 'w') as f:
                f.write(self._query)
        except Exception as e:
            logging.error('Could not save query: {}'.format(e))


def read_config():
    """
    Reads the config file and returns a dictionary of dictionaries (refer to README)
    :return: A dictionary of dictionaries. The key is the name of the section, and the value is a
    dictionary of the connection parameters.
    """
    try:
        config = configparser.RawConfigParser(inline_comment_prefixes="#")
        config.read('connection.cfg')

        return {k: dict(v.items()) for k, v in dict(config.items()).items()}

    except Exception as e:
        print('Could not read config file: {}'.format(e))
        print('Please make sure connection.cfg is present in the same directory as this file. Please refer to the README for more details.')


if __name__ == '__main__':
    args = Arguments()

    assert(args.task == 'clique'), \
        'Task must be clique'

    config = read_config()

    assert('DATABASE' in config), \
        "DATABASE section missing in config. Please refer to README for details."

    with DBConnectionManager.get_connection(config) as conn:
        cur = conn.cursor()

        query_template = (
            f'DROP TABLE IF EXISTS {OUTPUT_TABLE_NAME};\n'
            f'CREATE TABLE {OUTPUT_TABLE_NAME} AS\n'
            'WITH RECURSIVE solution\n'
            '(%V%, t, d) AS\n'
            '(\n'
            '    SELECT %SOURCE%, %DESTINATION%, %0% ,%DESTINATION%, 1\n'
            '    FROM %TABLE%\n'
            '    UNION ALL\n'
            '    SELECT %SOLUTION.V%, solution.t, %TABLE%.%DESTINATION%, solution.d+1\n'
            '    FROM solution\n'
            '    JOIN %TABLE% ON %TABLE%.%SOURCE%=solution.t\n'
            '    WHERE d < %K%\n'
            ')\n'
            'SELECT %V%\n'
            'FROM solution\n'
            'WHERE v1 = t and %V<V% and d = %K%\n'
            'ORDER BY %V%;'
        )

        query = QueryBuilder(args, save=True).build(query_template)

        cur.execute(query)

        conn.commit()

        print(OUTPUT_TABLE_NAME)
