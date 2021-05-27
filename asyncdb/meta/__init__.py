import asyncio
from asyncdb.exceptions import ProviderError, NoDataFound, StatementError
from asyncdb.utils import SafeDict


class asyncORM(object):
    """
    asyncORM
       Class for basic record interaction on AsyncDB
    """

    _fields = {}
    _columns = []
    _result = None
    _connection = None
    _type = "new"
    _table = None
    _query = None
    _loop = None
    _val = {}

    def __init__(self, db, result=None, type="new", loop=None):
        self._columns = []
        self._fields = {}
        self._result = result
        if result:
            for row in result:
                self._columns.append(row["column_name"])
                self._fields[row["column_name"]] = row["data_type"]
        self._type = type
        self._val = {}
        self._table = None
        self._query = None
        if loop:
            self._loop = loop
        else:
            self._loop = asyncio.get_event_loop()
        self._connection = db

    def __del__(self):
        """
        Del: try to cleanup the database connector.

        """
        self.close()

    async def terminate(self):
        """
        Close

        Explicit closing a database connection
        """
        await self._connection.close(timeout=5)

    def close(self):
        """
        Close

        Explicit closing a database connection
        """
        if self._connection:
            try:
                asyncio.get_running_loop().run_until_complete(
                    self._connection.close(timeout=5)
                )
            except Exception as err:
                pass
            # self._loop.run_until_complete()

    """
    Meta Definitions
    """

    def get_table(self):
        return self._table

    def table(self, table, loop=None):
        """
        Set a Table definition from DB
          TODO: check for table exists
        """
        if len(table) == 0:
            raise ValueError("Table Name is missing")

        self._table = table
        if loop:
            self._loop = loop
        try:
            self._query = self._connection.table(table)
            return self
        except Exception as e:
            print(e)
            return False

    def fields(self, fields=[]):
        """
        fields
        todo: detect when field is missing
        """
        if type(fields) == str:
            self._fields = [x.strip() for x in fields.split(",")]
        elif type(fields) == list:
            self._fields = fields
        self._query = self._connection.fields(sentence=self._query, fields=fields)
        return self

    def filter(self, filter={}, **kwargs):
        """
        filter
           filter is an alias for WHERE
        """
        where = {}
        if filter:
            self._query = self._connection.where(sentence=self._query, where=filter)
        if len(kwargs) > 0:
            where.update(kwargs)
            # print("HERE %s" % where)
            self._query = self._connection.where(sentence=self._query, where=where)
        return self

    def orderby(self, order):
        """
        orderby
           define Ordering criteria
        """
        if order:
            self._query = self._connection.orderby(sentence=self._query, ordering=order)
        return self

    def all(self):
        """
        all
           Get data from query
        """
        print("Connection Status: %s" % self._connection.connected)
        try:
            self._query = self._connection.get_query(self._query)
        except (ProviderError, StatementError) as err:
            return False
        if self._query:
            self._columns = self._connection.get_columns()
            try:
                self._result, error = self._loop.run_until_complete(
                    self._connection.query(self._query)
                )
                if self._result and not error:
                    return asyncResult(result=self._result, columns=self._columns)
            except NoDataFound:
                print("NO DATA")
                return False
        else:
            return False

    def fetch(self):
        return self.all()

    def one(self):
        """
        one
           Get only one row from query
        """
        print("Connection Status: %s" % self._connection.connected)
        try:
            self._query = self._connection.get_query(self._query)
        except (ProviderError, StatementError) as err:
            return False
        if self._query:
            self._columns = self._connection.get_columns()
            try:
                self._result, error = self._loop.run_until_complete(
                    self._connection.queryrow(self._query)
                )
                if self._result and not error:
                    return asyncRecord(result=self._result, columns=self._columns)
            except NoDataFound:
                print("NO DATA")
                return False
        else:
            return False

    def fetchrow(self):
        return self.one()

    """
    Magic Methods
    """

    def __getitem__(self, name):
        """
        Sequence-like operators
        """
        if self._val:
            return self._val[name]
        else:
            return False

    def __setitem__(self, name, value):
        if name in self._columns:
            self._val[name] = value
        else:
            raise KeyError("asyncORM Error: Invalid Column %s" % name)

    def __contains__(self, name):
        if name in self._columns:
            return True
        else:
            return False

    def __iter__(self):
        return iter(self._fields)

    def __getattr__(self, name):
        """
        getter
        """
        if self._val:
            try:
                return self._val[name]
            except KeyError:
                if hasattr(self, name):
                    return super(asyncORM, self).__getattr__(name)
                raise KeyError("asyncORM Error: invalid column name %s" % name)
                return None
            except TypeError:
                if hasattr(self, name):
                    return super(asyncORM, self).__getattr__(name)
                raise TypeError("asyncORM Error: Empty Object")
                return None
        else:
            return False

    def __setattr__(self, name, value):
        """
        setter
        """
        # print("DEFINE ATTRIBUTE: name {}, value {} is system {}".format(name, value, hasattr(self, name)))
        if hasattr(self, name):
            self.__dict__[name] = value
        elif name in self._columns:
            if isinstance(value, list):
                values = ",".join("'{}'".format(str(v)) for v in value)
                self._val[name] = "ARRAY[{}]".format(values)
            else:
                type = self._fields[name]
                if type in ["integer", "double precision", "long", "float", "numeric"]:
                    self._val[name] = value
                else:
                    self._val[name] = "'{}'".format(value.replace("'", r"''"))
        else:
            # object.__setattr__(self, name, value)
            raise KeyError("asyncORM Error: Invalid Column %s" % name)
            return None

    def columns(self):
        """
        columns
            get Column Names
        """
        return self._columns

    """
    Simply ORM-like methods
    """

    def new(self):
        """
        new
           create a new record
        """
        self._columns = []
        self._fields = {}
        self._val = {}
        if self._table:
            result = self._connection.column_info(table=self._table)
            if result:
                for row in result:
                    self._columns.append(row["column_name"])
                    self._fields[row["column_name"]] = row["data_type"]
                    self._type = "new"
                return self
            else:
                return False
        else:
            return False

    def save(self, **kwargs):
        """
        save
           saving the result
        """
        table = self.get_table()
        if self._type == "new":
            try:
                result = self._connection.insert(table=table, data=self._val)
                return result
            except Exception as err:
                print(err)
                return False
        else:
            return False


class asyncResult(object):
    """
    asyncResult
       Class for a Resultset Object
    """

    _result = {}
    _columns = []
    _idx = 0

    def __init__(self, result, columns=[]):
        self._result = result
        if columns:
            self._columns = columns
        self._idx = 0

    def get_result(self):
        return self._result

    def __iter__(self):
        return self

    def __next__(self):
        """
        Next: next object from iterator
        """
        if self._idx < len(self._result):
            row = self._result[self._idx]
            self._idx += 1
            return asyncRecord(row, self._columns)
        # End of Iteration
        raise StopIteration


class asyncRecord(object):
    """
    asyncRecord
        Class for Record object
    ----
      params:
          result: asyncpg resultset
    """

    _row = {}
    _columns = []

    def __init__(self, result, columns=[]):
        self._row = result
        if columns:
            self._columns = columns

    def result(self, key):
        if self._row:
            try:
                return self._row[key]
            except KeyError:
                print("Error on key: %s " % key)
                return None
        else:
            return None

    def get_result(self):
        return self._row

    def columns(self):
        return self._columns

    """
     Section: Simple magic methods
    """

    def __contains__(self, key):
        if key in self._columns:
            return True
        elif key in self._row:
            return True
        else:
            return False

    def __delitem__(self, key):
        if self._row:
            del self._row[key]

    def __getitem__(self, key):
        """
        Sequence-like operators
        """
        if self._row:
            return self._row[key]
        else:
            return False

    @property
    def keys(self):
        return self._columns

    def __getattr__(self, attr):
        """
        Attributes for dict keys
        """
        if self._row:
            try:
                return self._row[attr]
            except KeyError:
                raise KeyError("asyncRecord Error: invalid column name %s" % attr)
                return None
            except TypeError:
                raise TypeError("asyncRecord Error: invalid Result")
                return None
        else:
            return False
