"""
Dataclass Model for SQL-oriented databases.
"""
from .base import Model, ModelMeta
from typing import (
    Optional,
    Any
)
from dataclasses import (
    is_dataclass
)


class SQLModel(Model, metaclass=ModelMeta):
    def query_raw(self, columns: str = '*', filter: Optional[str] = ''):
        name = self.__class__.__name__
        schema = self.Meta.schema if self.Meta.schema is not None else ""
        return f"SELECT {columns} FROM {schema!s}.{name!s} {filter}"

    def schema(self, type: str = "sql") -> Any:
        result = None
        name = self.__class__.__name__
        schema = self.Meta.schema if self.Meta.schema is not None else ""
        if type == "json":
            return super(SQLModel, self).schema(type)
        elif type == "sql" or type == "SQL":
            # TODO: using lexers to different types of SQL
            table = self.Meta.name if self.Meta.name is not None else name
            doc = f"CREATE TABLE IF NOT EXISTS {schema}.{table} (\n"
            cols = []
            pk = []
            for name, field in self.columns().items():
                key = field.name
                default = None
                try:
                    default = field.metadata["db_default"]
                except KeyError:
                    if field.default is not None:
                        default = f"{field.default!r}"
                default = (
                    f"DEFAULT {default!s}"
                    if isinstance(default, (str, int))
                    else ""
                )
                if is_dataclass(field.type):
                    tp = "jsonb"
                    nn = ""
                else:
                    try:
                        tp = field.db_type()
                    except Exception as err:
                        print(err)
                        tp = "varchar"
                    nn = "NOT NULL" if field.required() is True else ""
                if field.primary_key is True:
                    pk.append(key)
                # print(key, tp, nn, default)
                cols.append(f" {key} {tp} {nn} {default}")
            doc = "{}{}".format(doc, ",\n".join(cols))
            if len(pk) >= 1:
                primary = ", ".join(pk)
                cname = f"pk_{schema}_{table}_pkey"
                doc = "{},\n{}".format(
                    doc, f"CONSTRAINT {cname} PRIMARY KEY ({primary})"
                )
            doc = doc + "\n);"
            result = doc
        return result
