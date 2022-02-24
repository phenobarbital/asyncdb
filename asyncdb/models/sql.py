"""
Dataclass Model for SQL-oriented databases.
"""
from .base import Model
from typing import (
    Optional,
    Any
)
from dataclasses import (
    is_dataclass
)


class SQLModel(Model):
    def query_raw(self, columns: str = '*', filter: Optional[str] = ''):
        name = self.__class__.__name__
        schema = self.Meta.schema if self.Meta.schema is not None else ""
        return f"SELECT {columns} FROM {schema!s}.{name!s} {filter}"

    @classmethod
    def model(cls, dialect: str = "sql") -> Any:
        clsname = cls.__name__
        schema = cls.Meta.schema if cls.Meta.schema is not None else ""
        table = cls.Meta.name if cls.Meta.name is not None else clsname.lower()
        columns = cls.columns(cls).items()
        if dialect == "sql" or dialect == "SQL":
            # TODO: using lexers to different types of SQL
            # And db_types to translate dataclass types to DB types.
            doc = f"CREATE TABLE IF NOT EXISTS {schema}.{table} (\n"
            cols = []
            pk = []
            for name, field in columns:
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
            return doc
        else:
            super(SQLModel, cls).model(cls, dialect)
            # return cls.model(cls, dialect)
