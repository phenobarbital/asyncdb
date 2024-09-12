from typing import Any, List
from abc import ABC, abstractmethod
import uuid
import inspect
import types
from datamodel.exceptions import ValidationError
from ..exceptions import DriverError
from ..models import Model, Field, is_missing, is_dataclass
from ..utils.types import Entity


null_values = {"null", "NULL"}
not_null_values = {"!null", "!NULL"}


class ModelBackend(ABC):
    """
    Interface for Backends with Dataclass-based Models Support.
    """

    # ## Class-based Methods.
    async def _create_(self, _model: Model, rows: list):
        """
        Create all records based on a dataset and return result.
        """
        try:
            table = f"{_model.Meta.name}"
        except AttributeError:
            table = _model.__name__
        results = []
        for row in rows:
            try:
                record = _model(**row)
            except (ValueError, ValidationError) as e:
                raise ValueError(f"Invalid Row for Model {_model}: {e}") from e
            if record:
                try:
                    result = await record.insert()
                    results.append(result)
                except Exception as e:
                    raise DriverError(f"Error on Creation {table}: {e}") from e
        return results

    @abstractmethod
    async def _remove_(self, _model: Model, **kwargs):
        """
        Deleting some records using Model.
        """

    @abstractmethod
    async def _updating_(self, _model: Model, *args, _filter: dict = None, **kwargs):
        """
        Updating records using Model.
        """

    @abstractmethod
    async def _fetch_(self, _model: Model, *args, **kwargs):
        """
        Returns one row from Model.
        """

    @abstractmethod
    async def _filter_(self, _model: Model, *args, **kwargs):
        """
        Filter a Model using Fields.
        """

    @abstractmethod
    async def _select_(self, _model: Model, *args, **kwargs):
        """
        Get a query from Model.
        """

    @abstractmethod
    async def _all_(self, _model: Model, *args):
        """
        Get queries with model.
        """

    @abstractmethod
    async def _get_(self, _model: Model, *args, **kwargs):
        """
        Get one row from model.
        """

    @abstractmethod
    async def _delete_(self, _model: Model, **kwargs):
        """
        delete a row from model.
        """

    @abstractmethod
    async def _update_(self, _model: Model, **kwargs):
        """
        Updating a row in a Model.
        """

    @abstractmethod
    async def _save_(self, _model: Model, **kwargs):
        """
        Save a row in a Model, using Insert-or-Update methodology.
        """

    @abstractmethod
    async def _insert_(self, _model: Model, **kwargs):
        """
        insert a row from model.
        """

    ## Aux Methods:
    def _get_value(self, field: Field, value: Any) -> Any:
        datatype = field.type
        new_val = None
        if is_dataclass(datatype) and value is not None:
            new_val = None if is_missing(value) else value
        if inspect.isclass(datatype) and value is None:
            if isinstance(datatype, (types.BuiltinFunctionType, types.FunctionType)):
                try:
                    new_val = datatype()
                except (TypeError, ValueError, AttributeError):
                    self._logger.error(f"Error Calling {datatype} in Field {field}")
                    new_val = None
        elif callable(datatype) and value is None:
            new_val = None
        else:
            new_val = value
        return new_val

    def _get_attribute(self, field: Field, value: Any, attr: str = "primary_key") -> Any:
        if hasattr(field, attr):
            datatype = field.type
            if field.primary_key is True:
                value = Entity.toSQL(value, datatype)
                return value
        return None

    def _where(self, fields: dict[Field], **where):
        """
        TODO: add conditions for BETWEEN, NOT NULL, NULL, etc
           Re-think functionality for parsing where conditions.
        """
        if not fields or not where or not isinstance(where, dict):
            return ""
        _cond = []
        for k, v in where.items():
            f = fields[k]
            datatype = f.type
            condition = self._get_condition(k, f, v, datatype)
            _cond.append(condition)
        _and = " AND ".join(_cond)
        result = f"\nWHERE {_and}"
        return result

    def _get_condition(self, key: str, field: Field, value: Any, datatype: Any) -> str:
        condition = ""
        if isinstance(value, list):
            null_vals = f" OR {key} is NULL" if None in value else ""
            values = ",".join(self._format_value(v) for v in value)
            condition = f"({key} = ANY(ARRAY[{values}]){null_vals})"
        elif value is None or value in null_values:
            condition = f"{key} is NULL"
        elif value in not_null_values:
            condition = f"{key} is NOT NULL"
        elif isinstance(value, bool):
            val = str(value)
            condition = f"{key} is {val}"
        elif isinstance(datatype, (list, List)):  # pylint: disable=W6001
            val = ", ".join([str(Entity.escapeLiteral(v, type(v))) for v in value])
            condition = f"ARRAY[{val}]<@ {key}::character varying[]"
        elif Entity.is_array(datatype):
            val = ", ".join([str(Entity.escapeLiteral(v, type(v))) for v in value])
            condition = f"{key} IN ({val})"
        else:
            # is an scalar value
            val = Entity.escapeLiteral(value, datatype)
            condition = f"{key}={val}"
        return condition

    def _format_value(self, value):
        if isinstance(value, str) and value is not None:
            return f"'{value}'"
        elif isinstance(value, uuid.UUID) and value is not None:
            return f"uuid('{value}')"
        elif value is not None:
            return str(value)
        return "NULL"
