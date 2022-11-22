from feast import ValueType
from feast.types import FeastType, from_value_type
from sqlalchemy import Column
from sqlalchemy.sql import sqltypes

_SQLALCHEMY_TYPES_TO_FS_TYPES = {
    sqltypes.Float: ValueType.FLOAT,
    sqltypes.String: ValueType.STRING,
    sqltypes.Integer: ValueType.INT64,
    sqltypes.Boolean: ValueType.BOOL,
    sqltypes.TIMESTAMP: ValueType.UNIX_TIMESTAMP,
    sqltypes.NullType: ValueType.NULL,
    (sqltypes.ARRAY, sqltypes.String): ValueType.STRING_LIST,
    (sqltypes.ARRAY, sqltypes.Integer): ValueType.INT64_LIST,
    (sqltypes.ARRAY, sqltypes.Boolean): ValueType.BOOL_LIST,
    (sqltypes.ARRAY, sqltypes.Float): ValueType.FLOAT_LIST,
}


def value_type_for_column(col: Column) -> ValueType:
    if isinstance(col.type, sqltypes.ARRAY):
        key = (col.type.__class__, col.type.item_type.__class__)
    else:
        key = col.type.__class__

    return _SQLALCHEMY_TYPES_TO_FS_TYPES[key]


def feast_type_for_colum(col: Column) -> FeastType:
    return from_value_type(value_type_for_column(col))
