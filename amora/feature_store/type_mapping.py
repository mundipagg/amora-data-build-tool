import sqlmodel
from feast import ValueType
from sqlalchemy.sql import sqltypes

SQLALCHEMY_TYPES_TO_FS_TYPES = {
    sqltypes.Float: ValueType.FLOAT,
    sqltypes.String: ValueType.STRING,
    sqlmodel.AutoString: ValueType.STRING,
    sqltypes.Integer: ValueType.INT64,
    sqltypes.Boolean: ValueType.BOOL,
    sqltypes.TIMESTAMP: ValueType.UNIX_TIMESTAMP,
}
