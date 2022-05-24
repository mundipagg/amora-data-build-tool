import sqlmodel
from feast import ValueType
from feast.types import Bool, Float64, Int64, String, UnixTimestamp
from sqlalchemy.sql import sqltypes

SQLALCHEMY_TYPES_TO_FS_VALUE_TYPES = {
    sqltypes.Float: ValueType.FLOAT,
    sqltypes.String: ValueType.STRING,
    sqlmodel.AutoString: ValueType.STRING,
    sqltypes.Integer: ValueType.INT64,
    sqltypes.Boolean: ValueType.BOOL,
    sqltypes.TIMESTAMP: ValueType.UNIX_TIMESTAMP,
}

SQLALCHEMY_TYPES_TO_FS_DTYPES = {
    sqltypes.Float: Float64,
    sqltypes.String: String,
    sqlmodel.AutoString: String,
    sqltypes.Integer: Int64,
    sqltypes.Boolean: Bool,
    sqltypes.TIMESTAMP: UnixTimestamp,
}
