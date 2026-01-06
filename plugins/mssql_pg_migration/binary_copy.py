"""
Binary COPY Protocol Implementation for PostgreSQL

This module implements PostgreSQL's binary COPY format for faster data transfer.
Binary COPY is 20-30% faster than text CSV COPY because:
1. No text encoding/decoding overhead
2. More compact wire format
3. PostgreSQL's binary parser is highly optimized

Binary COPY Format:
- Header: PGCOPY signature + flags + extension length
- Rows: column count + (length, data) for each column
- Trailer: -1 as int16

Reference: https://www.postgresql.org/docs/current/sql-copy.html
"""

import struct
from datetime import datetime, date, time as dt_time, timedelta
from decimal import Decimal, InvalidOperation
from io import BytesIO, RawIOBase
from typing import Any, Dict, Iterable, List, Optional, Tuple
from uuid import UUID
import logging

logger = logging.getLogger(__name__)

# PostgreSQL binary COPY header
# "PGCOPY\n\xff\r\n\0" followed by flags (4 bytes) and header extension length (4 bytes)
BINARY_HEADER = b'PGCOPY\n\xff\r\n\x00' + struct.pack('>I', 0) + struct.pack('>I', 0)

# Binary COPY trailer: -1 as int16
BINARY_TRAILER = struct.pack('>h', -1)

# PostgreSQL epoch: 2000-01-01 00:00:00 UTC
PG_EPOCH = datetime(2000, 1, 1)
PG_EPOCH_DATE = date(2000, 1, 1)

# Days between Unix epoch (1970-01-01) and PostgreSQL epoch (2000-01-01)
UNIX_TO_PG_EPOCH_DAYS = (PG_EPOCH_DATE - date(1970, 1, 1)).days


class BinaryEncoder:
    """
    Encodes Python values to PostgreSQL binary format.

    Supports the following PostgreSQL types:
    - int2, int4, int8 (smallint, integer, bigint)
    - float4, float8 (real, double precision)
    - bool (boolean)
    - text, varchar, char (text types)
    - bytea (binary data)
    - timestamp, timestamptz (without/with timezone)
    - date
    - time, timetz
    - numeric/decimal
    - uuid
    - json, jsonb
    """

    # Type OIDs for PostgreSQL (for reference)
    TYPE_OIDS = {
        'bool': 16,
        'bytea': 17,
        'int8': 20,
        'int2': 21,
        'int4': 23,
        'text': 25,
        'float4': 700,
        'float8': 701,
        'varchar': 1043,
        'date': 1082,
        'time': 1083,
        'timestamp': 1114,
        'timestamptz': 1184,
        'numeric': 1700,
        'uuid': 2950,
        'json': 114,
        'jsonb': 3802,
    }

    def __init__(self, column_types: Optional[List[str]] = None):
        """
        Initialize the encoder with optional column type hints.

        Args:
            column_types: List of PostgreSQL type names for each column.
                         If None, types are inferred from Python values.
        """
        self.column_types = column_types

    def encode_value(self, value: Any, pg_type: Optional[str] = None) -> bytes:
        """
        Encode a single value to PostgreSQL binary format.

        Args:
            value: Python value to encode
            pg_type: PostgreSQL type name (optional hint)

        Returns:
            Bytes including the 4-byte length prefix
        """
        if value is None:
            # NULL is represented as -1 length
            return struct.pack('>i', -1)

        # Get raw binary data
        data = self._encode_raw(value, pg_type)

        # Return length-prefixed data
        return struct.pack('>i', len(data)) + data

    def _encode_raw(self, value: Any, pg_type: Optional[str] = None) -> bytes:
        """
        Encode value to raw bytes (without length prefix).

        Args:
            value: Non-null Python value
            pg_type: PostgreSQL type hint

        Returns:
            Raw bytes for the value
        """
        # Type-specific encoding based on hint
        if pg_type:
            pg_type_lower = pg_type.lower()
            if pg_type_lower in ('int2', 'smallint'):
                return self._encode_int2(value)
            elif pg_type_lower in ('int4', 'integer', 'int', 'serial'):
                return self._encode_int4(value)
            elif pg_type_lower in ('int8', 'bigint', 'bigserial'):
                return self._encode_int8(value)
            elif pg_type_lower in ('float4', 'real'):
                return self._encode_float4(value)
            elif pg_type_lower in ('float8', 'double precision', 'double'):
                return self._encode_float8(value)
            elif pg_type_lower in ('bool', 'boolean'):
                return self._encode_bool(value)
            elif pg_type_lower in ('text', 'varchar', 'char', 'character varying'):
                return self._encode_text(value)
            elif pg_type_lower == 'bytea':
                return self._encode_bytea(value)
            elif pg_type_lower == 'timestamp':
                return self._encode_timestamp(value)
            elif pg_type_lower in ('timestamptz', 'timestamp with time zone'):
                return self._encode_timestamp(value)
            elif pg_type_lower == 'date':
                return self._encode_date(value)
            elif pg_type_lower == 'time':
                return self._encode_time(value)
            elif pg_type_lower in ('numeric', 'decimal'):
                return self._encode_numeric(value)
            elif pg_type_lower == 'uuid':
                return self._encode_uuid(value)
            elif pg_type_lower in ('json', 'jsonb'):
                return self._encode_json(value, is_jsonb=(pg_type_lower == 'jsonb'))

        # Infer type from Python value
        return self._encode_inferred(value)

    def _encode_inferred(self, value: Any) -> bytes:
        """Encode value by inferring type from Python type."""
        if isinstance(value, bool):
            return self._encode_bool(value)
        elif isinstance(value, int):
            # Choose smallest integer type that fits
            if -32768 <= value <= 32767:
                return self._encode_int2(value)
            elif -2147483648 <= value <= 2147483647:
                return self._encode_int4(value)
            else:
                return self._encode_int8(value)
        elif isinstance(value, float):
            return self._encode_float8(value)
        elif isinstance(value, Decimal):
            return self._encode_numeric(value)
        elif isinstance(value, datetime):
            return self._encode_timestamp(value)
        elif isinstance(value, date):
            return self._encode_date(value)
        elif isinstance(value, dt_time):
            return self._encode_time(value)
        elif isinstance(value, UUID):
            return self._encode_uuid(value)
        elif isinstance(value, bytes):
            return self._encode_bytea(value)
        elif isinstance(value, (dict, list)):
            return self._encode_json(value, is_jsonb=False)
        else:
            # Default: encode as text
            return self._encode_text(value)

    def _encode_bool(self, value: Any) -> bytes:
        """Encode boolean (1 byte)."""
        return b'\x01' if value else b'\x00'

    def _encode_int2(self, value: Any) -> bytes:
        """Encode smallint/int2 (2 bytes big-endian)."""
        return struct.pack('>h', int(value))

    def _encode_int4(self, value: Any) -> bytes:
        """Encode integer/int4 (4 bytes big-endian)."""
        return struct.pack('>i', int(value))

    def _encode_int8(self, value: Any) -> bytes:
        """Encode bigint/int8 (8 bytes big-endian)."""
        return struct.pack('>q', int(value))

    def _encode_float4(self, value: Any) -> bytes:
        """Encode real/float4 (4 bytes IEEE 754)."""
        return struct.pack('>f', float(value))

    def _encode_float8(self, value: Any) -> bytes:
        """Encode double precision/float8 (8 bytes IEEE 754)."""
        return struct.pack('>d', float(value))

    def _encode_text(self, value: Any) -> bytes:
        """Encode text/varchar as UTF-8."""
        if isinstance(value, bytes):
            return value
        return str(value).encode('utf-8')

    def _encode_bytea(self, value: Any) -> bytes:
        """Encode bytea (raw bytes)."""
        if isinstance(value, bytes):
            return value
        elif isinstance(value, str):
            return value.encode('utf-8')
        else:
            return bytes(value)

    def _encode_timestamp(self, value: Any) -> bytes:
        """
        Encode timestamp as microseconds since PostgreSQL epoch (2000-01-01).

        PostgreSQL stores timestamps as int64 microseconds since 2000-01-01.
        """
        if isinstance(value, str):
            # Parse ISO format string
            try:
                value = datetime.fromisoformat(value.replace('Z', '+00:00'))
            except ValueError:
                # Try common formats
                for fmt in ('%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M:%S.%f'):
                    try:
                        value = datetime.strptime(value, fmt)
                        break
                    except ValueError:
                        continue
                else:
                    raise ValueError(f"Cannot parse timestamp: {value}")

        if isinstance(value, datetime):
            # Remove timezone info for timestamp without time zone
            if value.tzinfo is not None:
                value = value.replace(tzinfo=None)

            # Calculate microseconds since PG epoch
            delta = value - PG_EPOCH
            microseconds = int(delta.total_seconds() * 1_000_000)
            return struct.pack('>q', microseconds)
        else:
            raise TypeError(f"Cannot encode timestamp from {type(value)}")

    def _encode_date(self, value: Any) -> bytes:
        """
        Encode date as days since PostgreSQL epoch (2000-01-01).

        PostgreSQL stores dates as int32 days since 2000-01-01.
        """
        if isinstance(value, str):
            value = datetime.strptime(value, '%Y-%m-%d').date()
        elif isinstance(value, datetime):
            value = value.date()

        if isinstance(value, date):
            days = (value - PG_EPOCH_DATE).days
            return struct.pack('>i', days)
        else:
            raise TypeError(f"Cannot encode date from {type(value)}")

    def _encode_time(self, value: Any) -> bytes:
        """
        Encode time as microseconds since midnight.

        PostgreSQL stores time as int64 microseconds since midnight.
        """
        if isinstance(value, str):
            # Parse time string (HH:MM:SS or HH:MM:SS.ffffff)
            parts = value.split(':')
            hours = int(parts[0])
            minutes = int(parts[1]) if len(parts) > 1 else 0
            seconds = float(parts[2]) if len(parts) > 2 else 0.0
            microseconds = int((hours * 3600 + minutes * 60 + seconds) * 1_000_000)
            return struct.pack('>q', microseconds)
        elif isinstance(value, dt_time):
            microseconds = (
                value.hour * 3600_000_000 +
                value.minute * 60_000_000 +
                value.second * 1_000_000 +
                value.microsecond
            )
            return struct.pack('>q', microseconds)
        else:
            raise TypeError(f"Cannot encode time from {type(value)}")

    def _encode_numeric(self, value: Any) -> bytes:
        """
        Encode numeric/decimal.

        PostgreSQL numeric format:
        - ndigits (int16): number of base-10000 digits
        - weight (int16): weight of first digit
        - sign (int16): 0=positive, 0x4000=negative, 0xC000=NaN
        - dscale (int16): display scale
        - digits[]: array of int16 base-10000 digits
        """
        if isinstance(value, str):
            try:
                value = Decimal(value)
            except InvalidOperation:
                # Handle special values
                if value.lower() == 'nan':
                    return struct.pack('>hhhh', 0, 0, 0xC000, 0)
                raise
        elif not isinstance(value, Decimal):
            value = Decimal(str(value))

        # Handle special cases
        if value.is_nan():
            return struct.pack('>hhhh', 0, 0, 0xC000, 0)
        if value.is_infinite():
            # PostgreSQL doesn't support infinity in numeric
            raise ValueError("Numeric type does not support infinity")

        # Determine sign
        sign = 0 if value >= 0 else 0x4000
        value = abs(value)

        # Convert to tuple representation
        sign_char, digits, exponent = value.as_tuple()

        # Handle zero
        if not digits or value == 0:
            return struct.pack('>hhhh', 0, 0, sign, 0)

        # Calculate scale (number of digits after decimal point)
        if exponent >= 0:
            dscale = 0
            # Append zeros for positive exponent
            digit_str = ''.join(str(d) for d in digits) + '0' * exponent
        else:
            dscale = -exponent
            digit_str = ''.join(str(d) for d in digits)
            # Pad with leading zeros if needed
            if len(digit_str) < dscale:
                digit_str = '0' * (dscale - len(digit_str)) + digit_str

        # Split into integer and fractional parts
        if dscale > 0:
            int_part = digit_str[:-dscale] or '0'
            frac_part = digit_str[-dscale:]
        else:
            int_part = digit_str
            frac_part = ''

        # Pad integer part to multiple of 4
        int_part = int_part.lstrip('0') or '0'
        int_pad = (4 - len(int_part) % 4) % 4
        int_part = '0' * int_pad + int_part

        # Pad fractional part to multiple of 4
        frac_pad = (4 - len(frac_part) % 4) % 4
        frac_part = frac_part + '0' * frac_pad

        # Convert to base-10000 digits
        pg_digits = []
        for i in range(0, len(int_part), 4):
            pg_digits.append(int(int_part[i:i+4]))
        for i in range(0, len(frac_part), 4):
            pg_digits.append(int(frac_part[i:i+4]))

        # Remove trailing zeros from digits
        while pg_digits and pg_digits[-1] == 0:
            pg_digits.pop()

        # Remove leading zeros and adjust weight
        weight = len(int_part) // 4 - 1
        while pg_digits and pg_digits[0] == 0:
            pg_digits.pop(0)
            weight -= 1

        ndigits = len(pg_digits)

        # Pack header
        result = struct.pack('>hhhh', ndigits, weight, sign, dscale)

        # Pack digits
        for d in pg_digits:
            result += struct.pack('>H', d)

        return result

    def _encode_uuid(self, value: Any) -> bytes:
        """Encode UUID as 16 bytes."""
        if isinstance(value, str):
            value = UUID(value)
        if isinstance(value, UUID):
            return value.bytes
        else:
            raise TypeError(f"Cannot encode UUID from {type(value)}")

    def _encode_json(self, value: Any, is_jsonb: bool = False) -> bytes:
        """Encode JSON/JSONB."""
        import json
        json_str = json.dumps(value, default=str)
        if is_jsonb:
            # JSONB has a version byte prefix
            return b'\x01' + json_str.encode('utf-8')
        return json_str.encode('utf-8')


class BinaryRowStream(RawIOBase):
    """
    Streaming binary COPY producer for PostgreSQL.

    This class lazily generates PostgreSQL binary COPY format from an iterator
    of rows, minimizing memory usage for large data transfers.

    Usage:
        encoder = BinaryEncoder(column_types)
        stream = BinaryRowStream(rows, encoder)
        cursor.copy_expert("COPY table FROM STDIN WITH (FORMAT BINARY)", stream)
    """

    def __init__(
        self,
        rows: Iterable[Tuple[Any, ...]],
        encoder: BinaryEncoder,
        column_types: Optional[List[str]] = None,
    ):
        """
        Initialize the binary stream.

        Args:
            rows: Iterable of row tuples
            encoder: BinaryEncoder instance
            column_types: Optional list of PostgreSQL type names
        """
        self._iterator = iter(rows)
        self._encoder = encoder
        self._column_types = column_types
        self._buffer = BytesIO()
        self._header_written = False
        self._exhausted = False
        self._row_count = 0

    def readable(self) -> bool:
        return True

    def read(self, size: int = -1) -> bytes:
        """Read up to size bytes from the stream."""
        # Write header if not done yet
        if not self._header_written:
            self._buffer.write(BINARY_HEADER)
            self._header_written = True

        # Fill buffer until we have enough data
        while (size < 0 or self._buffer.tell() < size) and not self._exhausted:
            try:
                row = next(self._iterator)
                self._encode_row(row)
                self._row_count += 1
            except StopIteration:
                # Write trailer
                self._buffer.write(BINARY_TRAILER)
                self._exhausted = True
                break

        # Return requested amount of data
        data = self._buffer.getvalue()
        self._buffer = BytesIO()

        if size < 0 or len(data) <= size:
            return data

        # Put excess back in buffer
        self._buffer.write(data[size:])
        return data[:size]

    def _encode_row(self, row: Tuple[Any, ...]) -> None:
        """Encode a single row and append to buffer."""
        # Write column count
        self._buffer.write(struct.pack('>h', len(row)))

        # Write each column
        for i, value in enumerate(row):
            pg_type = self._column_types[i] if self._column_types and i < len(self._column_types) else None
            self._buffer.write(self._encoder.encode_value(value, pg_type))

    @property
    def rows_encoded(self) -> int:
        """Number of rows encoded so far."""
        return self._row_count


def get_pg_type_from_mssql(mssql_type: str) -> str:
    """
    Map MSSQL type to PostgreSQL type for binary encoding.

    Args:
        mssql_type: SQL Server data type name

    Returns:
        PostgreSQL type name
    """
    mssql_type = mssql_type.lower()

    type_map = {
        # Integer types
        'bit': 'bool',
        'tinyint': 'int2',
        'smallint': 'int2',
        'int': 'int4',
        'bigint': 'int8',

        # Floating point
        'real': 'float4',
        'float': 'float8',

        # Decimal/numeric
        'decimal': 'numeric',
        'numeric': 'numeric',
        'money': 'numeric',
        'smallmoney': 'numeric',

        # String types
        'char': 'text',
        'varchar': 'text',
        'text': 'text',
        'nchar': 'text',
        'nvarchar': 'text',
        'ntext': 'text',

        # Binary types
        'binary': 'bytea',
        'varbinary': 'bytea',
        'image': 'bytea',

        # Date/time types
        'date': 'date',
        'time': 'time',
        'datetime': 'timestamp',
        'datetime2': 'timestamp',
        'smalldatetime': 'timestamp',
        'datetimeoffset': 'timestamptz',

        # Other types
        'uniqueidentifier': 'uuid',
        'xml': 'text',
    }

    # Handle types with parameters (e.g., varchar(50))
    base_type = mssql_type.split('(')[0].strip()

    return type_map.get(base_type, 'text')


def create_binary_copy_sql(
    schema_name: str,
    table_name: str,
    columns: List[str],
) -> str:
    """
    Create the COPY SQL statement for binary format.

    Args:
        schema_name: Target schema
        table_name: Target table
        columns: List of column names

    Returns:
        COPY SQL statement
    """
    from psycopg2 import sql

    quoted_columns = sql.SQL(', ').join([sql.Identifier(col) for col in columns])
    copy_sql = sql.SQL(
        'COPY {}.{} ({}) FROM STDIN WITH (FORMAT BINARY)'
    ).format(
        sql.Identifier(schema_name),
        sql.Identifier(table_name),
        quoted_columns
    )

    return copy_sql.as_string(None)  # Returns the composed SQL string


def stream_binary_copy(
    rows: Iterable[Tuple[Any, ...]],
    schema_name: str,
    table_name: str,
    columns: List[str],
    column_types: Optional[List[str]],
    postgres_conn,
) -> int:
    """
    Stream rows to PostgreSQL using binary COPY format.

    This is the main entry point for binary COPY operations.

    Args:
        rows: Iterable of row tuples to write
        schema_name: Target schema name
        table_name: Target table name
        columns: List of column names
        column_types: List of PostgreSQL type names (optional)
        postgres_conn: PostgreSQL connection (psycopg2)

    Returns:
        Number of rows written
    """
    from psycopg2 import sql as psql

    encoder = BinaryEncoder(column_types)
    stream = BinaryRowStream(rows, encoder, column_types)

    # Build COPY SQL
    quoted_columns = psql.SQL(', ').join([psql.Identifier(col) for col in columns])
    copy_sql = psql.SQL(
        'COPY {}.{} ({}) FROM STDIN WITH (FORMAT BINARY)'
    ).format(
        psql.Identifier(schema_name),
        psql.Identifier(table_name),
        quoted_columns
    )

    with postgres_conn.cursor() as cursor:
        cursor.copy_expert(copy_sql, stream)

    return stream.rows_encoded
