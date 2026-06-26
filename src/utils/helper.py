import re
from typing import Dict


_DATETIME_DIRECT_METHODS = {
        "aextract",
        "extract",
        "ayear",
        "year",
        "amonth",
        "month",
        "aday",
        "day",
        "ahour",
        "hour",
        "aminute",
        "minute",
        "asecond",
        "second",
        "adayofweek",
        "dayofweek",
        "adayofyear",
        "dayofyear",
        "aweek",
        "week",
        "aquarter",
        "quarter",
        "atz_localize",
        "tz_localize",
        "atz_convert",
        "tz_convert",
        "ais_month_start",
        "is_month_start",
        "ais_month_end",
        "is_month_end",
        "ais_year_start",
        "is_year_start",
        "ais_year_end",
        "is_year_end",
        "ais_quarter_start",
        "is_quarter_start",
        "ais_quarter_end",
        "is_quarter_end",
        "adays_in_month",
        "days_in_month",
        "ais_weekend",
        "is_weekend",
        "ais_weekday",
        "is_weekday",
        "ais_business_day",
        "is_business_day",
        "aweek_of_month",
        "week_of_month",
        "atimestamp",
        "timestamp",
        "afrom_timestamp",
        "from_timestamp",
        "astrftime",
        "strftime",
        "astrptime",
        "strptime",
        "areplace",
        "replace",
        "anormalize",
        "normalize",
    }

DB_TO_PANDAS_DTYPE_MAP: Dict[str, str] = {
    # =========================
    # INTEGERS
    # =========================
    "smallint": "int16",
    "int2": "int16",

    "integer": "int32",
    "int": "int32",
    "int4": "int32",

    "bigint": "int64",
    "int8": "int64",

    # =========================
    # FLOATING POINT
    # =========================
    "real": "float32",
    "float4": "float32",

    "double precision": "float64",
    "float8": "float64",
    "double": "float64",

    "float": "float64",  # default

    # =========================
    # DECIMAL / NUMERIC
    # =========================
    "numeric": "float64",
    "decimal": "float64",

    # =========================
    # BOOLEAN
    # =========================
    "boolean": "bool",
    "bool": "bool",

    # =========================
    # STRING / TEXT
    # =========================
    "text": "object",
    "varchar": "object",
    "character varying": "object",
    "char": "object",
    "character": "object",
    "string": "object",

    # =========================
    # DATE / TIME
    # =========================
    "date": "datetime64[ns]",
    "timestamp": "datetime64[ns]",
    "timestamp without time zone": "datetime64[ns]",
    "timestamp with time zone": "datetime64[ns]",

    "time": "object",  # pandas limitation

    # =========================
    # INTERVAL
    # =========================
    "interval": "timedelta64[ns]",

    # =========================
    # BINARY / BLOB
    # =========================
    "bytea": "object",
    "blob": "object",

    # =========================
    # JSON
    # =========================
    "json": "object",
    "jsonb": "object",

    # =========================
    # UUID
    # =========================
    "uuid": "object",
}

class SQLIdentifierSanitizer:
    """
    SQL Identifier Sanitizer supporting both strict validation and safe normalization.
    
    Handles:
    - Simple identifiers: column_name, table_name
    - Qualified identifiers: schema.table, db.schema.table
    - Validation mode: raises ValueError on invalid input (secure by default)
    - Normalization mode: cleans invalid characters (permissive)
    """
    
    # Pattern for valid identifier segments (letters, digits, underscore, must start with letter/underscore)
    _VALID_SEGMENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
    
    # Pattern for full qualified identifier (supports schema.table or db.schema.table)
    _VALID_QUALIFIED = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)*$")
    
    # Pattern to detect dangerous characters (SQL injection attempts)
    _DANGEROUS_CHARS = re.compile(r"[;\"'\-\s\(\)\[\]\{\}\*\|\\\/\%\+\=\<\>\!\?\&\^\~\`]")

    @classmethod
    def validate(cls, identifier: str, allow_qualified: bool = True) -> str:
        """
        Strict validation mode - raises ValueError if identifier is unsafe.
        
        Args:
            identifier: The identifier to validate (column, table, or schema.table)
            allow_qualified: If True, allows dot notation (schema.table)
            
        Returns:
            The cleaned identifier if valid
            
        Raises:
            ValueError: If identifier contains unsafe characters or invalid format
            TypeError: If identifier is not a string
        """
        if not isinstance(identifier, str):
            raise TypeError(f"Identifier must be a string, got {type(identifier).__name__}")
        
        identifier = identifier.strip()
        
        if not identifier:
            raise ValueError("Identifier cannot be empty")
        
        if allow_qualified:
            if not cls._VALID_QUALIFIED.match(identifier):
                raise ValueError(
                    f"Invalid SQL identifier: '{identifier}'. "
                    "Must contain only letters, digits, underscores, and dots (for schema.table). "
                    "Must start with letter or underscore."
                )
        else:
            if not cls._VALID_SEGMENT.match(identifier):
                raise ValueError(
                    f"Invalid SQL identifier: '{identifier}'. "
                    "Must contain only letters, digits, underscores. "
                    "Must start with letter or underscore. No dots allowed."
                )
        
        return identifier

    @classmethod
    def sanitize(cls, identifier: str, allow_qualified: bool = True) -> str:
        """
        Normalization mode - cleans invalid characters to create a safe identifier.
        
        Args:
            identifier: The identifier to sanitize
            allow_qualified: If True, preserves dot notation (schema.table becomes schema_table if invalid)
            
        Returns:
            A safe SQL identifier with invalid chars replaced by underscores
        """
        if not isinstance(identifier, str):
            identifier = str(identifier)
        
        # Strip whitespace and quotes
        identifier = identifier.strip().strip('"').strip("'")
        
        if not identifier:
            return "_"
        
        if allow_qualified and "." in identifier:
            # Handle qualified names (schema.table) - sanitize each part separately
            parts = identifier.split(".")
            sanitized_parts = [cls._sanitize_segment(part) for part in parts]
            return ".".join(sanitized_parts)
        else:
            return cls._sanitize_segment(identifier)

    @classmethod
    def _sanitize_segment(cls, segment: str) -> str:
        """Sanitize a single identifier segment (no dots)."""
        # Replace dangerous/special characters with underscores
        segment = cls._DANGEROUS_CHARS.sub("_", segment)
        
        # Preserve Unicode word characters (e.g., REGIÃO) and replace other
        # unsafe punctuation/symbols with underscore.
        segment = re.sub(r"[^\w]", "_", segment)
        
        # Handle empty result after sanitization
        if not segment:
            return "_identifier"
        
        return segment

    @classmethod
    def is_valid(cls, identifier: str, allow_qualified: bool = True) -> bool:
        """
        Check if identifier is valid without raising exceptions.
        
        Returns:
            True if valid, False otherwise
        """
        try:
            cls.validate(identifier, allow_qualified)
            return True
        except (ValueError, TypeError):
            return False

    @classmethod
    def sanitize_many(cls, identifiers: list, allow_qualified: bool = True) -> list:
        """
        Sanitize multiple identifiers at once.
        
        Returns:
            List of sanitized identifiers
        """
        return [cls.sanitize(idf, allow_qualified) for idf in identifiers]


# Convenience functions for direct usage
def sanitize_sql_identifier(identifier: str, allow_qualified: bool = True) -> str:
    """Normalize/clean an SQL identifier."""
    return SQLIdentifierSanitizer.sanitize(identifier, allow_qualified)

def validate_sql_identifier(identifier: str, allow_qualified: bool = True) -> str:
    """Strictly validate an SQL identifier, raise if invalid."""
    return SQLIdentifierSanitizer.validate(identifier, allow_qualified)



