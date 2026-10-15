import re

from memframe.exceptions import ConfigurationError

# ponytail: plot queries pull the raw columns into pandas; cap the fetch so a
# million-row table can't be dragged into a single chart. First-N rows (no
# ORDER BY — natural order is fine for plotting).
MAX_PLOT_ROWS = 10_000


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

# ponytail: DB_TO_PANDAS_DTYPE_MAP removed — dead code, zero runtime consumers; stdlib: use pandas.api.types.pandas_dtype if needed

class SQLIdentifierSanitizer:
    """
    SQL Identifier Sanitizer supporting both strict validation and safe normalization.

    Two rules, different jobs:
    - ``validate``: strict pretty-names (letters/digits/underscore). Legacy;
      too narrow for real quoted identifiers, so it is NOT the trust boundary.
    - ``sanitize``: quotable-normalization. Preserves every character that is
      safe inside a quoted identifier (including spaces and dashes — real
      SyncDB/CSV names), and neutralizes only what could break out of quotes.
      Callers must still quote the result. Raises on None/empty/NUL instead
      of silently rewriting to a different identifier.

    Handles:
    - Simple identifiers: column_name, table_name
    - Qualified identifiers: schema.table, db.schema.table
    - Validation mode: raises ValueError on invalid input (secure by default)
    - Normalization mode: cleans invalid characters (permissive)
    """

    # Pattern for valid identifier segments (letters, digits, underscore, must start with letter/underscore)
    # \Z (not $) so a trailing newline can't sneak past validation
    _VALID_SEGMENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*\Z")

    # Pattern for full qualified identifier (supports schema.table or db.schema.table)
    _VALID_QUALIFIED = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)*\Z")

    # Ponytail: only characters that can break out of a quoted identifier (or
    # poison a driver) are neutralized. Spaces, dashes and Unicode are legal
    # inside quotes, so rewriting them only misaddressed real columns.
    _DANGEROUS_CHARS = re.compile(r"[;\"'\(\)\[\]\{\}\*\|\\\/\%\+\=\<\>\!\?\&\^\~\`]")

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
            ConfigurationError: If identifier is invalid or not a string
        """
        if not isinstance(identifier, str):
            raise ConfigurationError(f"Identifier must be a string, got {type(identifier).__name__}")
        
        identifier = identifier.strip()
        
        if not identifier:
            raise ConfigurationError("Identifier cannot be empty")
        
        if allow_qualified:
            if not cls._VALID_QUALIFIED.match(identifier):
                raise ConfigurationError(
                    f"Invalid SQL identifier: '{identifier}'. "
                    "Must contain only letters, digits, underscores, and dots (for schema.table). "
                    "Must start with letter or underscore."
                )
        else:
            if not cls._VALID_SEGMENT.match(identifier):
                raise ConfigurationError(
                    f"Invalid SQL identifier: '{identifier}'. "
                    "Must contain only letters, digits, underscores. "
                    "Must start with letter or underscore. No dots allowed."
                )
        
        return identifier

    @classmethod
    def sanitize(cls, identifier: str, allow_qualified: bool = True) -> str:
        """
        Normalization mode - neutralizes quote-breaking characters.

        Preserves spaces, dashes and Unicode (all safe once quoted) so real
        column names address themselves. Raises on None/empty/NUL instead of
        silently returning a different identifier.

        Args:
            identifier: The identifier to sanitize
            allow_qualified: If True, preserves dot notation (schema.table becomes schema_table if invalid)

        Returns:
            A quotable SQL identifier.

        Raises:
            ConfigurationError: If identifier is None, empty, or contains NUL.
        """
        if identifier is None:
            raise ConfigurationError("Identifier cannot be None")
        if not isinstance(identifier, str):
            identifier = str(identifier)

        # Strip whitespace and quotes
        identifier = identifier.strip().strip('"').strip("'")

        if not identifier:
            raise ConfigurationError("Identifier cannot be empty")
        if "\x00" in identifier:
            raise ConfigurationError("Identifier contains a NUL byte")

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

        # Ponytail: word chars, spaces and dashes survive (all safe once
        # quoted); only other punctuation/symbols are neutralized.
        segment = re.sub(r"[^\w \-]", "_", segment)

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
        except (ConfigurationError, ValueError, TypeError):
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



