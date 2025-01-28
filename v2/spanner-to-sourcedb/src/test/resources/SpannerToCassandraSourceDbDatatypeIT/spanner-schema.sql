CREATE TABLE IF NOT EXISTS alldatatypecolumns (
    varchar_column STRING(20) NOT NULL,  -- Primary Key column
    tinyint_column INT64,                -- No specific TINYINT in Spanner, use INT64
    text_column STRING(MAX),             -- Fine for large text
    date_column DATE,                    -- Correct usage for Date
    smallint_column INT64,               -- Use INT64 for small integers
    mediumint_column INT64,              -- Use INT64 for medium-sized integers
    int_column INT64,                    -- Use INT64 for general integers
    bigint_column INT64,                 -- Use INT64 for large integers
    float_column FLOAT64,                -- Correct for floating-point numbers
    double_column FLOAT64,               -- Same as FLOAT64 for double precision
    decimal_column NUMERIC,              -- Correct for arbitrary precision numbers
    datetime_column TIMESTAMP,           -- TIMESTAMP for date & time
    timestamp_column TIMESTAMP,          -- Same as TIMESTAMP
    time_column STRING(MAX),             -- Use for time data (can store it as text)
    year_column STRING(MAX),             -- Year as a string (assuming formatted text)
    char_column STRING(10),              -- Fixed-length string for short data
    tinytext_column STRING(MAX),         -- Large text storage
    mediumtext_column STRING(MAX),       -- Large text storage
    longtext_column STRING(MAX),         -- Large text storage
    enum_column STRING(MAX),             -- Enum-like values stored as strings
    bool_column BOOL,                    -- Correct for boolean values
    other_bool_column BOOL,              -- Another boolean column
    bytes_column BYTES(MAX),             -- Correct for binary data storage

    -- List Columns (Use ARRAY or JSON for collection types)
    list_text_column JSON,               -- Storing list of text data
    list_int_column JSON,                -- Storing list of integers
    frozen_list_bigint_column JSON,      -- Storing a frozen list of big integers

    -- Set Columns (Use ARRAY or JSON for sets)
    set_text_column JSON,                -- Storing a set of text data
    set_date_column JSON,                -- Storing a set of date values
    frozen_set_bool_column JSON,         -- Storing a frozen set of boolean values

    -- Map Columns (Use JSON for map-type data)
    map_text_to_int_column JSON,         -- Storing map of text to integer
    map_date_to_text_column JSON,        -- Storing map of dates to text
    frozen_map_int_to_bool_column JSON,  -- Storing frozen map of integers to boolean

    -- Combinations of Collections (Array of Struct, Maps with Lists, etc.)
    map_text_to_list_column JSON,        -- Map of text to list
    map_text_to_set_column JSON,         -- Map of text to set
    set_of_maps_column JSON,             -- Set of maps
    list_of_sets_column JSON,            -- List of sets

    -- Frozen Combinations (Frozen Collections)
    frozen_map_text_to_list_column JSON, -- Frozen map of text to list
    frozen_map_text_to_set_column JSON,  -- Frozen map of text to set
    frozen_set_of_maps_column JSON,      -- Frozen set of maps
    frozen_list_of_sets_column JSON,     -- Frozen list of sets

    -- Varint Column (BYTES for variable-length integers)
    varint_column BYTES(MAX)             -- Storing varint as bytes
) PRIMARY KEY(varchar_column);

-- Create a change stream for the table
CREATE CHANGE STREAM allstream
  FOR ALL OPTIONS (
    value_capture_type = 'NEW_ROW',
    retention_period = '7d'
  );
