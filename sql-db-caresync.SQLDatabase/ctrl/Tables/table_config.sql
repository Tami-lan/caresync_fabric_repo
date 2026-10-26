CREATE TABLE [ctrl].[table_config] (
    [table_id]           INT           IDENTITY (1, 1) NOT NULL,
    [source_system]      VARCHAR (50)  NOT NULL,
    [source_schema_name] VARCHAR (100) NULL,
    [source_table_name]  VARCHAR (200) NULL,
    [source_file_format] VARCHAR (20)  NULL,
    [source_path]        VARCHAR (500) NULL,
    [bronze_table_name]  VARCHAR (200) NULL,
    [silver_table_name]  VARCHAR (200) NULL,
    [gold_table_name]    VARCHAR (200) NULL,
    [load_type]          VARCHAR (20)  NOT NULL,
    [watermark_column]   VARCHAR (100) NULL,
    [merge_keys]         VARCHAR (200) NULL,
    [stage]              VARCHAR (20)  NOT NULL,
    [is_active]          BIT           DEFAULT ((1)) NOT NULL,
    [created_timestamp]  DATETIME2 (7) DEFAULT (sysutcdatetime()) NOT NULL,
    [updated_timestamp]  DATETIME2 (7) DEFAULT (sysutcdatetime()) NOT NULL,
    PRIMARY KEY CLUSTERED ([table_id] ASC),
    CHECK ([load_type]='MERGE' OR [load_type]='APPEND' OR [load_type]='FULL'),
    CHECK ([stage]='SILVER_TO_GOLD' OR [stage]='SOURCE_TO_SILVER')
);


GO

