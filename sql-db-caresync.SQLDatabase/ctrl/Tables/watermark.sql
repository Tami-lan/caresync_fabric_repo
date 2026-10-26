CREATE TABLE [ctrl].[watermark] (
    [table_id]               INT           NOT NULL,
    [watermark_value]        VARCHAR (200) NULL,
    [last_updated_timestamp] DATETIME2 (7) DEFAULT (sysutcdatetime()) NOT NULL,
    PRIMARY KEY CLUSTERED ([table_id] ASC),
    FOREIGN KEY ([table_id]) REFERENCES [ctrl].[table_config] ([table_id])
);


GO

