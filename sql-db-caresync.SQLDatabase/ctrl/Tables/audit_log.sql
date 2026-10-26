CREATE TABLE [ctrl].[audit_log] (
    [audit_id]          INT           IDENTITY (1, 1) NOT NULL,
    [adf_run_id]        VARCHAR (100) NOT NULL,
    [table_id]          INT           NOT NULL,
    [stage]             VARCHAR (20)  NOT NULL,
    [status]            VARCHAR (20)  NOT NULL,
    [records_written]   INT           NULL,
    [start_time]        DATETIME2 (7) NULL,
    [end_time]          DATETIME2 (7) NULL,
    [created_timestamp] DATETIME2 (7) DEFAULT (sysutcdatetime()) NOT NULL,
    PRIMARY KEY CLUSTERED ([audit_id] ASC),
    CHECK ([stage]='SILVER_TO_GOLD' OR [stage]='SOURCE_TO_SILVER'),
    CHECK ([status]='IN_PROGRESS' OR [status]='FAILED' OR [status]='SUCCESS'),
    FOREIGN KEY ([table_id]) REFERENCES [ctrl].[table_config] ([table_id])
);


GO

