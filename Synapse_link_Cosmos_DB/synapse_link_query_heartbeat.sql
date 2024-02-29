IF (NOT EXISTS(SELECT * FROM sys.credentials WHERE name = 'synapse-course-cosmos-db-sam'))
    CREATE CREDENTIAL [synapse-course-cosmos-db-sam]
    WITH IDENTITY = 'SHARED ACCESS SIGNATURE', SECRET = 'Kc04KVFHQi9sCzdDlscLcsJFvrbw2M9arrNvoL5xhepPwxjH9O2im8jj2Enyg2vpzvgcFXI6Fo28ACDbzMkfJA=='
GO

SELECT TOP 100 *
FROM OPENROWSET(​PROVIDER = 'CosmosDB',
                CONNECTION = 'Account=synapse-course-cosmos-db-sam;Database=nyctaxidb',
                OBJECT = 'Heartbeat',
                SERVER_CREDENTIAL = 'synapse-course-cosmos-db-sam'
) AS [Heartbeat]
