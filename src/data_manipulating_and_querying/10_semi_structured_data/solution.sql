-- ============================================================================
-- Select Semi-Structured Data (XML and JSON)
-- Dialect: T-SQL (SQL Server)
-- ============================================================================

IF OBJECT_ID('dbo.pex_sample', 'U') IS NOT NULL DROP TABLE dbo.pex_sample;

CREATE TABLE dbo.pex_sample (
    id        INT           PRIMARY KEY,
    name      NVARCHAR(50)  NOT NULL,
    dept      NVARCHAR(50)  NOT NULL,
    json_data NVARCHAR(MAX) NOT NULL
);

INSERT INTO dbo.pex_sample (id, name, dept, json_data) VALUES
    (1, 'Alice', 'Engineering', '{"age":30,"skills":["Python","SQL"],"active":true}'),
    (2, 'Bob',   'Sales',       '{"age":25,"skills":["Excel"],"active":false}'),
    (3, 'Carol', 'Engineering', '{"age":35,"skills":["Java","Spark","SQL"],"active":true}');


-- ===== XML =====

-- 1. OPENROWSET — import XML data from file into table
SELECT *
FROM OPENROWSET(
    BULK 'C:\data\employees.xml',
    SINGLE_BLOB
) AS xmlfile;
-- returns raw XML content as varbinary; typically cast and shredded next


-- 2. FOR XML — retrieve relational data in XML format
SELECT id, name, dept
FROM dbo.pex_sample
FOR XML PATH('employee'), ROOT('employees');
-- <employees>
--   <employee><id>1</id><name>Alice</name><dept>Engineering</dept></employee>
--   <employee><id>2</id><name>Bob</name><dept>Sales</dept></employee>
--   <employee><id>3</id><name>Carol</name><dept>Engineering</dept></employee>
-- </employees>


-- 3. OPENXML — retrieve XML data in relational format
DECLARE @xml_doc XML = '
<employees>
  <employee id="1" name="Alice" dept="Engineering"/>
  <employee id="2" name="Bob"   dept="Sales"/>
</employees>';

DECLARE @hDoc INT;
EXEC sp_xml_preparedocument @hDoc OUTPUT, @xml_doc;

SELECT *
FROM OPENXML(@hDoc, '/employees/employee', 1)
WITH (
    id   INT          '@id',
    name NVARCHAR(50) '@name',
    dept NVARCHAR(50) '@dept'
);
-- id=1, Alice, Engineering
-- id=2, Bob,   Sales

EXEC sp_xml_removedocument @hDoc;


-- ===== JSON =====

-- 4. ISJSON — test whether a string contains valid JSON
SELECT
    ISJSON('{"key":"value"}')   AS valid,    -- 1
    ISJSON('not json')          AS invalid;  -- 0


-- 5. JSON_VALUE — extract a scalar value from a JSON string
SELECT name,
    JSON_VALUE(json_data, '$.age')    AS age,     -- 30, 25, 35
    JSON_VALUE(json_data, '$.active') AS active   -- true, false, true
FROM dbo.pex_sample;


-- 6. JSON_QUERY — extract an object or array from a JSON string
SELECT name,
    JSON_QUERY(json_data, '$.skills') AS skills
    -- ["Python","SQL"], ["Excel"], ["Java","Spark","SQL"]
FROM dbo.pex_sample;


-- 7. JSON_MODIFY — change a value in a JSON string
SELECT name,
    JSON_MODIFY(json_data, '$.age', 31) AS updated_json
    -- Alice's age changed from 30 to 31 in returned JSON
FROM dbo.pex_sample
WHERE id = 1;
