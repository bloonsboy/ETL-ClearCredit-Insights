/*LBE
Code for Stream and Task Workshop - Student Version
Interactive learning with exercises and questions

INSTRUCTIONS FOR STUDENTS:
==========================

1. PROGRESSIVE DIFFICULTY:
   - Section 1-2: BEGINNER (Basic setup and data ingestion)
   - Section 3-4: INTERMEDIATE (JSON parsing and streams)
   - Section 5-6: ADVANCED (Task orchestration and monitoring)
   - Section 7-8: EXPERT (Complex orchestration and optimization)
   - Section 9-10: MASTER LVL 100 BOSS (Data quality and PII protection)

2. LEARNING APPROACH:
   - Start with Section 1 and work sequentially
   - Each section builds on the previous one
   - Complete all YOUR CODE HERE sections
   - Answer all questions for deeper understanding
   - Use hints when needed - they're there to help!

3. HINTS SYSTEM:
   - All hints and solutions are in a separate file: snowflake_streams_tasks_hints.sql
   - Try to solve exercises first without looking at hints
   - When stuck, check the hints file for the corresponding exercise number
   - Hints are numbered to match exercise numbers (e.g., HINT 1.1, HINT 1.2, etc.)

4. ASSESSMENT:
   - Complete all sections for basic understanding
   - Master sections 1-6 for intermediate level
   - Complete sections 7-10 for advanced level
   - Bonus challenges for expert level

Good luck with your Snowflake learning !
*/

-- ===========================================
-- SECTION 1: SETUP AND PREPARATION (BEGINNER LEVEL)
-- ===========================================

-- Exercise 1.1: Create the necessary role and permissions
-- DIFFICULTY: BEGINNER

USE ROLE ACCOUNTADMIN;
SET myname = current_user();

CREATE ROLE IF NOT EXISTS Data_ENG; -- Create role Data_ENG

GRANT ROLE Data_ENG TO USER IDENTIFIER($myname); -- Grant role to current user

GRANT CREATE DATABASE ON ACCOUNT TO ROLE Data_ENG; --Grant create database permission

GRANT CREATE TASK ON ACCOUNT TO ROLE Data_ENG;
GRANT USAGE ON WAREHOUSE ORCHESTRATION_WH TO ROLE Data_ENG; -- Grant task execution permissions
GRANT OPERATE ON WAREHOUSE ORCHESTRATION_WH TO ROLE Data_ENG;

GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE Data_ENG; --Grant imported privileges on SNOWFLAKE database

-- Exercise 1.2: Create warehouse and database
-- DIFFICULTY: BEGINNER

CREATE WAREHOUSE IF NOT EXISTS Orchestration_WH
  WITH WAREHOUSE_SIZE = 'XSMALL'
  WAREHOUSE_TYPE = 'STANDARD'
  AUTO_SUSPEND = 300
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE; -- Create warehouse Orchestration_WH (XSMALL, auto-suspend 5 min)

GRANT USAGE, OPERATE ON WAREHOUSE Orchestration_WH TO ROLE Data_ENG; --Grant warehouse privileges to Data_ENG role

CREATE DATABASE IF NOT EXISTS Credit_card; -- Create database Credit_card

GRANT USAGE ON DATABASE Credit_card TO ROLE Data_ENG;
GRANT USAGE ON SCHEMA Credit_card.public TO ROLE Data_ENG;
GRANT CREATE SCHEMA ON DATABASE Credit_card TO ROLE Data_ENG; -- Grant database privileges to Data_ENG role

-- Switch to the new role and database
USE ROLE Data_ENG;
USE DATABASE Credit_card;
USE SCHEMA public;
USE WAREHOUSE Orchestration_WH;

USE ROLE Data_ENG;
USE DATABASE Credit_card;
USE SCHEMA PUBLIC;
USE WAREHOUSE Orchestration_WH;

-- ===========================================
-- SECTION 2: DATA INGESTION SETUP (BEGINNER LEVEL)
-- ===========================================

-- Exercise 2.1: Create staging infrastructure
-- DIFFICULTY: BEGINNER

CREATE OR REPLACE FILE FORMAT JSON_FMT
  TYPE = 'JSON'
  STRIP_OUTER_ARRAY = TRUE
  ALLOW_DUPLICATE = FALSE
  NULL_IF = ('NULL','');

CREATE OR REPLACE STAGE CC_STAGE
  FILE_FORMAT = JSON_FMT
  COMMENT = 'internal stage for CC stream lab'; -- Create internal stage CC_STAGE with JSON file format

CREATE OR REPLACE TABLE CC_TRANS_STAGING (
  raw VARIANT,
  ingested_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
); -- Create staging table CC_TRANS_STAGING with VARIANT column

-- Question 2.1: Why do we use a VARIANT column for JSON data?
-- Answer: We use VARIANT because it natively supports semi-structured data storage (JSON, AVRO, XML) in Snowflake,
-- allows fast querying with JSON functions (FLATTEN, : notation), and preserves the flexible structure of JSON objects.

-- Exercise 2.2: Create the data generation stored procedure
-- TODO: This is provided for you - study the Java code to understand how it works

CREATE OR replace procedure SIMULATE_KAFKA_STREAM(mystage STRING,prefix STRING,numlines INTEGER)
  RETURNS STRING
  LANGUAGE JAVA
  PACKAGES = ('com.snowflake:snowpark:latest')
  HANDLER = 'StreamDemo.run'
  AS
  $$
    import com.snowflake.snowpark_java.Session;
    import java.io.*;
    import java.util.HashMap;
    public class StreamDemo {
      public String run(Session session, String mystage,String prefix,int numlines) {
        SampleData SD=new SampleData();
        BufferedWriter bw = null;
        File f=null;
        try {
            f = File.createTempFile(prefix, ".json");
            FileWriter fw = new FileWriter(f);
	        bw = new BufferedWriter(fw);
            boolean first=true;
            bw.write("[");
            for(int i=1;i<=numlines;i++){
                if (first) first = false;
                else {bw.write(",");bw.newLine();}
                bw.write(SD.getDataLine(i));
            }
            bw.write("]");
            bw.close();
            return session.file().put(f.getAbsolutePath(),mystage,options)[0].getStatus();
        }
        catch (Exception ex){
            return ex.getMessage();
        }
        finally {
            try{
	            if(bw!=null) bw.close();
                if(f!=null && f.exists()) f.delete();
	        }
            catch(Exception ex){
	            return ("Error in closing:  "+ex);
	        }
        }
      }

      private static final HashMap<String,String> options = new HashMap<String, String>() {
        { put("AUTO_COMPRESS", "TRUE"); }
      };

      public static class SampleData {
      private static final java.util.Random R=new java.util.Random();
      private static final java.text.NumberFormat NF_AMT = java.text.NumberFormat.getInstance();
      String[] transactionType={"PURCHASE","PURCHASE","PURCHASE","PURCHASE","PURCHASE","PURCHASE","PURCHASE","PURCHASE","PURCHASE","PURCHASE","REFUND"};
      String[] approved={"true","true","true","true","true","true","true","true","true","true","false"};
      static {
        NF_AMT.setMinimumFractionDigits(2);
        NF_AMT.setMaximumFractionDigits(2);
        NF_AMT.setGroupingUsed(false);
      }

      private static int randomQty(int low, int high){
        return R.nextInt(high-low) + low;
      }

      private static double randomAmount(int low, int high){
        return R.nextDouble()*(high-low) + low;
      }

      private String getDataLine(int rownum){
        StringBuilder sb = new StringBuilder()
            .append("{")
            .append("\"element\":"+rownum+",")
            .append("\"object\":\"basic-card\",")
            .append("\"transaction\":{")
            .append("\"id\":"+(1000000000 + R.nextInt(900000000))+",")
            .append("\"type\":"+"\""+transactionType[R.nextInt(transactionType.length)]+"\",")
            .append("\"amount\":"+NF_AMT.format(randomAmount(1,5000)) +",")
            .append("\"currency\":"+"\"USD\",")
            .append("\"timestamp\":\""+java.time.Instant.now()+"\",")
            .append("\"approved\":"+approved[R.nextInt(approved.length)]+"")
            .append("},")
            .append("\"card\":{")
                .append("\"number\":"+ java.lang.Math.abs(R.nextLong()) +"")
            .append("},")
            .append("\"merchant\":{")
            .append("\"id\":"+(100000000 + R.nextInt(90000000))+"")
            .append("}")
            .append("}");
        return sb.toString();
      }
    }
}
$$;

-- Question 2.2: What does this stored procedure simulate?
-- Answer: It simulates the injection of JSON batches (as if Kafka messages were being pushed) into a Snowflake stage: it creates a JSON file containing 'numlines' transaction records and then uploads it to the specified stage.

-- Exercise 2.3: Test data generation
-- DIFFICULTY: BEGINNER

CALL SIMULATE_KAFKA_STREAM('@CC_STAGE','cc_sample_',100); -- Call SIMULATE_KAFKA_STREAM with appropriate parameters


LIST @CC_STAGE; -- List files in the stage to verify creation

COPY INTO CC_TRANS_STAGING(raw)
FROM @CC_STAGE
FILE_FORMAT = (format_name = 'JSON_FMT')
ON_ERROR = 'continue'; -- Copy data from stage to staging table

SELECT COUNT(*) AS staging_rows FROM CC_TRANS_STAGING; -- Check row count in staging table

-- ===========================================
-- SECTION 3: JSON DATA EXPLORATION (INTERMEDIATE LEVEL)
-- ===========================================

-- Exercise 3.1: Explore JSON structure
-- DIFFICULTY: INTERMEDIATE

SELECT raw:card:number::string AS card_number, ingested_at
FROM CC_TRANS_STAGING
LIMIT 20; -- Select card numbers from the JSON data

SELECT
  raw:transaction:id::number AS transaction_id,
  raw:transaction:amount::number AS amount,
  raw:transaction:currency::string AS currency,
  raw:transaction:approved::boolean AS approved,
  raw:transaction:type::string AS tx_type,
  raw:transaction:timestamp::string AS tx_timestamp
FROM CC_TRANS_STAGING
LIMIT 50; -- Parse and display transaction details (id, amount, currency, approved, type, timestamp)

SELECT
  raw:transaction:id::number AS transaction_id,
  raw:transaction:amount::number AS amount,
  raw:transaction:type::string AS tx_type
FROM CC_TRANS_STAGING
WHERE raw:transaction:amount::number < 600; -- Filter transactions with amount < 600

-- Question 3.1: What is the advantage of using VARIANT columns for JSON data?
-- Answer: VARIANT preserves the JSON structure, allows direct access to nested fields,
-- schema flexibility (schema-on-read), and native support for semi-structured functions.

-- Exercise 3.2: Create a normalized view
-- DIFFICULTY: INTERMEDIATE

CREATE OR REPLACE VIEW CC_TRANS_STAGING_VIEW AS
SELECT
  metadata$filename AS source_file,
  raw:element::number AS element,
  raw:object::string AS object_type,
  raw:transaction:id::number AS transaction_id,
  raw:transaction:type::string AS transaction_type,
  raw:transaction:amount::number AS amount,
  raw:transaction:currency::string AS currency,
  raw:transaction:timestamp::timestamp_ltz AS transaction_ts,
  raw:transaction:approved::boolean AS approved,
  raw:card:number::string AS card_number,
  raw:merchant:id::number AS merchant_id,
  ingested_at
FROM CC_TRANS_STAGING; -- Create view CC_TRANS_STAGING_VIEW with proper column mapping

SELECT * FROM CC_TRANS_STAGING_VIEW LIMIT 20;

-- Question 3.2: Why do we need to enable change tracking?
-- Answer: To detect new records/changes (CDC) and allow streams/tasks to process only deltas rather than the full dataset (efficiency and idempotence).

-- ===========================================
-- SECTION 4: STREAMS AND CHANGE DATA CAPTURE (INTERMEDIATE LEVEL)
-- ===========================================

-- Exercise 4.1: Create and test streams
-- DIFFICULTY: INTERMEDIATE

CREATE OR REPLACE STREAM CC_TRANS_STAGING_VIEW_STREAM ON VIEW CC_TRANS_STAGING_VIEW
  SHOW_INITIAL_ROWS = TRUE; -- Create stream CC_TRANS_STAGING_VIEW_STREAM on the view

SELECT * FROM CC_TRANS_STAGING_VIEW_STREAM LIMIT 50; -- Check initial stream content

SELECT COUNT(*) AS stream_rows FROM CC_TRANS_STAGING_VIEW_STREAM; -- Count records in the stream

-- Question 4.1: What does SHOW_INITIAL_ROWS=true do in stream creation?
-- Answer: It indicates that the stream should include existing rows (initial snapshot) as initial changes, not just future changes.

-- Exercise 4.2: Create analytical table
-- DIFFICULTY: INTERMEDIATE

CREATE OR REPLACE TABLE CC_TRANS_ALL (
  transaction_id NUMBER,
  transaction_type VARCHAR,
  amount NUMBER,
  currency VARCHAR,
  transaction_ts TIMESTAMP_LTZ,
  approved BOOLEAN,
  card_number VARCHAR,
  merchant_id NUMBER,
  source_file VARCHAR,
  loaded_at TIMESTAMP_LTZ default current_timestamp()
); -- Create table CC_TRANS_ALL with proper schema

INSERT INTO CC_TRANS_ALL (transaction_id, transaction_type, amount, currency, transaction_ts, approved, card_number, merchant_id, source_file)
SELECT transaction_id, transaction_type, amount, currency, transaction_ts, approved, card_number, merchant_id, source_file
FROM CC_TRANS_STAGING_VIEW_STREAM
WHERE metadata$action = 'INSERT' OR metadata$action IS NULL; -- Insert data from stream into analytical table

SELECT COUNT(*) AS analytical_count FROM CC_TRANS_ALL;
SELECT * FROM CC_TRANS_ALL ORDER BY loaded_at DESC LIMIT 20; -- Verify data in analytical table

-- Question 4.2: What is the difference between the staging table and analytical table?
-- Answer: The staging table retains raw semi-structured data (VARIANT JSON) as ingested; the analytical table is normalized, cleaned, and optimized for analysis/reporting.

-- ===========================================
-- SECTION 5: TASK ORCHESTRATION (ADVANCED LEVEL)
-- ===========================================

-- Exercise 5.1: Create your first task
-- DIFFICULTY: ADVANCED

CREATE OR REPLACE TASK GENERATE_TASK
  WAREHOUSE = Orchestration_WH
  SCHEDULE = 'USING CRON * * * * * UTC' 
  COMMENT = 'Task to simulate data generation every minute'
AS
  call SIMULATE_KAFKA_STREAM('@CC_STAGE','cc_auto_',10); -- Create task GENERATE_TASK with 1-minute schedule

DESCRIBE TASK GENERATE_TASK; -- Describe the task to see its definition

EXECUTE TASK GENERATE_TASK; -- Execute the task manually

RESUME TASK GENERATE_TASK; -- Resume the task to run on schedule

-- Question 5.1: What are the benefits of using tasks vs manual execution?
-- Answer: Automation, repeatability, orchestration (dependencies), reduction of human errors, scheduled execution, and integration with streams for near-real-time pipelines.

-- Exercise 5.2: Create data processing task
-- DIFFICULTY: ADVANCED

CREATE OR REPLACE TASK PROCESS_FILES_TASK
  WAREHOUSE = Orchestration_WH
  SCHEDULE = 'USING CRON */3 * * * * UTC'
  WHEN
    system$stream_has_data('CC_TRANS_STAGING_VIEW_STREAM') = true
AS

  COPY INTO CC_TRANS_STAGING(raw)
  FROM @CC_STAGE
  file_format = (format_name = 'JSON_FMT')
  on_error = 'continue'; -- Create task PROCESS_FILES_TASK with 3-minute schedule


EXECUTE TASK PROCESS_FILES_TASK; -- Execute task manually and verify results

RESUME TASK PROCESS_FILES_TASK; -- Resume the task

-- Exercise 5.3: Create data refinement task
-- DIFFICULTY: ADVANCED

CREATE OR REPLACE TASK REFINE_TASK
  WAREHOUSE = Orchestration_WH
  WHEN
    system$stream_has_data('CC_TRANS_STAGING_VIEW_STREAM') = true
as
  merge into CC_TRANS_ALL T
  using (
    select transaction_id, transaction_type, amount, currency, transaction_ts, approved, card_number, merchant_id, source_file
    from CC_TRANS_STAGING_VIEW_STREAM
  ) S
  on T.transaction_id = S.transaction_id
  when matched then update set
    transaction_type = S.transaction_type,
    amount = S.amount,
    currency = S.currency,
    transaction_ts = S.transaction_ts,
    approved = S.approved,
    card_number = S.card_number,
    merchant_id = S.merchant_id,
    loaded_at = current_timestamp()
  when not matched then insert (transaction_id, transaction_type, amount, currency, transaction_ts, approved, card_number, merchant_id, source_file)
    values (S.transaction_id, S.transaction_type, S.amount, S.currency, S.transaction_ts, S.approved, S.card_number, S.merchant_id, S.source_file);

execute task REFINE_TASK;

resume task REFINE_TASK;

-- Question 5.2: What does SYSTEM$STREAM_HAS_DATA() do?
-- Answer: It is a system function that returns TRUE if the specified stream contains unconsumed changes (i.e., there are deltas to process).

-- ===========================================
-- SECTION 6: MONITORING AND REPORTING (ADVANCED LEVEL)
-- ===========================================

-- Exercise 6.1: Monitor task execution
-- DIFFICULTY: ADVANCED
-- TODO: Create monitoring queries


select *
from table(information_schema.task_history())
order by scheduled_time desc
limit 50;

select *
from snowflake.account_usage.task_history
where task_name in ('GENERATE_TASK','PROCESS_FILES_TASK','REFINE_TASK')
order by scheduled_time desc
limit 50;

select *
from table(information_schema.task_history())
where name = 'REFINE_TASK'
order by scheduled_time desc
limit 20;


-- Question 6.1: What is the difference between INFORMATION_SCHEMA and ACCOUNT_USAGE?
-- Answer: INFORMATION_SCHEMA provides metadata at the current database/schema level (immediate), while ACCOUNT_USAGE is an audit/usage view at the account level, available in the SNOWFLAKE.ACCOUNT_USAGE schema, with some latency (a few minutes/hours) but global scope.

-- Exercise 6.2: Analyze data flow
-- DIFFICULTY: ADVANCED
-- TODO: Create queries to understand data flow

select 'staging' as table_name, count(*) as cnt from CC_TRANS_STAGING
union all
select 'analytical' as table_name, count(*) as cnt from CC_TRANS_ALL;

select max(transaction_ts) as latest_tx_ts from CC_TRANS_ALL;

select approved, count(*) as cnt, avg(amount) as avg_amount
from CC_TRANS_ALL
group by approved;

-- ===========================================
-- SECTION 7: ADVANCED ORCHESTRATION (EXPERT LEVEL)
-- ===========================================

-- Exercise 7.1: Create task dependencies
-- DIFFICULTY: EXPERT
-- TODO: Create a sequential task pipeline

create or replace task PIPELINE_ROOT
  warehouse = Orchestration_WH
  schedule = 'USING CRON */5 * * * * UTC' 
as
  call SIMULATE_KAFKA_STREAM('@CC_STAGE','cc_pipe_',20);

create or replace task PIPELINE_STEP_1
  warehouse = Orchestration_WH
  after PIPELINE_ROOT
as
  copy into CC_TRANS_STAGING(raw)
  from @CC_STAGE
  file_format = (format_name = 'JSON_FMT')
  on_error = 'continue';

create or replace task PIPELINE_STEP_2
  warehouse = Orchestration_WH
  after PIPELINE_STEP_1
as
  merge into CC_TRANS_ALL T
  using (
    select transaction_id, transaction_type, amount, currency, transaction_ts, approved, card_number, merchant_id, source_file
    from CC_TRANS_STAGING_VIEW
  ) S
  on T.transaction_id = S.transaction_id
  when not matched then insert (transaction_id, transaction_type, amount, currency, transaction_ts, approved, card_number, merchant_id, source_file)
    values (S.transaction_id, S.transaction_type, S.amount, S.currency, S.transaction_ts, S.approved, S.card_number, S.merchant_id, S.source_file);

resume task PIPELINE_ROOT;
resume task PIPELINE_STEP_1;
resume task PIPELINE_STEP_2;


-- Question 7.1: How do task dependencies work in Snowflake?
-- Answer: Tasks can be chained using the AFTER clause or the WHEN condition; a 'child' task runs after its parent if the parent succeeds. Snowflake orchestrates the execution order and allows parallel execution if multiple children exist.

-- Exercise 7.2: Parallel processing
-- DIFFICULTY: EXPERT
-- TODO: Create tasks that can run in parallel

create or replace task PAR_STEP_A
  warehouse = Orchestration_WH
  after PIPELINE_STEP_1
as

  insert into CC_TRANS_ALL (transaction_id, transaction_type, amount, currency, transaction_ts, approved, card_number, merchant_id, source_file)
  select transaction_id + 1000000000, transaction_type, amount, currency, transaction_ts, approved, card_number, merchant_id, source_file
  from CC_TRANS_STAGING_VIEW
  where transaction_id is not null
  limit 0; 

create or replace task PAR_STEP_B
  warehouse = Orchestration_WH
  after PIPELINE_STEP_1
as
  select count(*) from CC_TRANS_STAGING; 

resume task PAR_STEP_A;
resume task PAR_STEP_B;

-- ===========================================
-- SECTION 8: CLEANUP AND BEST PRACTICES (EXPERT LEVEL)
-- ===========================================

-- Exercise 8.1: Task management
-- DIFFICULTY: EXPERT
-- TODO: Properly manage task lifecycle

alter task GENERATE_TASK suspend;
alter task PROCESS_FILES_TASK suspend;
alter task REFINE_TASK suspend;
alter task PIPELINE_ROOT suspend;
alter task PIPELINE_STEP_1 suspend;
alter task PIPELINE_STEP_2 suspend;
alter task PAR_STEP_A suspend;
alter task PAR_STEP_B suspend;

show tasks;

-- Question 8.1: Why is it important to suspend tasks when not needed?
-- Answer: To avoid costs (warehouse consumption), unnecessary executions, production errors, and to control the order of pipeline resumption.

-- Exercise 8.2: Performance analysis
-- DIFFICULTY: EXPERT
-- TODO: Analyze the performance of your pipeline

select task_name, sum(execution_time) as total_exec_ms, count(*) as runs
from snowflake.account_usage.task_history
where task_name in ('REFINE_TASK','PROCESS_FILES_TASK','GENERATE_TASK')
group by task_name;

select 'staging' as t, count(*) from CC_TRANS_STAGING
union all
select 'analytical', count(*) from CC_TRANS_ALL;

select query_text, total_elapsed_time, execution_status
from snowflake.account_usage.query_history
where query_text ilike '%COPY INTO CC_TRANS_STAGING%' or query_text ilike '%MERGE into CC_TRANS_ALL%'
order by start_time desc
limit 20;
-- ===========================================
-- SECTION 9: COMPREHENSIVE DATA QUALITY CHECKS (MASTER LEVEL)
-- ===========================================

-- Exercise 9.1: Basic data quality validation
-- DIFFICULTY: INTERMEDIATE
-- TODO: Implement comprehensive data quality checks

select transaction_id, count(*) as cnt
from CC_TRANS_ALL
group by transaction_id
having count(*) > 1
order by cnt desc
limit 50;

select count(*) as negative_amounts
from CC_TRANS_ALL
where amount <= 0 or amount is null;

select
  sum(case when transaction_id is null then 1 else 0 end) as missing_tx_id,
  sum(case when card_number is null then 1 else 0 end) as missing_card
from CC_TRANS_ALL;

select card_number, count(*) from CC_TRANS_ALL
where card_number not rlike '^[0-9]+$'
group by card_number
limit 50;

select *
from CC_TRANS_ALL
where amount > 10000
order by amount desc
limit 50;

-- Exercise 9.2: Advanced data quality metrics
-- DIFFICULTY: ADVANCED
-- TODO: Create comprehensive data quality dashboard

create or replace table DQ_METRICS (
  metric_date date,
  metric_name varchar,
  metric_value number,
  details variant,
  recorded_at timestamp_ltz default current_timestamp()
);

insert into DQ_METRICS(metric_date, metric_name, metric_value, details)
select current_date(), 'completeness_transaction_id', count(*) - sum(case when transaction_id is null then 1 else 0 end), object_construct('total', count(*), 'missing_tx_id', sum(case when transaction_id is null then 1 else 0 end))
from CC_TRANS_ALL;

insert into DQ_METRICS(metric_date, metric_name, metric_value, details)
select current_date(), 'valid_amounts', count(*) - sum(case when amount <= 0 or amount is null then 1 else 0 end), object_construct('total', count(*), 'invalid_amounts', sum(case when amount <= 0 or amount is null then 1 else 0 end))
from CC_TRANS_ALL;

insert into DQ_METRICS(metric_date, metric_name, metric_value, details)
select current_date(), 'valid_currency', count(*) - sum(case when currency not in ('USD') or currency is null then 1 else 0 end), object_construct('total', count(*), 'invalid_currency', sum(case when currency not in ('USD') or currency is null then 1 else 0 end))
from CC_TRANS_ALL;

-- Exercise 9.3: Data quality monitoring and alerting
-- DIFFICULTY: EXPERT
-- TODO: Implement automated data quality monitoring

create or replace procedure PROC_RUN_DQ_CHECKS()
returns varchar
language sql
as
$$
begin
  insert into DQ_METRICS(metric_date, metric_name, metric_value, details)
  select current_date(), 'dq_snapshot_total', count(*), object_construct('note','snapshot') from CC_TRANS_ALL;
  return 'DQ checks run';
end;
$$;

create or replace task DQ_TASK
  warehouse = Orchestration_WH
  schedule = 'USING CRON 0 * * * * UTC' 
as
  call PROC_RUN_DQ_CHECKS();

resume task DQ_TASK;

-- ===========================================
-- SECTION 10: PII PROTECTION AND DATA MASKING (MASTER LEVEL)
-- ===========================================

-- Exercise 10.1: Identify PII in credit card data
-- DIFFICULTY: INTERMEDIATE
-- TODO: Identify and categorize PII fields

create or replace table PII_CLASSIFICATION (
  field_name varchar,
  sensitivity varchar,
  reason varchar,
  recommended_action varchar
);

insert into PII_CLASSIFICATION values
  ('card_number','HIGH','Card number identifies payment instrument','Masking, restricted access, encryption at rest'),
  ('transaction_id','MEDIUM','May identify transaction but not directly person','Pseudonymize or restrict access'),
  ('merchant_id','LOW','Business identifier','Role-based access');

-- Exercise 10.2: Implement data masking for PII
-- DIFFICULTY: ADVANCED
-- TODO: Create masked views for different user roles

create or replace view CC_TRANS_MASKED_ANALYST as
select
  transaction_id,
  transaction_type,
  amount,
  currency,
  transaction_ts,
  approved,
  case
    when current_role() in ('DATA_ENG','ANALYST_ROLE') then concat(substr(card_number,1,6),'******',substr(card_number,-4))
    else '****MASKED****'
  end as card_number_masked,
  merchant_id,
  source_file
from CC_TRANS_ALL;

create or replace view CC_TRANS_MASKED_AUDITOR as
select
  transaction_id,
  transaction_type,
  amount,
  currency,
  transaction_ts,
  approved,
  'AUDITOR_MASKED' as card_number_masked,
  merchant_id,
  source_file
from CC_TRANS_ALL;

create role if not exists ANALYST_ROLE;
grant role ANALYST_ROLE to user identifier($myname);
grant usage on database Credit_card to role ANALYST_ROLE;
grant usage on schema Credit_card.public to role ANALYST_ROLE;
grant select on view CC_TRANS_MASKED_ANALYST to role ANALYST_ROLE;

-- Exercise 10.3: Advanced PII protection strategies
-- DIFFICULTY: EXPERT
-- TODO: Implement advanced PII protection mechanisms

create or replace masking policy mask_card_number AS (val string) returns string ->
  case
    when current_role() in ('ANALYST_ROLE','DATA_ENG') then concat(substr(val,1,6),'******',substr(val,-4))
    else '****MASKED****'
  end;

alter table CC_TRANS_ALL modify column card_number set masking policy mask_card_number;

create or replace task RETENTION_TASK
  warehouse = Orchestration_WH
  schedule = 'USING CRON 0 3 * * * UTC' 
as
  delete from CC_TRANS_ALL where transaction_ts < dateadd(year, -3, current_timestamp());

resume task RETENTION_TASK;

create or replace procedure PROC_ANONYMIZE_CARD()
returns string
language sql
as
$$
begin
  update CC_TRANS_ALL
  set card_number = sha2(card_number,256)
  where card_number is not null;
  return 'Anonymization complete';
end;
$$;
-- Question 10.1: What are the key principles of PII protection in data systems?
-- Answer: Minimization (collect only what is necessary), encryption, masking, role-based restricted access, anonymization/pseudonymization, traceability (logs), limited retention, legal compliance.

-- Question 10.2: How would you implement GDPR compliance for this credit card data?
-- Answer: Document purposes, obtain legal basis (consent or contract execution), minimize data, pseudonymize/anonymize when possible, provide data subject rights (access, erasure), define DPO/processes, notify in case of breaches, maintain processing records, establish subcontractor agreements and technical/organizational measures (encryption, access control).

-- Question 10.3: What are the trade-offs between data utility and privacy protection?
-- Answer: The more you protect (mask/anonymize), the more you lose in granularity and analytical utility; retention and masking reduce risk but limit fine-grained analysis. Balance is key: default protection + dedicated environments for analysis (secure enclaves, synthetic data, differential privacy if necessary).
