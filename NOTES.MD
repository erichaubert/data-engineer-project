#### **Requirements Gaps:**
* What is the expected behavior for invalid data?
* Is it a known limitation that revenue/budget is being double counted since multiple production companies are associated with a single movie budget?
* Will this data be coming in as a full set? each month?  Will it be insert only or are corrections possible?
* Is the raw data needed to be in any 'live' datasource?  At the moment my data model is very flat to meet the basic requirements.

#### **Design Discussion:**
 I picked spark due to the simplicity of getting the code up and running quickly.  I already started to feel the pain of using spark for such simple transforms.  My cycle time was getting longer and longer due to the amount of time spark takes to perform such simple actions. In the past, I have replaced such simple jobs with direct CSV reads that write out using the parquet library directly.  I have seen jobs that take 4 minutes to process 18kb of data drop to sub second due to replacing the spark code.  Of course all of this depends on the scope of the program and how often it is touched.  Additionally, spark loves to eat exceptions and problems with your data.  By not using it, I sleep better at night for mission critical information. I know a passing jobs means all the data was processed and not dropped on the floor like spark tends to do.  Additionally, the scala collection API are so robust that you effectively have almost identical code in many cases because most transforms are actually pretty simple in practice.  

Parsing & Scrubbing - The data has some issues with it.  Some of the rows break rules about CSV escaping newline characters.  I chose to drop these rows for the puropses of this exercise.  I would probably scrub and validate the data first if this were a production job in conjunction with the changes above.

I would add more functional aspects to the design of the code.  I would use named Dataset definitions for each of the tables to enforce schema and use those stronger types to avoid passing fluffy dataframes all over the place.

See datadiagram.png for the data model.
I have a very flat data model since the minimum requirements are satisfied with several simple rollup tables.
Each table  
 
#### **Next Steps:**
My next steps would be to create a simple postgres with flyway.  I would add a docker-compose or something similar to launch a postgres.  I would split the project into a multi module project of `etl`, `api`, and `test`.
My test would call flyway migration on the api endpoint to stage the tables.  The test would call the job code to create the parquet artifacts.  I would add features to the job to load the data into postgres form the parquet files(then probably push the parquet to S3 for archiving).
Since my data model is very flat with no need for joins, the API implementations become very simple with some basic postgres queries.

#### **API/Storage Considerations:**
If the data is quite large(>1TB), I would stick with spark to process it in some batch job.  My artifacts would be pushed to S3. From there, data could still be loaded into postgres if the cardinality of the rollup is low enough.  If the dataset is huge, I might use snowflake to execute the larger queries.

If the data is not large, I would stick with postgres for storage in addition to S3 since archiving is very cheap.  The API would probably handle both the loading & serving of the API since the memory pressure should be quite low.  If it was a problem, I would use AWS batch or something similar to run the batch jobs.  A lambda might even be sufficient if the data is small enough.

The data size determines the needs for storage scaling above(postgres vs snowflake).  If the API is called heavily, it should horizontally scale easily since this is all read only from the API.  Redis caching would save a significant amount of query load but keep a simple architecture.   

#### **Monitoring:**
I assume logs are already being saved in a cloud solution.  I would use metrics liek StatsD/grafana etc to track job times and ingestion counts.  I prefer to put important business metrics on a tightly controlled event bus(like kinesis) to avoid any interruptions.

#### **Failures/Recovery:**
Jobs Failures should not impact the currently served data.  Depending on the storage layer and insert vs upsert requirements, I might partition the data in the storage layer and do an atomic swap to the new data in a single transaction.  The job would not commit any data until after all parquet files are completely generated.  At that time, we could put the API to the latest jobTimeStamp partition and flush any caches to atomically swap to the new data.  Old data could be retained or exposed on the api using additional query params if the use case exists.
Recovery is likely manual for the batch job unless common situations occur like network timeouts etc. The API will will only degrade by having older data but will still function.

#### **Security/Auth:**
I would start with a JWT auth model to avoid centralized auth server calls.  The downside is the ability to remove a user's access in 'an instant'.  This keeps the api independent of downstream failures outside of service owned data sources.
This data appears to not have GDPR/PII concerns.  If those existed, we would need to be more judicious with the archiving of data and how we do deletes. Deleting a single row from a parquet file can be a pain, but it could be a viable option depending on the access patterns.

Additional considerations might be better addressed in conversation as the permutations of architecture are going to explode quickly :)

 