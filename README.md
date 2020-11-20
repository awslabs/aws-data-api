# AWS Data API's - Beta

AWS Data API's offer you the ability to replace traditional database back ends for your applications with simple HTTP API's. They offer the speed, scalability, reliability, and security of a sophisticated NOSQL platform, but with zero coding and no servers to manage. In seconds, you can create a new Data API Namespace that includes your data model, natural language search, and sophisticated data lineage tracking which is presented to your application as an HTTPS REST API.

Data API's can provide developers with a powerful new way to build applications, replacing complex database clusters, data modelling, and search integration with a simple to use, document oriented API. It also unifies your application data models with your data lake, allowing simple exports and direct queries gainst application data from the Glue Data Catalog and Amazon Athena.

Data API Features include:

* __Database Storage__
	* Data API’s provide both structured or document type storage through DynamoDB
	* For a given ‘table’ (called a Namespace in Data API’s), you store Data Items as well as a separate set of Metadata associated with Data Items
	* Data & Metadata can have schemas applied, allowing for simple ‘flat’ RDBMS type tables, or sophisticated document models. You can choose whether you allow your application developers to extend these schemas.
	* Master Data Management features around ItemMaster reconciliation
	* Optimistic Concurrency Control is supported, and can be configured as required by an Administrator
	* Soft deletion that supports restoration is supported, as are ‘tombstone’ deletes in support of ‘right to be forgotten’ requirements
* __Flexible Queries__
	* Data API’s provide native indexing of both Data and Metadata attributes
	* Reference graph searching
* __ElasticSearch Integration__
	* You can supply an ElasticSearch cluster which will be used to automatically index all Data & Metadata, and augment the query API
* __Data Lake Integration__
	* API Namespaces are automatically registered with AWS Glue, and you can write Athena queries against your Data
	* API Namespaces can be exported to Amazon S3 on demand
* __Streaming__
	* Every Data and Metadata store is preconfigured with Dynamo Update Streams, and provides API integration to create streaming clients
* __Data Graph__
	* You can supply a Gremlin endpoint, which enables arbitrary ‘References’ graphs which support data lineage tracking and any other type of data connections customers wish to make

Please click [here](../../wiki) for full Documentation.

### FAQ

__Q: Should Data API's be used to implement applications directly?__

Probably not, but it's up to you. In general, we expect that Data API's are responsible for the core requirements of managing the data you would normally store in a relational or NOSQL database, and then you build your application logic and presentation layer on top. However, if your application simply needs the ability to create/read/update/delete data without a large amount of logic, Data API's may be suitable for direct implementation from a client side dynamic web app.

__Q: Why wouldn't I use Data API's, and instead stick to my existing RDBMS architecture?__

Data API's allow you to manage both structured and semi-structured data models, including where you require schema validation or not. Schema validation for Resources will allow you to implement the types of data models typically implemented on an RDBMS, but rarely for more than 3 tables. If your data model requires tens of interlinked, 3NF tables, then Data API's will not enable you to leverage the referential integrity checking that you get from an RDBMS. Also, RDBMS's allow to create Views and Triggers, modify behaviour with Stored procedures, and so on. A comparison table is provided:

| Relational Feature | Supported by Data API's? |
| ------------------ | ------------------------ |
| Rows | Yes. Data API's can also support `lists` and `maps` in a `row`, resulting in the ability to manage a document oriented structure. By adding a Schema that only supports scalar types, you have a direct analogue to a RDBMS `row` |
| Primary Keys | Yes. Each Data API Namespace must have a Primary Key |
| Foreign Keys | Sort of. By requiring that a "Parent" Item include a "Child" attribute with a given structure, you can create child->parent integrity checking, and making the "Child" attribute mandatory creates parent->child checks. |
| Views | Not directly. Can be implemented with Glue Catalog Views against the linked tables |
| Joins | No, but you can use a Document structure to manage 1:M data structures that allow cross type queries |
| Column Filter Queries | Yes, you can use the `/find` API to query any Namespace on any Attribute. If any Attribute is indexed, then the first index is used |
| Arbitrary SQL | Not Directly. Available through Athena, in that each Data API Namespace/Stage is registered with the AWS Glue Catalog. Also, if you configure an ElasticSearch integration, you can use the full breadth of ElasticSearch syntax to query all Data API Namespaces. |
| Cost/Rule based optimiser | No. Athena will implement cost based optimisation when querying Data API data |
| Triggers | Can be implemented with [AWS Lambda to create Triggers](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Streams.Lambda.html)|
| Table Indexes | Yes, Data API's support up to 5 indexes on each of the Resource or Metadata tables. |
| Data Dictionary | Yes, supported through the `/namespaces` API, and for each Namespace, the ability to view the schema with `/namespace/schema/<schema type>`. |
| Grants | Implemented through IAM security policies for both DynamoDB as well as [OAuth2 support for API Gateway](https://docs.aws.amazon.com/apigateway/latest/developerguide/permissions.html) |
| ACID | __A__tomicity: Yes. Each Data API modification is independently processed and completely processed, or not at all.</br> __C__onsistency: No. Data API's don't support Transactions (yet) and all API requests are independently applied. However, you can implement pseudo-consistency by using non-relational nested object structures in data API's, which are consistently applied to the top level Resource or Metadata.</br> __I__solation: Not relevant because Data API's dont implement Transactions.</br> __D__urability: Yes, all changes are persisted to at least 2 AWS Availability Zones before the API request completes |
