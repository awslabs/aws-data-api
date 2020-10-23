_AWS Internal Information_

aws-data-api-interest channel on slack

----
# AWS Data API's

AWS Data API's offer you the ability to create data oriented application API's that deliver the speed, scalability, reliability, and security of a sophisticated NOSQL platform with zero coding or configuration, and no servers to manage. In seconds, you can create a new Data API Namespace that includes strong data models, natural language search, and sophisticated data lineage tracking, presented as an HTTPS REST API.

Data API's can provide application developers with a powerful new way to build services, replacing complex back-end infrastructure of database clusters and data modelling with a simple to use, document oriented API. It also unifies your application data models with a data lake, offering native integration of your data without the need for periodic ETL processes.

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
