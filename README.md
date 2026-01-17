# Databricks Declarative Pipeline: Brazilian Ecommerce Reference Architecture

This repository demonstrates a modern, Databricks **Declarative Pipeline** and **Unity Catalog**. It processes the Brazilian Ecommerce dataset through a multi-stage Medallion Architecture.

## Pipeline Visual
![Pipeline Graph](images/pipeline.png)<br>
*Figure 1: The end-to-end lineage showing Streaming Tables (Bronze/Silver/Gold) and Materialized Views.*

## Architecture Overview

The pipeline follows the Medallion design pattern, utilizing streaming tables for efficient, incremental processing and Unity Catalog for centralized governance.

### Data Flow Stages
1.  **Bronze (Ingestion):** Raw data is ingested from cloud storage into streaming tables. This stage maintains the raw fidelity of the source data.
2.  **Silver (Transformation & Quality):** Data is cleaned and refined. This layer implements pipeline expectations to enforce data quality and transforms raw records into normalized streaming tables.
3.  **Gold (Analytics):** A Star Schema implementation consisting of:
    * `order_items_fact`
    * `orders_dim`
    * `customers_dim`
4.  **Presentation Layer:** A Materialized View is used to calculate complex analytics, specifically identifying the **top 2 orders per customer** by total value.

## Tech Stack
* **Databricks Declarative Pipeline:** Declarative pipeline framework.
* **Unity Catalog:** For fine-grained data governance and lineage.
* **Delta Lake:** High-performance storage layer with ACID transactions.
* **SQL/Python:** Implementation languages for transformations.

## Getting Started

### Prerequisites
* Access to a Databricks Workspace with Unity Catalog enabled.
* The Brazilian Ecommerce dataset from data directory, uploaded to a S3 location.

### Deployment
1.  Clone this repository into your Databricks Workspace.
2.  Create an external location in Unity Catalog pointing to the raw data location.
3.  Refer `catalog_creation` notebook to create the Unity Catalog schema and tables.
4.  Open the pipeline folder in pipeline editor.
5.  Start the pipeline.

## Roadmap to Production
This is a reference architecture. To move this to a production-grade environment, the following enhancements are required:

* **Security & Governance:** Implement fine-grained Access Control Lists (ACLs) at the schema and table levels within Unity Catalog.
* **Performance Optimization:** Implement Delta Lake optimization techniques, including:
    * `Z-ORDER` clustering on frequently filtered columns (e.g., `customer_id`, `order_date`).
    * Predictive Optimization and Vacuum policies.
* **Observability:** Integrate a reporting tool (e.g., Databricks SQL Dashboards, Power BI, or Tableau) to visualize data quality metrics and business KPIs.
* **CI/CD:** Integrate with Databricks Asset Bundles (DABs) for automated deployments.