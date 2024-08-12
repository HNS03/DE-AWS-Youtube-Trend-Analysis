# Data Engineering YouTube Analysis Project (AWS)

## Overview

This project seeks to securely manage, streamline, and analyze structured and semi-structured YouTube video data by focusing on video categories and trending metrics.

## Project Workflow
1. **Data Ingestion** — Develop a mechanism to collect data from various sources.
2. **ETL System** — Transform raw data into the required format.
3. **Data Lake** — Establish a centralized repository to store data from multiple sources.
4. **Scalability** — Ensure the system can scale as the volume of data increases.
5. **Cloud** — Utilize AWS for processing large volumes of data that cannot be handled locally.
6. **Reporting** — Create a dashboard to answer the questions we posed earlier.

## AWS Services Used
1. **Amazon S3**: Amazon S3 is an object storage service offering scalable manufacturing, high data availability, security, and performance.
2. **AWS IAM**: AWS Identity and Access Management (IAM) allows secure management of access to AWS services and resources.
3. **QuickSight**: Amazon QuickSight is a scalable, serverless business intelligence (BI) service powered by machine learning, designed for the cloud.
4. **AWS Glue**: AWS Glue is a serverless data integration service that simplifies data discovery, preparation, and combination for analytics, machine learning, and application development.
5. **AWS Lambda**: AWS Lambda is a computing service that enables developers to execute code without the need for server management.
6. **AWS Athena**: Amazon Athena is an interactive query service for S3 that allows querying data directly in S3 without needing to load it.

## Dataset Information
This Kaggle dataset includes statistics (in CSV format) on daily popular YouTube videos collected over several months. Each day features up to 200 trending videos across various locations, with separate files for each region. The dataset contains details such as video title, channel title, publication time, tags, views, likes, dislikes, description, and comment count. Additionally, there is a `category_id` field, which varies by region, included in the JSON file associated with each area.

https://www.kaggle.com/datasets/datasnaek/youtube-new

## Architecture Diagram
<img src="architecture.jpeg">