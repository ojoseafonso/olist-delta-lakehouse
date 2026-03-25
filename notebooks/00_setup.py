# Databricks notebook source
spark.sql("CREATE CATALOG IF NOT EXISTS olist")
spark.sql("CREATE SCHEMA IF NOT EXISTS olist.bronze")
spark.sql("CREATE SCHEMA IF NOT EXISTS olist.silver")
spark.sql("CREATE SCHEMA IF NOT EXISTS olist.gold")