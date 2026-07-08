-- Second logical database in the shared Postgres instance, for Apache
-- Polaris's own relational-jdbc persistence (catalog metadata, not
-- platform business metadata). See Roadmap.md "Catalog" decision.
create database polaris_db;
