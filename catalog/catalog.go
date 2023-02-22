package catalog

const SchemaSQL = `CREATE SCHEMA IF NOT EXISTS ptor;`
const TableSQL = `CREATE TABLE ptor.worker_%d(id bigint primary key, t char(8192), last_update timestamp without time zone default (now() at time zone 'UTC'));`
