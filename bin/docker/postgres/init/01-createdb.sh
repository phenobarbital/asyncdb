#!/bin/bash

# Immediately exits if any error occurs during the script
# execution. If not set, an error could occur and the
# script would continue its execution.
set -o errexit


# Creating an array that defines the environment variables
# that must be set. This can be consumed later via arrray
# variable expansion ${REQUIRED_ENV_VARS[@]}.
readonly REQUIRED_ENV_VARS=(
  "PG_DB_USER"
  "PG_DB_SPUSER"
  "PG_DB_PASSWORD"
  "PG_DB_DATABASE"
  "POSTGRES_USER"
  "POSTGRES_PASSWORD")


# Checks if all of the required environment
# variables are set. If one of them isn't,
# echoes a text explaining which one isn't
# and the name of the ones that need to be
check_env_vars_set() {
  for required_env_var in ${REQUIRED_ENV_VARS[@]}; do
    if [[ -z "${!required_env_var}" ]]; then
      echo "Error:
    Environment variable '$required_env_var' not set.
    Make sure you have the following environment variables set:
      ${REQUIRED_ENV_VARS[@]}
Aborting."
      exit 1
    fi
  done
}


# Performs the initialization in the already-started PostgreSQL
# using the preconfigured POSTGRE_USER user.


init_db() {
  psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
  CREATE DATABASE $PG_DB_DATABASE
    WITH OWNER = "$PG_DB_SPUSER"
         ENCODING = 'UTF8'
         TEMPLATE = template0
         LC_COLLATE = 'en_US.UTF-8'
         LC_CTYPE = 'en_US.UTF-8'
         CONNECTION LIMIT = -1;
  GRANT ALL PRIVILEGES ON DATABASE $PG_DB_DATABASE TO "$PG_DB_USER";
  GRANT ALL PRIVILEGES ON DATABASE $PG_DB_DATABASE TO "$PG_DB_SPUSER";
  GRANT ALL PRIVILEGES ON DATABASE $PG_DB_DATABASE TO "$POSTGRES_USER";
EOSQL
}

init_create_extensions() {
  psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname $PG_DB_DATABASE  <<-EOSQL
  CREATE EXTENSION btree_gin;
  CREATE EXTENSION btree_gist;
  CREATE EXTENSION hstore;
  CREATE EXTENSION tablefunc;
  CREATE EXTENSION "uuid-ossp";
  CREATE EXTENSION fuzzystrmatch;
EOSQL
}

init_users() {
  psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
     CREATE ROLE "$PG_DB_SPUSER" WITH SUPERUSER CREATEDB CREATEROLE LOGIN ENCRYPTED PASSWORD '$PG_DB_PASSWORD';
     CREATE ROLE "$PG_DB_USER" WITH NOSUPERUSER CREATEDB NOCREATEROLE LOGIN ENCRYPTED PASSWORD '$PG_DB_PASSWORD';
EOSQL
}

# Main execution:
# - verifies if all environment variables are set
# - runs the SQL code to create user and database
main() {
  check_env_vars_set
  init_users
  init_db
  init_create_extensions
}

# Executes the main routine with environment variables
# passed through the command line. We don't use them in
# this script but now you know 🤓
main "$@"
