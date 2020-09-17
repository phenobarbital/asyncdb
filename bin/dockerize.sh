
#!/bin/bash
# ================================================================================
# Docker Installer: Build and configure an docker environment
# for use in TROC Navigator
#
# Copyright © 2020 Jesús Lara Giménez (phenobarbital) <jesuslarag@gmail.com>
# Version: 0.1
#
#    Developed by Jesus Lara (phenobarbital) <jesuslara@phenobarbital.info>
#
#    License: GNU GPL version 3  <http://gnu.org/licenses/gpl.html>.
#    This is free software: you are free to change and redistribute it.
#    There is NO WARRANTY, to the extent permitted by law.
# ================================================================================


binpath="$(pwd)"
libpath="${binpath}/lib"
libarepa="${libpath}/libarepa.sh"
templater="${libpath}/templater.sh"
parse_yaml="${libpath}/parse_yaml.sh"
dockerdir="${binpath}/docker/"

# get library utility
source $libarepa
source $parse_yaml

# declaring verbose (debug)
VERBOSE=1
SUITE=`get_ubuntu`

# read yaml file
get_config "conf/config.yml"

### main execution program ###

if [ "$(id -u)" != "0" ]; then
   error "==== must be run as root ====" >&2
   exit 1
fi

usage()
{
	echo "Usage: $(basename $0) [--install] [--configure] [--launch] [--restoredb] [--debug] [-h|--help]"
    return 1
}

help()
{
	usage
cat <<EOF
This script is a helper to install a Debian GNU/Linux Server-Oriented
Automate, Secure and easily install a Debian Enterprise-ready Server/Workstation
Options:
  -n, --hostname             specify the name of the debian server
  -r, --role                 role-based script for running in server after installation
  -D, --domain               define Domain Name
  -l, --lan                  define LAN Interface (ej: eth0)
  --packages                 Extra comma-separated list of packages
  --debug                    Enable debugging information
  Help options:
      --help     give this help list
      --usage	 Display brief usage message
      --version  print program version
EOF
	echo ''
	get_version
	exit 1
}

INSTALL=0
INSTALL_DOCKER=0
CONFIGURE=0
LAUNCH=0
START=0
STOP=0
ENVIRONMENT=0
PERMISSION=0
RESTOREDB=0

# processing arguments
ARGS=`getopt -n$0 -u -a -o r:n:m:d:l:h --longoptions debug,usage,verbose,permission,restoredb,version,help,install,install-docker,configure,launch,stop,environment,start,stop -- "$@"`
eval set -- "$ARGS"

while [ $# -gt 0 ]; do
	case "$1" in
    --install)
        INSTALL=1
        INSTALL_DOCKER=1
        ;;
    --install-docker)
        INSTALL_DOCKER=1
        ;;
    --configure)
        CONFIGURE=1
        ;;
    --launch)
        LAUNCH=1
        ;;
    --environment)
       ENVIRONMENT=1
        ;;
    --start)
        START=1
        ;;
    --stop)
        STOP=1
        ;;
    --debug)
        DEBUG='true'
        ;;
    --permission)
        PERMISSION=1
        ;;
    --restoredb)
        RESTOREDB=1
        ;;
    --verbose)
        VERBOSE=1
        ;;
    --version)
  get_version
  exit 0;;
    -h|--help)
        help
        exit 1
        ;;
    --)
        break;;
    -?)
        usage_err "unknown option '$1'"
        exit 1
        ;;
    *)
        usage
        exit 1
        ;;
esac
shift
done


install_env()
{
  if [ -z "${network_interface}" ]; then
    iface=$(first_iface)
  else
    iface=${network_interface}
  fi
  debug "Getting this local interface: ${iface} with IP ${ip}"
  ip=$(get_local_ip ${iface})
  info " Starting: Configure Hosts "
  CHANGES=$(egrep ${ip} /etc/hosts | tail)
  echo $CHANGES
  echo $CHANGES
  if [ -z "$CHANGES" ]; then
     info "Applying Hosts changes"
cat <<EOF >> /etc/hosts

# Navigator-Next Docker Hosts
EOF

  x=0
  for i in ${hosts[*]}; do
cat <<EOF >> /etc/hosts
# hosts
${ip} ${i}.${network_domain}
EOF
     [ "$i" = "$x" ]
     x="$((x+1))"
  done

  fi

 # add current user to docker group
 [ $SUDO_USER ] && current_user=$SUDO_USER || current_user=`whoami`
 adduser $current_user docker

 # install packages
 info ":::: Installing dependencies"
 package_list "${install_dependencies[*]}"

 info "Get Docker key:"
 curl -fsSL ${install_key} | sudo apt-key add -

 info "Adding Docker Repository:"
 sudo add-apt-repository "deb [arch=amd64] ${install_repository} ${SUITE} stable"

 sudo apt update

 info "Installing Docker: "
 package_list "${install_docker[*]}"
}

install_docker()
{
  info "Get current Docker Images:"
  x=0
  for i in ${docker_images[*]}; do
      if is_debug; then
          debug "Docker Image: $i";
      fi
      docker pull "${i}"
      [ "$i" = "$x" ]
      x="$((x+1))"
  done
}

restore_db()
{
  warning "Will start to restore Navigator database, this can last a very long time."

  cd ${dockerdir}
  local docker_name=$(docker ps | grep ${postgres_image} | awk '{ print $1 }')
  local BACKUP="/opt/${postgres_PG_DATABASE}.backup"
  DB_USER="postgres"
  docker exec -i "${docker_name}" psql -v ON_ERROR_STOP=1 --username "$DB_USER" --dbname $postgres_PG_DATABASE  <<-EOSQL
   ALTER SYSTEM SET autovacuum_vacuum_scale_factor = 0.1;
   ALTER SYSTEM SET autovacuum = off;
   SELECT pg_reload_conf();
EOSQL
  docker exec -i "${docker_name}" pg_restore --no-acl --no-owner -j ${postgres_restore_cpus} --username "$DB_USER" --dbname $postgres_PG_DATABASE --format=c $BACKUP
  docker exec -i "${docker_name}" psql -v ON_ERROR_STOP=1 --username "$DB_USER" --dbname $postgres_PG_DATABASE  <<-EOSQL
  ALTER SYSTEM RESET autovacuum;
  ALTER SYSTEM RESET autovacuum_vacuum_scale_factor;
  SELECT pg_reload_conf();
EOSQL

restore_db_permissions

info "Finished database restore."
}


restore_db_permissions()
{
  cd ${dockerdir}
  local docker_name=$(docker ps | grep ${postgres_image} | awk '{ print $1 }')
  local BACKUP="/opt/${postgres_PG_DATABASE}.backup"
  DB_USER="postgres"
  info "Repairing permissions over database in docker name ${docker_name}: "

  docker exec -i "${docker_name}" psql -v ON_ERROR_STOP=1 --username "$DB_USER" --dbname $postgres_PG_DATABASE  <<-EOSQL
  ALTER DEFAULT PRIVILEGES IN SCHEMA public
  GRANT SELECT ON TABLES TO PUBLIC;
EOSQL

  # permissions on schemas
  for schema in $(echo "SELECT schemaname FROM pg_stat_user_tables GROUP BY schemaname;" | docker exec -i "${docker_name}" psql -v ON_ERROR_STOP=1 --username "$DB_USER" -A -t ${postgres_PG_DATABASE});
  do
     echo "GRANT ALL ON SCHEMA ${schema} TO \"${postgres_PG_SPUSER}\";"
     echo "ALTER SCHEMA ${schema} OWNER TO ${postgres_PG_USER};" | psql -v ON_ERROR_STOP=1 --username "$DB_USER" --dbname ${postgres_PG_DATABASE}
  done

  # permissions on tables
  for table in $(echo "SELECT schemaname || '.' || relname FROM pg_stat_user_tables;" | docker exec -i "${docker_name}" psql -v ON_ERROR_STOP=1 --username "$DB_USER" -A -t ${postgres_PG_DATABASE});
  do
      echo "GRANT ALL ON TABLE ${table} TO \"${postgres_PG_SPUSER}\";"
      echo "GRANT ALL ON TABLE ${table} TO \"${postgres_PG_USER}\";"
      echo "ALTER TABLE ${table} OWNER TO \"${postgres_PG_USER}\";" | docker exec -i "${docker_name}" psql -v ON_ERROR_STOP=1 --username "$DB_USER" --dbname $postgres_PG_DATABASE
  done

  # permissions on views
  for view in $(echo "SELECT schemaname || '.' || viewname from pg_catalog.pg_views WHERE schemaname != 'pg_catalog';" | psql -v ON_ERROR_STOP=1 --host 127.0.0.1 --username "$DB_USER" -A -t ${postgres_PG_DATABASE});
  do
     echo "ALTER VIEW ${view} OWNER TO ${postgres_PG_USER};" | psql -v ON_ERROR_STOP=1 --host 127.0.0.1 --username "$DB_USER" --dbname ${postgres_PG_DATABASE}
  done
}

config_docker()
{
  if [ -z "${network_interface}" ]; then
    iface=$(first_iface)
  else
    iface=${network_interface}
  fi
  LOCAL_IP=$(get_local_ip ${iface})
  # creation of docker-compose
  debug "Creating Docker Compose File: "
  render_template "${dockerdir}/docker-compose.yml.tmpl" > ${dockerdir}/docker-compose.yml
  # postgres Dockerfile
  render_template "${dockerdir}/postgres/Dockerfile.tmpl" > ${dockerdir}/postgres/Dockerfile

  configure_environment

  info "Creation of environment on ${conf_basedir}: "
  mkdir -p "${conf_basedir}"

  x=0
  for i in ${conf_volumes[*]}; do
      mkdir -p "${conf_basedir}/${i}"
      [ "$i" = "$x" ]
      x="$((x+1))"
  done

  # configure logging
  chown 70:70 ${postgres_logdir} -fR
  # building the containers
  cd ${dockerdir}
  x=0
  for i in ${docker_containers[*]}; do
      if is_debug; then
          debug "  == Docker Container: $i";
      fi
      docker-compose build "${i}"
      if [ ${docker_autostart} == 'true' ]; then
        # check first if docker is running
        compose_image=$(docker-compose -p docker ps | grep "Up" | grep "${i}" | awk '{print $1}')
        debug $compose_image
        debug $i
        if [[ -z $(docker ps | grep "${compose_image}") ]]; then
          #docker-compose up -d "${i}"
          echo "docker-compose up -d ${i}"
        else
          warning " == This Docker is already running: $i"
        fi
      fi
      [ "$i" = "$x" ]
      x="$((x+1))"
  done

  info " = Finish configuration."
}

launch_docker()
{
  info "Launching Docker containers: ..."
  cd ${dockerdir}
  for i in ${docker_containers[*]}; do
      if is_debug; then
          debug "  == Launch Docker Container: $i";
      fi
      docker-compose up -d "${i}"
      [ "$i" = "$x" ]
      x="$((x+1))"
  done
}


stop_docker()
{
  info "Stopping Docker containers: ..."
  cd ${dockerdir}
  for i in ${docker_containers[*]}; do
      if is_debug; then
          debug "  == Stop on Docker Container: $i";
      fi
      docker-compose stop "${i}"
      [ "$i" = "$x" ]
      x="$((x+1))"
  done
}

configure_environment()
{
  # re-configure environment
  debug "Creating ENV files for Navigator and Query API: "
  if [ -z "${network_interface}" ]; then
    iface=$(first_iface)
  else
    iface=${network_interface}
  fi
  LOCAL_IP=$(get_local_ip ${iface})

  DIR_API="${api_source}/env"
  DIR_NAV="${navigator_source}/env"
  if [[ ! -d "$DIR_API" ]]; then
    debug "Creating directory for .env environment files"
    mkdir -p $DIR_API
    mkdir -p $DIR_NAV
  fi
  # copying environment into ENV directory
  render_template "${dockerdir}/env.tmpl" > ${DIR_API}/.env
  render_template "${dockerdir}/env.tmpl" > ${DIR_NAV}/.env
}

main()
{
  if [ "$INSTALL" -eq 1 ]; then
    install_env
  fi
  if [ "$INSTALL_DOCKER" -eq 1 ]; then
    install_docker
  fi
  if [ "$CONFIGURE" -eq 1 ]; then
   config_docker
  fi
  if [ "$LAUNCH" -eq 1 ]; then
   launch_docker
  fi
  if [ "$ENVIRONMENT" -eq 1 ]; then
   configure_environment
  fi
  if [ "$RESTOREDB" -eq 1 ]; then
   restore_db
  fi
  if [ "$PERMISSION" -eq 1 ]; then
   restore_db_permissions
  fi
  if [ "$STOP" -eq 1 ]; then
   stop_docker
  fi
}

# = end = #
main
info "= All done. ="
exit 0
