#!/bin/sh
#
# Common shell functions and messages
#
# Author:
# Jesus Lara <jesuslarag@gmail.com>
#
# This library is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License as published by the Free Software Foundation; either
# version 2.1 of the License, or (at your option) any later version.
# This library is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# Lesser General Public License for more details.
# You should have received a copy of the GNU Lesser General Public
# License along with this library; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA

# library version
VERSION='1.2'
LOGFILE='/tmp/troc.log'
VERBOSE=0
HOST='8.8.8.8'

## programs
LOGGER=/usr/bin/logger

###### Message functions

# export colors
export NORMAL='\033[0m'
export RED='\033[1;31m'
export GREEN='\033[1;32m'
export YELLOW='\033[1;33m'
export WHITE='\033[1;37m'
export BLUE='\033[1;34m'

get_version()
{
	echo $VERSION;
}

function is_debug() {
    [ "$VERBOSE" -eq 1 ] && return 0 || return 1
}


str2lower()
{
  echo "$1" | awk '{print tolower($0)}'
}

str2upper()
{
  echo "$1" | awk '{print toupper($0)}'
}

# from: http://unix.stackexchange.com/questions/60653/need-to-improve-urlencode-function
urlencode()
{
  string=$1; format=; set --
  while case "$string" in "") false;; esac do
    tail=${string#?}
    head=${string%$tail}
    case $head in
      [-._~0-9A-Za-z]) format=$format%c; set -- "$@" "$head";;
      *) format=$format%%%02x; set -- "$@" "'$head";;
    esac
    string=$tail
  done
  printf "$format\\n" "$@"
}

## get user confirmation
confirm(){
        while true; do
                read -p "Are you sure (y/n)? " yn
                case $yn in
                        [Yy]* ) break;;
                        [Nn]* ) exit;;
                        * ) echo "Please answer (Y/yes or N/no)";;
                esac
        done
}

#  If we're running verbosely show a message, otherwise swallow it.
#
message()
{
    msg="$*"
      echo -e $msg >&2;
}

#
# send a message to a LOGFILE
#
logMessage() {
  scriptname=$(basename $0)
  if [ -f "$LOGFILE" ]; then
	echo "`date +"%D %T"` $scriptname : $@" >> $LOGFILE
  fi
}

log()
{
	APPNAME=`basename $0`"["$$"]"
	## APPNAME=$(str2upper $scriptname)
	MSG=$1
	${LOGGER} -t ${APPNAME} "$MSG"
	debug "* $MSG"
}

#
# display a green-colored message, only if global variable VERBOSE=True
#
info()
{
	message="$*"
	printf "$GREEN"
	printf "%s"  "$message" >&2;
	# tput sgr0 # Reset to normal.
	# echo -e "\e[0m"
	echo -e `printf "$NORMAL"`
}

#
# display a yellow-colored warning message in console
#
warning()
{
	message="$*"
	printf "$YELLOW"
	printf "%s" "$message" >&2;
    	#printf "$NORMAL"
	echo -e `printf "$NORMAL"`
    	logMessage "WARN: $message"
}

#
# display a blue-colored message
#
debug()
{
	message="$*"
	if [ $VERBOSE -eq 1 ]; then
		printf "$BLUE"
		printf "%s"  "$message" >&2;
		# tput sgr0 # Reset to normal.
		echo -e `printf "$NORMAL"`
    	fi
    logMessage "DEBUG: $message"
}

#
# display and error (red-colored) and return non-zero value
#
error()
{
	message="$*"
	scriptname=$(basename $0)
	printf "$RED"
	printf "%s"  "$scriptname: $message" >&2;
	echo -e `printf "$NORMAL"`
	logMessage "ERROR:  $message"
	return 1
}

usage_err()
{
	error "$*"
	exit 1
}

##### Network functions

is_connected()
{
    ping -q -c2 $HOST > /dev/null
    if [ $? -eq 0 ]; then
        return 0
    else
        return 1
    fi
}

get_ifaces()
{
  iface=$(cat /proc/net/dev | grep ':' | cut -d ':' -f 1 | tr '\n' ' ')
  echo $iface
}

first_iface()
{
	iface=$(ip addr show | grep "state UP" | awk '{print $2}' | cut -d ':' -f 1 | head -n1 | tr '\n' ' ')
	echo $iface
}

get_publicip()
{
ip=$(curl -s http://whatismyip.akamai.com/)
if [ "$?" -ne "0" ]; then
	return 1
else
   if expr "$ip" : '[0-9][0-9]*\.[0-9][0-9]*\.[0-9][0-9]*\.[0-9][0-9]*$' >/dev/null; then
	echo $ip
   else
      ip=$(curl -s https://wtfismyip.com/text)
      if expr "$ip" : '[0-9][0-9]*\.[0-9][0-9]*\.[0-9][0-9]*\.[0-9][0-9]*$' >/dev/null; then
        echo $ip
      else
        return 1
      fi
   fi
fi
return 0
}

get_local_ip()
{
	iface=$1
	ip=$(ip addr show ${iface} | awk '/inet / {print $2}' | head -n1 | cut -d '/' -f1 | tr -d '[[:space:]]')
	if [ -z "$ip" ]; then
 	echo ''
 	return 1
  else
     echo $ip
     return 0
  fi
}

get_ip()
{
 iface=$1
 ip=$(ifconfig ${iface}| awk '/inet addr:/ { sub(/addr:/, "", $2); print $2 }' | tr -d '[[:space:]]')
 if [ -z "$ip" ]; then
	echo ''
	return 1
 else
    echo $ip
    return 0
 fi
}

### get dns resolution
get_dns()
{
	if [ -s "/tmp/resolv.conf.auto" ]; then
	DNS1=$(cat /tmp/resolv.conf.auto | grep -e "^nameserver" | grep -v "127.0.0.1" | grep -v "8.8.8.8" | awk '{ print $2 }' | head -n1)
	if [ ! -n "${DNS1}" ]; then
		DNS1="8.8.8.8"
	fi
    else
    DNS1="8.8.8.8"
    fi
	export DNS1
	echo $DNS1
}

### TODO: check faster dns resolver
resolv_address(){
	if [ -z "$DNS1" ]; then
		DNS=$(get_dns)
	else
		DNS=$DNS1
	fi
    a=$1
        addr=$(dig @$DNS -4 +short +time=2 +nofail +search $1 | grep -E "^([0-9])")
		if [ $? -ne 0 ]; then
			addr=$(host $1 | awk '/has address/ { print $4 }')
			if [ $? -ne 0 ]; then
                addr=$(nslookup $1 $DNS | awk '/^Address/ { print $3 }' | grep -E "^([0-9])" | grep -v ':' | grep -v $DNS | grep -v '127.0.0.1')
            fi
        fi
        echo $addr
}

get_mac()
{
	iface=$1
	mac=$(ifconfig ${iface} | awk '/HWaddr/ { print tolower($5) }' | head -n1)
	if [ $? -eq 0 ]; then
	    echo $mac
            return 0
	fi
}

get_gateway()
{
    echo $(ip route | grep "default via" | awk '{ print $3 }')
}

mask2cdr()
{
   # Assumes there's no "255." after a non-255 byte in the mask
   local x=${1##*255.}
   set -- 0^^^128^192^224^240^248^252^254^ $(( (${#1} - ${#x})*2 )) ${x%%.*}
   x=${1%%$3*}
   echo $(( $2 + (${#x}/4) ))
}

cdr2mask()
{
   # Number of args to shift, 255..255, first non-255 byte, zeroes
   set -- $(( 5 - ($1 / 8) )) 255 255 255 255 $(( (255 << (8 - ($1 % 8))) & 255 )) 0 0 0
   [ $1 -gt 1 ] && shift $1 || shift
   echo ${1-0}.${2-0}.${3-0}.${4-0}
}


#### Processes functions

# get number of processes already running
num_processes()
{
	process=$1
	nump=$(ps | grep ash | grep -v grep | awk '{print $1}' | wc | awk '{ print $1}')
	if [ $? -eq 0 ]; then
	    echo $nump
	else
		echo 0
	fi
}

## get number of processes (except current)
check_orphans()
{
	process=$1
	nump=$(ps | grep ash | grep -v grep | awk '{print $1}' | grep -v $$ | wc | awk '{ print $1}')
	if [ $? -eq 0 ]; then
	    echo $nump
	    return 1
	else
		echo 0
		return 0
	fi
}

# check if process is already running
check_process()
{
	process=$1
	ps | grep ${process} | grep -v grep > /dev/null
	return $?
}

# kill a process that is already running
kill_process()
{
	process=$1
	check_process $process
	if [ $? -eq 0 ]; then
	  pid=$(ps | grep "$process" | grep -v grep | awk '{ print $1 }')
	  for p in $pid; do
		# checking first
		ps | grep $process | grep -v grep | grep $p > /dev/null
		debug "killing process $process with pid $p"
		kill -9 $p > /dev/null
	  done
	else
	  debug "no process with name $1"
	fi
	return 0
}

### kill orphans processes
kill_orphans()
{
	process=$1
	check_orphans $process
	if [ $? -ne 0 ]; then
	  pid=$(ps | grep "$process" | grep -v grep | grep -v $$ | awk '{ print $1 }')
	  for p in $pid; do
		# checking first
		ps | grep $process | grep -v grep | grep $p > /dev/null
		debug "killing process $process with pid $p"
		kill -9 $p > /dev/null
	  done
	else
	  debug "no process with name $1"
	fi
	return 0
}

### Debian functions

# return host distribution based on lsb-release
get_distribution()
{
	if [ -z $(which lsb_release) ]; then
		echo "lxc-tools error: lsb-release is required"
		exit 1
	fi
	lsb_release -s -i
}

# get codename (ex: wheezy)
get_suite()
{
	if [ -z $(which lsb_release) ]; then
		echo "lxc-tools error: lsb-release is required"
		exit 1
	fi
	lsb_release -s -c
}

get_ubuntu()
{
	cat /etc/apt/sources.list | grep -F "deb http:" | head -n1 |  awk '{ print $3}' | cut -d '/' -f1
}

# install package with no prompt and default options
install_package()
{
	message "installing package $@"
	#
	# Install the packages
	#
	DEBIAN_FRONTEND=noninteractive sudo apt --option Dpkg::Options::="--force-overwrite" --option Dpkg::Options::="--force-confold" --yes install "$@"
}

function package_list() {
    local list=$1
    x=0
    for i in ${list[*]}; do
        if is_debug; then
            echo "Package: $i";
        fi
				install_package "${i}"

        [ "$i" = "$x" ]
        x="$((x+1))"
    done
    if is_debug; then echo; fi
}

is_installed()
{
	pkg="$@"
	if [ -z "$pkg" ]; then
		echo `dpkg -l | grep -i $pkg | awk '{ print $2}'}`
	fi
	return 0
}
### network functions

ifdev() {
IF=(`cat /proc/net/dev | grep ':' | cut -d ':' -f 1 | tr '\n' ' '`)
}

firstdev() {
	if [ -z "$LAN_INTERFACE" ]; then
		ifdev
		LAN_INTERFACE=${IF[1]}
	fi
}


### domain info

define_domain()
{
	echo -n 'Please define a Domain name [ex: example.com]: '
	read _DOMAIN_
	if [ -z "$_DOMAIN_" ]; then
		message "error: Domain not defined"
		return 1
	else
		DOMAIN=$_DOMAIN_
	fi
}

get_hostname()
{
	if [ -z "$NAME" ]; then
		NAME=`hostname --short`
	fi
}

get_domain()
{
	if [ -z "$DOMAIN" ] || [ "$DOMAIN" = "auto" ]; then
		# auto-configure domain:
		_DOMAIN_=`hostname -d`
		if [ -z "$_DOMAIN_" ]; then
			define_domain
		else
			DOMAIN=$_DOMAIN_
		fi
	fi
}


# parsing YAML
# Based on https://gist.github.com/pkuczynski/8665367
parse_yaml() {
    local yaml_file=$1
    local prefix=$2
    local s
    local w
    local fs

    s='[[:space:]]*'
    w='[a-zA-Z0-9_.-]*'
    fs="$(echo @|tr @ '\034')"

    (
        sed -e '/- [^\“]'"[^\']"'.*: /s|\([ ]*\)- \([[:space:]]*\)|\1-\'$'\n''  \1\2|g' |

        sed -ne '/^--/s|--||g; s|\"|\\\"|g; s/[[:space:]]*$//g;' \
            -e "/#.*[\"\']/!s| #.*||g; /^#/s|#.*||g;" \
            -e "s|^\($s\)\($w\)$s:$s\"\(.*\)\"$s\$|\1$fs\2$fs\3|p" \
            -e "s|^\($s\)\($w\)${s}[:-]$s\(.*\)$s\$|\1$fs\2$fs\3|p" |

        awk -F"$fs" '{
            indent = length($1)/2;
            if (length($2) == 0) { conj[indent]="+";} else {conj[indent]="";}
            vname[indent] = $2;
            for (i in vname) {if (i > indent) {delete vname[i]}}
                if (length($3) > 0) {
                    vn=""; for (i=0; i<indent; i++) {vn=(vn)(vname[i])("_")}
                    printf("%s%s%s%s=(\"%s\")\n", "'"$prefix"'",vn, $2, conj[indent-1],$3);
                }
            }' |

        sed -e 's/_=/+=/g' |

        awk 'BEGIN {
                FS="=";
                OFS="="
            }
            /(-|\.).*=/ {
                gsub("-|\\.", "_", $1)
            }
            { print }'
    ) < "$yaml_file"
}

get_config() {
    local yaml_file="$1"
    local prefix="$2"
    eval "$(parse_yaml "$yaml_file" "$prefix")"
}

# render a template configuration file
# preserve formatting
# expand variables
render_template() {
  eval "echo \"$(cat $1)\""
}
