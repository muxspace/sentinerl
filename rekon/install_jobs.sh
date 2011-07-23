#!/bin/bash
# Author: Brian Lee Yung Rowe
# Date: 2001.07.21

do_exit()
{
  echo $1
  exit $2
}

do_usage()
{
  do_exit "Usage: $0 [-aejdv] [-E erlang_dir] [host:port]" $1
}

content_type()
{
  case $1 in
    go | *.html) content_type="text/html";;
    *.js) content_type="application/javascript";;
    *.css) content_type="text/css";;
    *.png) content_type="image/png";;
    *.gif) content_type="image/gif";;
    *.template) content_type="application/x-sammy-template";;
    *) content_type="text/plain";;
  esac
  echo $content_type
}


do_install_jobs()
{
  echo "Installing jobs"
  riak_url="http://$node/riak/rekon.jobs"
  base_dir="`dirname $0`/jobs"
  for f in $(ls $base_dir)
  do
    [ -n "$verbose" ] && echo "Uploading job $f to rekon.jobs"
    content_type="application/javascript"
    curl -X PUT -H"Content-Type: $content_type" $riak_url/$f --data-binary @$base_dir/$f
  done
}


do_install_erlang()
{
  echo "Installing erlang modules to $module_dir"
  sudo mkdir -p $module_dir
  sudo erlc -o $module_dir rekon/erlang/*.erl

}

node="127.0.0.1:8098"
module_dir=/etc/riak/erlang
while getopts "aeE:jv?" option
do
  case $option in
    a) erlang=yes; jobs=yes;;
    e) erlang=yes;;
    E) erlang=yes; module_dir=$OPTARG;;
    j) jobs=yes;;
    v) verbose=yes;;
    '?') do_usage 0;;
    *) do_usage 1;;
  esac
done
shift $(($OPTIND - 1))

[ -n "$1" ] && node=$1

[ -n "$jobs" ] && do_install_jobs
[ -n "$erlang" ] && do_install_erlang


echo "Installed, now visit: $riak_url/go"
