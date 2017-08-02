#!/bin/sh

if [ -f /etc/memcached.conf ]; then
  mv -f /etc/memcached.conf /etc/memcached.orig
fi

if [ -f /etc/memcached.conf ]; then
  mv -f  /apps/memcache/memcached.nflx /etc/memcached.conf
fi

mkdir -p /data/keydump
chmod -Rf 777 /data/keydump

mkdir -p /data/memcache
chmod -Rf 777 /data/memcache

mkdir -p /data/checkpoint
chmod -Rf 777 /data/checkpoint

ln -s /apps/memcache/daemontools-service /service/nflx-memcache
update-rc.d -f memcached remove
/usr/sbin/update-rc.d nflx-memcache defaults 99
