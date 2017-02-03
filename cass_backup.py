#!/usr/bin/env python

# Super duper, coffe making, dish washing script to create manage snapshots
# on cassandra nodes.
# In a primitive way it can also create compresses backups of the snapshots.
# version: 0.02

import argparse
import os
import socket
import subprocess
import time
import glob
import shutil
import tarfile
import datetime
import sys

# TODO: A lot... Clean up, lint. Write a proper class out of it...?
#       Exceptions handing would be nice too

def exec_cmd(cmd, arg=""):
    '''
    Execute a command.
    '''

    process = subprocess.Popen(cmd + ' ' + arg, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    (result, error) = process.communicate()

    rc = process.wait()

    if rc != 0:
        print "Error: failed to exec command:", cmd
        print error
    return result

def list_snapshots():
    '''
    Simply list all snapshots,with nodetool.
    '''
    print exec_cmd('nodetool listsnapshots', "")

def create_snapshot(tag="", ktlist=""):
    '''
    Creates a snapshot tag is the name for the snapshot dir, ktlist is a list of keyspaces.
    If not provided snapshot for all keyspaces will be created.
    '''

    resp = "Creating at %s snapshot" % datetime.datetime.now()
    args = ""
    if tag:
        args = args + '-t' + ' ' + tag + ' '
        resp = resp + " with name: %s" % tag
    if ktlist:
        args = args + '-kt' + ' ' + ktlist
        resp = resp + " for keyspaces: %s" % ktlist
    if not args:
        resp = resp + ' all keyspaces.'

    print resp
    exec_cmd('nodetool snapshot', args)

def get_keyspaces(data_dir="/var/lib/cassandra/data"):
    '''
    Get a list of keyspaces available in the cassandra data dir.
    '''

    keyspace_list = os.listdir(data_dir)
    return keyspace_list

def get_tables_path(keyspace="all", data_dir="/var/lib/cassandra/data"):
    '''
    Returns an array of paths for tables for a given keyspace.
    '''

    tables = []
    if keyspace == "all":
        tables.append(glob.glob('%s/*/*' % (data_dir)))
    else:
        tables.append(glob.glob('%s/%s//*' % (data_dir, keyspace)))   #double  / to match the extact string

    return tables

def get_snapshots_path(keyspace="all", table="all", snapshot="all", data_dir="/var/lib/cassandra/data"):
    '''
    Returns a list of paths snapshots for a given keyspace and table.
    '''

    if keyspace == "all":   # brrr...
        keyspace = "*"
    if table == "all":
        table = "*"
    if snapshot == "all":
        snapshot = "*"

    snapshots = glob.glob('%s/%s/%s/snapshots/%s' % (data_dir, keyspace, table, snapshot))
    return snapshots


def remove_snapshots(hours=24, keyspace="all", table="all", data_dir="/var/lib/cassandra/data"):
    '''
    Remove snapshots older than n hours. Default is 24hours.
    '''

    if keyspace == "all":   #brrr... this is terb... ahh whatever
        keyspace = "*"
    if table == "all":
        table = "*"

    snapshot_list = glob.glob('%s/%s/%s/snapshots/*' % (data_dir, keyspace, table))
    print snapshot_list
    for snapshot in snapshot_list:
        if (int(time.time()) - int(os.path.getmtime(snapshot))) > int(hours * 3600):
            if os.path.exists(snapshot):
                # remove if exists
                #print "Keyspace: %s, table %s, snapshot: %s removed." % (keyspace, table, os.path.basename(snapshot)) #fixit
                shutil.rmtree(snapshot)
    print "Removed snapshots older than: %d hours. For keyspace: %s. Table: %s. In dir: %s." % (hours, keyspace, table, data_dir)

def rm_tar_files(targetdir, hours=48):
    '''
    Remove tar files older than n hours.
    '''

    files_list = glob.glob('%s/*.tar.gz' % (targetdir))

    for f in files_list:
        if (int(time.time()) - int(os.path.getmtime(f))) > int(hours * 3600):
            if os.path.exists(f):
                os.remove(f)
                print "Removed file: %s" % f

    if files_list == 0:
        print "nothing"
    else:
        print "Removed tar.gz files older than: %d hours. In dir: %s" % (hours, targetdir)

def compr_snapshot(filename, target, keyspace="all", table="all", snapshot="all", data_dir="/var/lib/cassandra/data"):
    '''
    Compres snapshot. By default all snapshots will be compressed
    '''
    print "Doing tar gzip of snapshot files at: %s. (This takes some time)." % datetime.datetime.now()
    snapshots = get_snapshots_path(keyspace, table, snapshot, data_dir)
    tar_path = "%s/%s" % (target, filename)

    with tarfile.open(tar_path, "w:gz") as tar:
        for snapshot in snapshots:
            tar.add(snapshot)
        tar.close()
    print 'Created tar.gz file: %s/%s at: %s' % (target, filename, datetime.datetime.now())

def backup_schemas(target_dir, filename, tmp_dir='/tmp', host="localhost"):
    '''
    Create a backup of the keyspace schemas. And the ring range for a node. Requires a target directory and a filename.
    '''
    print "Starting schemas backup at %s" % datetime.datetime.now()
    keyspaces = exec_cmd('cqlsh %s -e "DESC KEYSPACES"' % host)
    if not keyspaces:
        print "Schema backup failed."
        sys.exit(1)

    tar_path = "%s/%s" % (tmp_dir, filename)

    if not os.path.exists("%s/schemas/" % tmp_dir):
        os.makedirs("%s/schemas/" % tmp_dir)

    ip = socket.gethostbyname('%s' % host)
    tmp = exec_cmd('nodetool ring')
    ring = []
    for line in tmp.split('\n'):
        if ip in line:
            ring.append(line.split()[-1])

    ring_line = "initial_token: %s" % ','.join(ring)

    with tarfile.open(tar_path, "w:gz") as tar:
        for keyspace in keyspaces.split():
            path = "%s/schemas/%s.cql" % (tmp_dir, keyspace)
            f = open(path, 'w')
            f.write(exec_cmd('cqlsh %s -e "DESC KEYSPACE %s"' % (host, keyspace)))
            f.close()
            tar.add(path)
        f = open("%s/schemas/%s_ring.out" % (tmp_dir, host), 'w')
        f.write(ring_line)
        f.close()
        tar.add("%s/schemas/%s_ring.out" % (tmp_dir, host))
        tar.close()
    shutil.rmtree("%s/schemas/" %tmp_dir)    #remove tmp files
    shutil.move(tar_path, target_dir)
    print "Created schema and ring backup %s for schemas: %s at: %s" % (filename, keyspaces.rstrip(), datetime.datetime.now())

def main():
    now = datetime.datetime.now().strftime('%d%m%Y_%H%M%S')
    
    os.system('/usr/bin/ionice -c2 -n7 -p%s' % os.getpid()) # Make my self a little bit less I/O and CPU hungry.
    os.nice(19)

    p = argparse.ArgumentParser(description='This is script is handling cassandra snapshots. It makes coffe creates, removes and backups cassandra snapshots.')

    sub = p.add_subparsers(help='commands', dest='mode')

    create = sub.add_parser('create', help='Create snapshots. Requires snapshot name. Keyspace is optional if not provided snapshot for all keyspaces will be created.')
    create.add_argument('-n', '--snapshot-name', required=True, help='Name for the snapshot.')
    create.add_argument('-k', '--keyspace', help='List of keyspaces to snapshot.', nargs='*')

    remove = sub.add_parser('remove', help='Remove snapshots. Keyspace is optional if not provided snapshot for all keyspaces will be created.')
    remove.add_argument('-H', '--hours', required=True, help='Remove snapshots older than n hours.', type=int)
    remove.add_argument('-k', '--keyspace', help='List of keyspaces from which the snapshots should be removed. Default is all.', nargs='*', default=['all'])
    remove.add_argument('-t', '--table', help='List of tables from which the snapshots should beremoved. Default is all.', nargs='*', default=['all'])

    backup = sub.add_parser('backup', help='Backup. Create a snapshot compress and store it in a remote dir. It will also backup schemas and ring range for a node.')
    subsub = backup.add_subparsers(help='commands', dest='backup_mode')
    backup_create = subsub.add_parser('create', help='Create backup and move it to target dir.')
    backup_create.add_argument('-t', '--target', required=True, help='Target directory for backups.')
    backup_create.add_argument('-n', '--snapshot-name', required=True, help='Snapshot name.')
    backup_create.add_argument('-H', '--host-name', default="localhost", help='Hostname for cqlsh connection. Default: localhost.')

    backup_remove = subsub.add_parser('remove', help='Remove backups older than n hours.')
    backup_remove.add_argument('-H', '--hours', required=True, help='Remove snapshots older than n hours.', type=int)
    backup_remove.add_argument('-t', '--target', required=True, help='Target directory for.')
    backup_remove.add_argument('-f', '--force', action='store_true', help='Do not ask if you are sure.')

    list_s = sub.add_parser('list', help='Simply list snapshots.')

    #optionals for all.
    p.add_argument('-T', '--temp', default='/tmp', help='Temp directory used by this script. Default /tmp')
    p.add_argument('-D', '--data-dir', default='/var/lib/cassandra/data', help='Cassandra data directory to work on.')

    #print p.parse_args()
    args = p.parse_args()

    if args.mode == 'create':
        if args.keyspace == None:
            create_snapshot(args.snapshot_name)
        else:
            create_snapshot(args.snapshot_name, ','.join(args.keyspace))
    elif args.mode == 'remove':
        remove_snapshots(args.hours, ','.join(args.keyspace), ','.join(args.table), args.data_dir)
    elif args.mode == 'list':
        list_snapshots()
    elif args.mode == 'backup':
        if args.backup_mode == 'create':
            create_snapshot(args.snapshot_name)
            compr_snapshot("%s.tar.gz" % args.snapshot_name, args.temp, data_dir="%s" % args.data_dir)
            shutil.move("%s/%s.tar.gz" % (args.temp, args.snapshot_name), args.target)
            backup_schemas(args.target, "%s_schemas_%s.tar.gz" % (args.snapshot_name, now), args.temp, host="%s" % args.host_name)
        elif args.backup_mode == 'remove':
            if args.force:
                rm_tar_files(args.target, args.hours)
            else:
                print "Are you sure that you want to remove all *.tar.gz files in %s older than %d hours ? (y/n)" % (args.target, args.hours)
                yesno = raw_input().lower()
                if yesno == 'y':
                    rm_tar_files(args.target, args.hours)
                elif yesno == 'n':
                    print "Alrighty, canceling. Bye"
                else:
                    print "I don't know what you mean. Canceling..."

if __name__ == "__main__":
    main()
