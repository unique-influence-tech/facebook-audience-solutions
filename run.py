"""
Audience Management

Usage:

    Run for the first time:
    
     python run.py build 
    
    Run the on-going process:
    
     python run.py execute 
    
    Rebuild the audiences and tables:
    
     python run.py rebuild 
    
    Tear down the database and delete audiences:
    
     python run.py teardown 

"""
import sys
import os
import argparse

from audience import config, models
from audience import Sorter, Adapter
from audience import (stream_ftp, sqlite_import,
                      sqlite_truncate, write_database)


def build(config):
    """ This acts as a build process for the first time you run
    the custom audience management flow.

    :params config: module, configuration
    """
    write_database(config)
    data = stream_ftp(config)
    sqlite_import('customers', data)

    prepared = Sorter()
    prepared.add_sort

    if config.DEBUG:
        adapter = Adapter(config.TESTING_SITE_ID)
        # Create audiences
        adapter.create_audience(config.CURRENT+' test')
        adapter.create_audience(config.LAPSED+' test')
        adapter.create_audience(config.EXTRA+' test')
        # Add users 
        adapter.add_users(config.CURRENT+' test', prepared.current)
        adapter.add_users(config.LAPSED+' test', prepared.lapsed)
        adapter.add_users(config.EXTRA+' test', prepared.extra_lapsed)
    else:
        adapter = Adapter(config.SITE_ID)
        # Create audiences
        adapter.create_audience(config.CURRENT)
        adapter.create_audience(config.LAPSED)
        adapter.create_audience(config.EXTRA)
        # Add users 
        adapter.add_users(config.CURRENT, prepared.current)
        adapter.add_users(config.LAPSED, prepared.lapsed)
        adapter.add_users(config.EXTRA, prepared.extra_lapsed)


def execute(config):
    """ This is the on-going audience management flow. This
    includes processing new files, removing and adding users.

    :params config: module, configuration
    """
    data = stream_ftp(config, keyword='_')
    sqlite_import('customers', data)

    prepared = Sorter()
    prepared.add_remove_sort

    if config.DEBUG:
        adapter = Adapter(config.TESTING_SITE_ID)
        # Remove users
        adapter.remove_users(config.CURRENT+' test', prepared.current_deletes)
        adapter.remove_users(config.LAPSED+' test', prepared.lapsed_deletes)
        adapter.remove_users(config.EXTRA+' test', prepared.extra_lapsed_deletes)
        # Add users
        adapter.add_users(config.CURRENT+' test', prepared.current)
        adapter.add_users(config.LAPSED+' test', prepared.lapsed)
        adapter.add_users(config.EXTRA+' test', prepared.extra_lapsed)
    else:
        adapter = Adapter(config.SITE_ID)
        # Remove users
        adapter.remove_users(config.CURRENT, prepared.current_deletes)
        adapter.remove_users(config.LAPSED, prepared.lapsed_deletes)
        adapter.remove_users(config.EXTRA, prepared.extra_lapsed_deletes)
        # Add users
        adapter.add_users(config.CURRENT, prepared.current)
        adapter.add_users(config.LAPSED, prepared.lapsed)
        adapter.add_users(config.EXTRA, prepared.extra_lapsed)


def teardown(config):
    """ Delete database and custom audiences.
    
    :params config: module, configuration
    """
    try:
        os.remove(config.DATABASE_PATH)
    except FileNotFoundError:
        print('Attempted db file removal. No db file to remove.')

    if config.DEBUG:
        adapter = Adapter(config.TESTING_SITE_ID)
        # Delete audiences
        adapter.delete_audience(config.CURRENT+' test')
        adapter.delete_audience(config.LAPSED+' test')
        adapter.delete_audience(config.EXTRA+' test')
    else:
        adapter = Adapter(config.SITE_ID)
        # Delete audiences
        adapter.delete_audience(config.CURRENT)
        adapter.delete_audience(config.LAPSED)
        adapter.delete_audience(config.EXTRA)


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Run the Audience Management Process')
    parser.add_argument("action", help="build|teardown|execute|rebuild")
    args = parser.parse_args()

    if args.action == 'build':
        build(config)
    if args.action == 'teardown':
        teardown(config)
    if args.action == 'execute':
        execute(config)
    if args.action == 'rebuild':
        teardown(config)
        build(config)
    
        