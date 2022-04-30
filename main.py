#!/bin/python3
import os
import pathlib
import hashlib
import collections
import sqlite3


# dir to start the check
CHECK_DIR = 'files'

# SQL Lite DB file
SQL_LITE_DB = 'python-dup.db'

# buffer size for file hashing
BUF_SIZE = 65536

def db_connect():
    con = sqlite3.connect(SQL_LITE_DB)
    cur = con.cursor()
    # cur.execute('''DROP TABLE files''')
    cur.execute('''
    CREATE TABLE IF NOT EXISTS files (
        path TEXT PRIMARY KEY,
        hash varchar(128) NOT NULL,
        modTime INTEGER
    )
    ''')
    con.commit()
    return con


def hash_file(fpath):
    sha512 = hashlib.sha512()
    with open(fpath, 'rb') as f:
        while True:
            data = f.read(BUF_SIZE)
            if not data: break
            sha512.update(data)
    return sha512.hexdigest()

def update_file(db_cur, file_path, modTime):
    fHash = hash_file(file_path)
    db_cur.execute('''
        INSERT INTO files (path, hash, modTime) 
        VALUES (?, ?, ?)
        ON CONFLICT(path) DO UPDATE SET hash = ?, modTime = ?''', 
        (file_path, fHash, modTime, fHash, modTime))


def traverse_dir(db_con, start_dir):
    db_cur = db_con.cursor()

    # traverse root directory, and list directories as dirs and files as files
    for root, _, files in os.walk(start_dir):
        path = root.split(os.sep)
        if path[-1].startswith('_'): continue

        for file in files:
            # absolute file path resolution
            file_path = os.path.abspath(os.path.join(root, file))
            # get file modification time
            modTime = pathlib.Path(file_path).stat().st_mtime_ns 
            # check if file has been checked already
            db_res = db_cur.execute('SELECT modTime FROM files WHERE path = ?', (file_path, )).fetchone()
            if db_res != None:
                # check if file has been change since last scan
                if modTime == db_res[0]: continue
                print(f'file changed')

            print(f'adding / updating db {file_path}')     
            update_file(db_cur, file_path, modTime)
    
    # store changes in database
    db_con.commit()

    
def get_duplicates(db_con):
    db_cur = db_con.cursor()

    hashes = db_cur.execute('SELECT hash, COUNT(PATH) FROM files GROUP BY hash HAVING COUNT(hash) > 1').fetchall() 
    # pre scan if files still exist and clean up db
    duplicates_list = []
    to_delete = []
    for row in hashes:
        file_dups = []
        for dup_file in db_cur.execute('SELECT path, modTime FROM files WHERE hash = ?', (row[0], )):
            if os.path.exists(dup_file[0]):
                file_dups.append(dup_file)
            else:
                # file does not exist anymore, remove from database
                to_delete.append((dup_file[0], ))

        duplicates_list.append(file_dups)

    # commit db changes
    db_cur.executemany('''
                    DELETE FROM files
                    WHERE path = ?
                ''', to_delete)
    db_con.commit()
    return duplicates_list


def main():
    # connect to database
    db_con = db_connect()

    # run through given dir
    traverse_dir(db_con, CHECK_DIR)
    
    # create a list of duplicate files
    duplicates_list = get_duplicates(db_con)

    # print duplicates
    for row in duplicates_list:
        print(f'Found {len(row)} duplicates:')
        for dup_file in row:
            print(f'\t{dup_file[0]} {dup_file[1]}')
    
    # close db connection
    db_con.close()

if __name__ == '__main__':
    main()