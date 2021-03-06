#!/bin/python3
import os
import pathlib
import hashlib
import io
import sqlite3
import multiprocessing as mp

# dir to start the check
CHECK_DIR = 'files'

# SQL Lite DB file
SQL_LITE_DB = 'python-dup.db'

# buffer size for file hashing
BUF_SIZE = io.DEFAULT_BUFFER_SIZE

def db_connect():
    con = sqlite3.connect(SQL_LITE_DB)
    cur = con.cursor()
    #cur.execute('''DROP TABLE files''')
    cur.execute('''
    CREATE TABLE IF NOT EXISTS files (
        path TEXT PRIMARY KEY,
        hash varchar(128) NOT NULL,
        modTime INTEGER,
        size INTEGER
    )
    ''')
    con.commit()
    return con

def collect_files(start_dirs):
    result = []
    for dir in start_dirs:
        if os.path.isfile(dir):
            # oops dir is a file, add it to list
            result.append(dir)
            continue

        # traverse root directory, and list directories as dirs and files as files
        for basepath, _, files in os.walk(dir):
            absPathsFiles = [os.path.abspath(os.path.join(basepath, f)) for f in files]
            result.extend(absPathsFiles)
            
    return result
        

def scan_files(db_con, files):
    db_cur = db_con.cursor()
    db_to_update = []
    for file_path in files:
        # get file modification time
        modTime = pathlib.Path(file_path).stat().st_mtime_ns
        fileSize = pathlib.Path(file_path).stat().st_size
        if fileSize == 0: continue 
        # check if file has been checked already
        db_res = db_cur.execute('SELECT modTime FROM files WHERE path = ?', (file_path, )).fetchone()
        if db_res != None:
            # check if file has been change since last scan
            if modTime == db_res[0]: continue
            print(f'file changed', end=' ')

        print(f'{file_path}')     
        # append to tuple to be updated
        db_to_update.append((file_path,modTime, fileSize))
    
    return db_to_update    

def add_hash2_update(f):
    fHash = hash_file(f[0])
    return (f[0], fHash, f[1], f[2], fHash, f[2])

def update_db(db_con, updates_list):
    with mp.Pool() as p:
        db_updates = p.imap(add_hash2_update, updates_list, chunksize=10)

        # store changes in database
        db_cur = db_con.cursor()
        db_cur.executemany('''
            INSERT INTO files (path, hash, modTime, size) 
            VALUES (?, ?, ?, ?)
            ON CONFLICT(path) DO UPDATE SET hash = ?, modTime = ?''', 
            db_updates)

        db_con.commit()

def hash_file(fpath):
    sha512 = hashlib.sha512()
    with open(fpath, 'rb') as f:
        while True:
            data = f.read(BUF_SIZE)
            if not data: break
            sha512.update(data)
    return sha512.hexdigest()    

def get_duplicates(db_con):
    db_cur = db_con.cursor()

    hashes = db_cur.execute('SELECT hash, COUNT(PATH) FROM files GROUP BY hash HAVING COUNT(hash) > 1').fetchall() 
    # pre scan if files still exist and clean up db
    duplicates_list = []
    to_delete = []
    for row in hashes:
        file_dups = []
        for dup_file in db_cur.execute('SELECT path, modTime, size FROM files WHERE hash = ?', (row[0], )):
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

def menu():
    import argparse
    parser = argparse.ArgumentParser(description='Duplicate file detection.')
    parser.add_argument('startdirs', metavar='DIR', type=str, nargs='+',
                    help='Directory to scan for duplicate files')
    
    return parser.parse_args()

def main():
    args = menu()
    # connect to database
    db_con = db_connect()

    # run through given dir
    files = collect_files(args.startdirs)
    
    # actually scan for duplicate files
    updates = scan_files(db_con, files)
    # update database
    update_db(db_con, updates)

    # create a list of duplicate files
    duplicates_list = get_duplicates(db_con)

    # print duplicates
    for row in duplicates_list:
        print(f'Found {len(row)} duplicates:')
        for dup_file in row:
            print(f'\t{dup_file[0]}, modification: {dup_file[1]}, size: {dup_file[2]}')
    
    # close db connection
    db_con.close()

if __name__ == '__main__':
    main()