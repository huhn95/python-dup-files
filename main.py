#!/bin/python3
import os
import hashlib
import collections

# dir to start the check
CHECK_DIR = 'files'

# buffer size for file hashing
BUF_SIZE = 65536

def hash_file(fpath):
    sha512 = hashlib.sha512()
    with open(fpath, 'rb') as f:
        while True:
            data = f.read(BUF_SIZE)
            if not data: break
            sha512.update(data)
    return sha512.hexdigest()


def main():
    files_dict = {}
    duplicate_files = collections.defaultdict(set)

    # traverse root directory, and list directories as dirs and files as files
    for root, _, files in os.walk(CHECK_DIR):
        path = root.split(os.sep)
        if path[-1].startswith('_'): continue

        # print folder structure
        print((len(path) - 1) * '---', os.path.basename(root))
        for file in files:
            # print file
            print(len(path) * '   ', file)
            file_path = os.path.join(root, file)
            fHash = hash_file(file_path)

            if fHash in files_dict:
                duplicate_files[fHash] = duplicate_files[fHash].union({files_dict[fHash], file_path})
            else: 
                files_dict[fHash] = file_path
    print('Done.')
    print('duplicate files:')
    for dup in duplicate_files:
        print(duplicate_files[dup])

if __name__ == '__main__':
    main()