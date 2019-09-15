import json
import os
from shutil import copyfile
from typing import List, Dict
from os import listdir
from os.path import isfile, join
import re
import time

filename = "chapter2.txt"


def load_data_from_file(path=None) -> str:
    with open(path if path else filename, 'r') as f:
        data = f.read()
    return data


class ShardHandler(object):
    """
    Take any text file and shard it into X number of files with
    Y number of replications.
    """

    def __init__(self):
        self.mapping = self.load_map()
        self.last_char_position = 0

    mapfile = "mapping.json"

    def write_map(self) -> None:
        """Write the current 'database' mapping to file."""
        with open(self.mapfile, 'w') as m:
            json.dump(self.mapping, m, indent=2)

    def load_map(self) -> Dict:
        """Load the 'database' mapping from file."""
        if not os.path.exists(self.mapfile):
            return dict()
        with open(self.mapfile, 'r') as m:
            return json.load(m)

    def _reset_char_position(self):
        self.last_char_position = 0

    def build_shards(self, count: int, data: str = None) -> [str, None]:
        """Initialize our miniature databases from a clean mapfile. Cannot
        be called if there is an existing mapping -- must use add_shard() or
        remove_shard()."""
        if self.mapping != {}:
            return "Cannot build shard setup -- sharding already exists."

        spliced_data = self._generate_sharded_data(count, data)

        for num, d in enumerate(spliced_data):
            self._write_shard(num, d)

        self.write_map()

    def _write_shard(self, num: int, data: str) -> None:
        """Write an individual database shard to disk and add it to the
        mapping."""
        if not os.path.exists("data"):
            os.mkdir("data")
        with open(f"data/{num}.txt", 'w') as s:
            s.write(data)

        if num == 0:
            # We reset it here in case we perform multiple write operations
            # within the same instantiation of the class. The char position
            # is used to power the index creation.
            self._reset_char_position()

        self.mapping.update(
            {
                str(num): {
                    'start': (
                        self.last_char_position if
                            self.last_char_position == 0 else
                            self.last_char_position + 1
                        ),
                    'end': self.last_char_position + len(data)
                }
            }
        )
        print(self.mapping)
        self.last_char_position += len(data)

    def _generate_sharded_data(self, count: int, data: str) -> List[str]:
        """Split the data into as many pieces as needed."""
        splicenum, rem = divmod(len(data), count)

        result = [data[splicenum * z:splicenum * (z + 1)] for z in range(count)]
        # take care of any odd characters
        if rem > 0:
            result[-1] += data[-rem:]

        return result

    def load_data_from_shards(self) -> str:
        """Grab all the shards, pull all the data, and then concatenate it."""
        result = list()

        for db in self.mapping.keys():
            with open(f'data/{db}.txt', 'r') as f:
                result.append(f.read())
        return ''.join(result)

    def add_shard(self) -> None:
        """Add a new shard to the existing pool and rebalance the data."""
        self.mapping = self.load_map()
        data = self.load_data_from_shards()
        keys = [int(z) for z in list(self.mapping.keys())]
        keys.sort()
        # why 2? Because we have to compensate for zero indexing
        new_shard_num = max(keys) + 2

        spliced_data = self._generate_sharded_data(new_shard_num, data)

        for num, d in enumerate(spliced_data):
            self._write_shard(num, d)

        self.write_map()

        self.sync_replication()

    def _remove_file(self, key) -> None:
        
        file_list = [f for f in listdir('./data/') if isfile(join('./data/', f))]
        originals = [f for f in file_list if '-' not in f]
        originals = sorted(originals, key=lambda x: int(x[:-4]))
        reps = [f for f in file_list if '-' in f]
        reps = sorted(reps, key=lambda x: int(x.split('-')[0]))
        target = originals[len(originals)-1]

        for f in reps:
            print(f)
            s_org = f.split('-')[0] + '.txt'
            if target in s_org:
                os.remove(f'./data/{f}')

        os.remove(f'./data/{target}')
        print(self.mapping)
        self.mapping.pop(key, None)
        print(self.mapping)

    def remove_shard(self) -> None:
        """Loads the data from all shards, removes the extra 'database' file,
        and writes the new number of shards to disk.
        """
        self.mapping = self.load_map()
        data = self.load_data_from_shards()
        keys = [int(z) for z in list(self.mapping.keys())]
        keys.sort()
        # why 2? Because we have to compensate for zero indexing
        new_shard_num = max(keys)

        spliced_data = self._generate_sharded_data(new_shard_num, data)
        keys = list(self.mapping.keys())
        key = keys[len(keys)-1]
        self._remove_file(key)

        for num, d in enumerate(spliced_data):
            self._write_shard(num, d)

        self.write_map()

        self.sync_replication()

    def add_replication(self) -> None:
        """Add a level of replication so that each shard has a backup. Label
        them with the following format:

        1.txt (shard 1, primary)
        1-1.txt (shard 1, replication 1)
        1-2.txt (shard 1, replication 2)
        2.txt (shard 2, primary)
        2-1.txt (shard 2, replication 1)
        ...etc.

        By default, there is no replication -- add_replication should be able
        to detect how many levels there are and appropriately add the next
        level.
        """
        data = self.load_data_from_shards()
        file_list = [f for f in listdir('./data/') if isfile(join('./data/', f))]

        originals = [f for f in file_list if '-' not in f]
        originals = sorted(originals, key=lambda x: int(x[:-4]))
        reps = [f for f in file_list if '-' in f]
        reps = sorted(reps, key=lambda x: int(x.split('-')[0]))

        if not reps:
            for f in originals:
                with open(f'data/{f}', 'r') as _file:
                    data = _file.read()
                    name = _file.name[:-4]
                    filename = f'{name}-1.txt'
                    with open(filename, 'w+') as s:
                        s.write(data)
        else:
            for f in reps:
                with open(f'data/{f}', 'r') as _file:
                    data = _file.read()
                    num = str(int(_file.name.split('-')[1][:-4]) + 1)
                    name = f[:-4].split('-')[0]
                    filename = f'{name}-{num}'
                    with open(f'./data/{filename}.txt', 'w+') as s:
                        s.write(data)


    def remove_replication(self) -> None:
        """Remove the highest replication level.

        If there are only primary files left, remove_replication should raise
        an exception stating that there is nothing left to remove.

        For example:

        1.txt (shard 1, primary)
        1-1.txt (shard 1, replication 1)
        1-2.txt (shard 1, replication 2)
        2.txt (shard 2, primary)
        etc...

        to:

        1.txt (shard 1, primary)
        1-1.txt (shard 1, replication 1)
        2.txt (shard 2, primary)
        etc...
        """

        self.sync_replication()
        file_list = [f for f in listdir('./data/') if isfile(join('./data/', f))]
        originals = [f for f in file_list if '-' not in f]
        originals = sorted(originals, key=lambda x: int(x[:-4]))
        reps = [f for f in file_list if '-' in f]
        reps = sorted(reps, key=lambda x: int(x.split('-')[0]))

        try:
            max_rep = max(list(map(lambda x: int(x.split('-')[1][:-4]), reps)))
        except ValueError:
            max_rep = None

        if max_rep:
            for o in originals:
                name = o[:-4]
                path = f'data/{name}-{max_rep}.txt'
                os.remove(path)
        else:
            raise Exception('Only originals found.')

    def sync_replication(self) -> None:
        """Verify that all replications are equal to their primaries and that
         any missing primaries are appropriately recreated from their
         replications."""
        file_list = [f for f in listdir('./data/') if isfile(join('./data/', f))]

        originals = [f for f in file_list if '-' not in f]
        originals = sorted(originals, key=lambda x: int(x[:-4]))
        reps = [f for f in file_list if '-' in f]
        reps = sorted(reps, key=lambda x: int(x.split('-')[0]))

        int_orig = [int(o[:-4]) for o in originals]
        orig_missing = []
        for i, o in enumerate(int_orig):
            print(i, o)
            if i != o:
                orig_missing.append(i)
        for o in orig_missing:
            for r in reps:
                if f'{o}-' in r:
                    with open(f'data/{r}', 'r') as _file:
                        data = _file.read()
                        with open(f'data/{o}.txt', 'w+') as s:
                            s.write(data)

        file_list = [f for f in listdir('./data/') if isfile(join('./data/', f))]
        originals = [f for f in file_list if '-' not in f]
        originals = sorted(originals, key=lambda x: int(x[:-4]))
        reps = [f for f in file_list if '-' in f]
        reps = sorted(reps, key=lambda x: int(x.split('-')[0]))
        
        try:
            max_rep = max(list(map(lambda x: int(x.split('-')[1][:-4]), reps)))
        except ValueError:
            max_rep = None
        if max_rep:
            for i in range(1, max_rep+1):
                for f in originals:
                    with open(f'data/{f}', 'r') as _file:
                        data = _file.read()
                        name = _file.name[:-4]
                        filename = f'{name}-{i}.txt'
                        with open(filename, 'w+') as s:
                            s.write(data)

    def get_shard_data(self, shardnum=None) -> [str, Dict]:
        """Return information about a shard from the mapfile."""
        if not shardnum:
            return self.get_all_shard_data()
        data = self.mapping.get(shardnum)
        if not data:
            return f"Invalid shard ID. Valid shard IDs: {self.mapping.keys()}"
        return f"Shard {shardnum}: {data}"

    def get_all_shard_data(self) -> Dict:
        """A helper function to view the mapping data."""
        return self.mapping


s = ShardHandler()
