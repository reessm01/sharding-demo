import os, json

filename = "chapter2.txt"

def load_data_from_file(path=None):
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

    mapfile = "mapping.json"

    def write_map(self):
        with open(self.mapfile, 'w') as m:
            json.dump(self.mapping, m, indent=2)

    def load_map(self):
        if not os.path.exists(self.mapfile):
            return dict()
        with open(self.mapfile, 'r') as m:
            return json.load(m)

    def build_shards(self, count, data=None):
        if self.mapping != {}:
            return "Cannot build shard setup -- sharding already exists."

        spliced_data = self._generate_sharded_data(count, data)

        for num, d in enumerate(spliced_data):
            self._write_shard(num, d)

        self.write_map()

    def _write_shard(self, num, data):
        with open(f"data/{num}.txt", 'w') as s:
            s.write(data)

        self.mapping.update(
            {
                str(num): {
                    'start': num * len(data),
                    'end': (num + 1) * len(data)
                }
            }
        )

    def _generate_sharded_data(self, count, data):
        """Split the data into as many pieces as the count."""
        splicenum, rem = divmod(len(data), count)

        result = [data[splicenum * z:splicenum * (z + 1)] for z in range(count)]
        # take care of any odd characters
        if rem > 0:
            result[-1] += data[-rem:]

        return result

    def load_data_from_shards(self):
        result = list()

        for db in self.mapping.keys():
            with open(f'data/{db}.txt', 'r') as f:
                result.append(f.read())
        return ''.join(result)

    def add_shard(self):
        self.mapping = self.load_map()
        data = self.load_data_from_shards()
        # why 2? Because we have to compensate for zero indexing
        new_shard_num = str(int(max(list(self.mapping.keys()))) + 2)

        spliced_data = self._generate_sharded_data(int(new_shard_num), data)

        for num, d in enumerate(spliced_data):
            self._write_shard(num, d)

        self.write_map()

    def remove_shard(self):
        pass

    def add_replication(self):
        pass

    def remove_replication(self):
        pass

    def get_shard_data(self, shardnum=None):
        if not shardnum:
            return self.get_all_shard_data()
        data = self.mapping.get(shardnum)
        if not data:
            return f"Invalid shard ID. Valid shard IDs: {self.mapping.keys()}"

    def get_all_shard_data(self):
        return self.mapping


s = ShardHandler()

s.build_shards(5, load_data_from_file())

print(s.mapping.keys())

s.add_shard()

print(s.mapping.keys())
