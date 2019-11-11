import os


class Directory:
    def __init__(self, name, parent=None):
        self.parent = parent
        self.name = name
        self.children = {}
        self.read_counter = 0
        self.write_counter = 0

    def __str__(self):
        return os.path.join(str(self.parent), self.name)

    def __contains__(self, item):
        return item in self.children

    def readable(self):
        return self.write_counter == 0

    def writable(self):
        return self.write_counter == 0 and self.read_counter == 0

    def empty(self):
        return len(self.children) == 0

    def get_root(self):
        if self.name == '/':
            return self
        else:
            return self.parent.get_root(self)

    def get_absolute_path(self, path):
        sw = lambda x, s: x.startswith(s)
        cur_dir = self
        while sw(path, '/') or sw(path, '../') or sw(path, './') or '/' in path:
            if path.startswith('/'):
                cur_dir = self.get_root()
                path = path[1:]
            elif path.startswith('../'):
                if cur_dir != self.get_root():
                    cur_dir = cur_dir.parent
                path = path[3:]
            elif path.startswith('./'):
                path = path[2:]
            elif isinstance(cur_dir, File):
                return None, "No such file or directory"
            else:
                parent_dir, path = path.split('/', 1)
                if parent_dir in cur_dir:
                    cur_dir = cur_dir[parent_dir]
                else:
                    return None, "No such file or directory"
        return cur_dir, os.path.join(str(cur_dir), self.name)

    def add_file(self, file_name):
        new_file = File(self, file_name)
        self.children[file_name] = new_file
        return new_file


class File:
    def __init__(self, parent, name):
        self.parent = parent
        self.name = name
        self.nodes = []
        self.read_counter = 0
        self.write_counter = 0

    def __str__(self):
        return os.path.join(str(self.parent), self.name)

    def readable(self):
        return self.write_counter == 0

    def writable(self):
        return self.write_counter == 0 and self.read_counter == 0
