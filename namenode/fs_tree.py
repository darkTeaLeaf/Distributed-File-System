import os


class Directory:
    def __init__(self, name, parent=None):
        self.parent = parent
        self.name = name
        self.children_files = {}
        self.children_directories = {}

        self.read_counter = 0
        self.write_counter = 0

    def __str__(self):
        if self.parent is None:
            return self.name
        else:
            return os.path.join(str(self.parent), self.name)

    def __contains__(self, item):
        return item in self.children_files or item in self.children_directories

    def __hash__(self):
        return hash(str(self))

    def to_dict(self):
        files = [name for name, obj in self.children_files.items()]
        dirs = {name: obj.to_dict() for name, obj in self.children_directories.items()}
        return {'f': files, 'd': dirs}

    def readable(self):
        return self.write_counter == 0

    def writable(self):
        return self.write_counter == 0 and self.read_counter == 0

    def empty(self):
        return len(self.children_directories) == 0 and len(self.children_files) == 0

    def get_root(self):
        if self.name == '/':
            return self
        else:
            return self.parent.get_root()

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
                return None, "No such directory."
            else:
                parent_dir, path = path.split('/', 1)
                if parent_dir in cur_dir:
                    cur_dir = cur_dir[parent_dir]
                else:
                    return None, "No such file or directory."
        return cur_dir, os.path.join(str(cur_dir), self.name)

    def add_file(self, file_name):
        new_file = File(file_name, self)
        self.children_files[file_name] = new_file
        return new_file

    def delete_file(self, file_name):
        return self.children_files.pop(file_name)

    def add_directory(self, dir_name):
        new_dir = Directory(dir_name, self)
        self.children_directories[dir_name] = new_dir
        return new_dir

    def delete_directory(self, dir_name):
        return self.children_files.pop(dir_name)

    def set_read_lock(self):
        if self.parent is not None:
            self.parent.set_read_lock()
        self.read_counter += 1

    def release_read_lock(self):
        if self.parent is not None:
            self.parent.release_read_lock()
        self.read_counter -= 1

    def set_write_lock(self):
        if self.parent is not None:
            self.parent.set_write_lock()
        self.write_counter += 1

    def release_write_lock(self):
        if self.parent is not None:
            self.parent.release_write_lock()
        self.write_counter -= 1


class File:
    def __init__(self, name, parent):
        self.parent = parent
        self.name = name
        self.nodes = set()
        self.read_counter = 0
        self.write_counter = 0

    def __str__(self):
        return os.path.join(str(self.parent), self.name)

    def __hash__(self):
        return hash(str(self))

    def readable(self):
        return self.write_counter == 0

    def writable(self):
        return self.write_counter == 0 and self.read_counter == 0

    def set_read_lock(self):
        self.parent.set_read_lock()
        self.read_counter += 1

    def release_read_lock(self):
        self.parent.release_read_lock()
        self.read_counter -= 1

    def set_write_lock(self):
        self.parent.set_write_lock()
        self.write_counter += 1

    def release_write_lock(self):
        self.parent.release_write_lock()
        self.write_counter -= 1
