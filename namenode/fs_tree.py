import os


class Directory:
    def __init__(self, parent, name):
        self.parent = parent
        self.name = name
        self.children = {}
        self.read_counter = 0
        self.write_counter = 0

    def __str__(self):
        return os.path.join(str(self.parent), self.name)

    def readable(self):
        return self.write_counter == 0

    def writable(self):
        return self.write_counter == 0 and self.read_counter == 0

    def empty(self):
        return len(self.children) == 0

    # def add_child(self, ):


class File:
    def __init__(self, parent, name, size, creation_date):
        self.parent = parent
        self.name = name
        self.nodes = []
        self.read_counter = 0
        self.write_counter = 0
        self.size = size
        self.creation_date = creation_date
        self.change_date = creation_date

    def __str__(self):
        return os.path.join(str(self.parent), self.name)

    def readable(self):
        return self.write_counter == 0

    def writable(self):
        return self.write_counter == 0 and self.read_counter == 0
