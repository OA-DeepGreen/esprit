EXAMPLE = """
____a___a__
___________
___________
____A_B_A__
__b________
1___B___b_X
__b________
____A_B_A__
___________
___________
____a___a__
"""

OUT = "cosmic.txt"

from curses import wrapper
import time

class Node(object):
    def __init__(self, x, y):
        self.x = x
        self.y = y

    def display(self):
        return "$"

class Space(Node):
    def __init__(self, x, y):
        super(Space, self).__init__(x, y)
        self.enter = None
        self.exit = None

    def isOccupied(self):
        return self.exit is not None

    def display(self):
        if self.enter is None or self.exit is None:
            return "_"
        if self.enter == "n":
            if self.exit == "s": return "|"
            if self.exit in ["e", "w"]: return "+"
        if self.enter == "e":
            if self.exit == "w" : return "-"
            if self.exit in ["n", "s"]: return "+"
        if self.enter == "s":
            if self.exit == "n": return "|"
            if self.exit in ["e", "w"]: return "+"
        if self.enter == "w":
            if self.exit == "e": return "-"
            if self.exit in ["n", "s"]: return "+"

class PickUp(Node):
    def __init__(self, x, y, ident):
        super(PickUp, self).__init__(x, y)
        self.ident = ident

    def display(self):
        return self.ident

class DropOff(Node):
    def __init__(self, x, y, ident):
        super(DropOff, self).__init__(x, y)
        self.ident = ident

    def display(self):
        return self.ident.upper()

class Start(Space):
    def __init__(self, x, y):
        super(Start, self).__init__(x, y)

    def display(self):
        return "o"

class Finish(Space):
    def __init__(self, x, y):
        super(Finish, self).__init__(x, y)

    def display(self):
        return "X"

class Pod(object):
    def __init__(self, definition):
        self.nodes = []
        self.width = 0
        self.height = 0
        self.train_size = None
        self._current = None
        self._parse(definition)

    def _parse(self, definition):
        definition = definition.strip()
        x = 0
        y = 0
        for c in definition:
            if c == "_":
                self.nodes.append(Space(x, y))
            elif c in ["a", "b"]:
                self.nodes.append(PickUp(x, y, c))
            elif c in ["A", "B"]:
                self.nodes.append(DropOff(x, y, c.lower()))
            elif c in ["1", "2"]:
                node = Start(x, y)
                self.nodes.append(node)
                self.train_size = int(c)
                self._current = node
            elif c == "X":
                self.nodes.append(Finish(x, y))
            elif c == "\n":
                x = 0
                y += 1
                continue

            x += 1

        self.width = x
        self.height = y + 1

    def serialise(self):
        grid = [["$"] * self.width for x in range(self.height)]

        for node in self.nodes:
            grid[node.y][node.x] = node.display()

        rows = ["".join(x) for x in grid]
        return "\n".join(rows)

    def _start(self):
        for node in self.nodes:
            if isinstance(node, Start):
                return node

    def get(self, coords):
        x, y = coords
        if x < 0 or x > self.width or y < 0 or y > self.height:
            return None
        for node in self.nodes:
            if node.x == x and node.y == y:
                return node
        return None

    def options(self):
        x = self._current.x
        y = self._current.y
        return self.optionsFor((x, y))

    def optionsFor(self, coords):
        x, y = coords
        above = self.get((x, y - 1))
        right = self.get((x + 1, y))
        below = self.get((x, y + 1))
        left = self.get((x - 1, y))
        opts = []
        if above is not None and isinstance(above, Space) and not above.isOccupied():
            opts.append("n")
        if right is not None and isinstance(right, Space) and not right.isOccupied():
            opts.append("e")
        if below is not None and isinstance(below, Space) and not below.isOccupied():
            opts.append("s")
        if left is not None and isinstance(left, Space) and not left.isOccupied():
            opts.append("w")

        return opts

    def travel(self, direction):
        self._current.exit = direction
        coords = (self._current.x, self._current.y)
        if direction == "n":
            coords = (coords[0], coords[1] - 1)
        elif direction == "e":
            coords = (coords[0] + 1, coords[1])
        elif direction == "s":
            coords = (coords[0], coords[1] + 1)
        elif direction == "w":
            coords = (coords[0] - 1, coords[1])
        self._current = self.get(coords)

        opposites = {"n" : "s", "e" : "w", "s" : "n", "w" : "e"}
        self._current.enter = opposites[direction]

    def backtrack(self):
        direction = self._current.enter
        self._current.enter = None
        coords = (self._current.x, self._current.y)
        if direction == "n":
            coords = (coords[0], coords[1] - 1)
        elif direction == "e":
            coords = (coords[0] + 1, coords[1])
        elif direction == "s":
            coords = (coords[0], coords[1] + 1)
        elif direction == "w":
            coords = (coords[0] - 1, coords[1])
        self._current = self.get(coords)
        self._current.exit = None

    def current_coords(self):
        return (self._current.x, self._current.y)

    def isComplete(self):
        return isinstance(self._current, Finish)

def _hasPathTo(pod, coords):
    current = pod.current_coords()
    area = []
    lookups = [current]
    while len(lookups) > 0:
        lookup = lookups.pop()
        x = lookup[0]
        y = lookup[1]
        options = pod.optionsFor(lookup)
        for o in options:
            if o == "n":
                above = (x, y - 1)


def _getToComplete(pod, path_history, stdscr):
    while not pod.isComplete():
        options = pod.options()
        coords = pod.current_coords()
        if coords not in path_history:
            path_history[coords] = []
        history = path_history.get(coords)
        direction = None
        for o in options:
            if o not in history:
                direction = o
                break
        if direction is not None:
            history.append(direction)
            pod.travel(direction)
        else:
            del path_history[coords]
            pod.backtrack()
        output(pod, stdscr)
        time.sleep(0.1)

def solve(pod, stdscr):
    with open(OUT, "wb") as f:
        path_history = {}
        for i in range(10):
            _getToComplete(pod, path_history, stdscr)
            f.write(pod.serialise() + "\n\n")
            f.flush()

            pod.backtrack()



def output(pod, stdscr):
    str = pod.serialise()
    lines = str.split("\n")
    for j in range(len(lines)):
        stdscr.addstr(j, 0, lines[j])
    stdscr.refresh()

def main(stdscr):
    # Clear screen
    stdscr.clear()
    pod = Pod(EXAMPLE)
    solve(pod, stdscr)
    stdscr.getkey()

wrapper(main)


# solve(pod)