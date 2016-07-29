# coding=utf-8
from random import choice
import sys, time, select, os
from functools import partial


# curses non-blocking character fetching
def get_c():
    ch = None
    try:
        os.system('stty raw</dev/tty')
        if select.select([sys.stdin], [], [], 0) == ([sys.stdin], [], []):
            ch = sys.stdin.read(1)
    finally:
        os.system('stty -raw</dev/tty')
    return ch

# while True:
#     print "char: %s" % (get_c())
#     time.sleep(0.1)


raw_pieces = [
"""\
 o
oxo
""",
"""\
o
x
oo
""",
]


class Piece(object):
    def __init__(self, blocks, piece_id):
        self.blocks = blocks
        self.id = piece_id

    def get_blocks(self, location, orientation):
        dx, dy = location
        vx, vy = orientations[orientation]
        return [
            (
                x * vx - y * vy + dx,
                y * vx + x * vy + dy
            )
            for x, y in self.blocks
        ]


pieces = []
orientations = ((1, 0), (0, 1), (-1, 0), (0, -1))


def _build_pieces():
    # build piece date
    for piece_number, piece in enumerate(raw_pieces, start=1):
        origin = (0, 0)
        blocks = []
        for y, line in enumerate(piece.splitlines()):
            for x, ch in enumerate(line):
                if not ch.isspace():
                    blocks.append((x, y))
                if ch in ("x", "X"):
                    origin = x, y
        pieces.append(Piece([(x-origin[0], y - origin[1]) for x, y in blocks], piece_number))
_build_pieces()


WIDTH = 10
HEIGHT = 20
SPAWN = (5, 23)
APPEARANCE = {None: " ", 0: "X"}
APPEARANCE.update({n: str(n) for n in range(1, len(pieces) + 1)})

field = [[None]*WIDTH for _ in range(HEIGHT)]
score = 0
current_piece = None
current_location = None
current_orientation = 0


def main():
    FRAME_TIME = 0.1
    FRAMES_PER_ADVANCE = 8
    KEYMAP = {
        'w': rotate,
        'a': partial(move, -1),
        's': drop,
        'd': partial(move, 1),
        ' ': drop,
    }

    frames_passed = FRAMES_PER_ADVANCE

    while True:
        frames_passed += 1

        # read input
        while True:
            key = get_c()
            if not key:
                break
            else:
                key = key.lower()
                KEYMAP.get(key, lambda: None)()
                draw_game()

        if frames_passed >= FRAMES_PER_ADVANCE:
            frames_passed = 0
            if not advance():
                break  # game over
            draw_game()

        time.sleep(FRAME_TIME)


def advance():
    """drop the current piece one line or place it onto the field if it's at the bottom
    return true if the game continues, false otherwise"""
    global current_piece, current_location, current_orientation

    if not current_piece:
        # spawn new piece
        current_piece = choice(pieces)
        current_orientation = 0
        current_location = SPAWN
        return True  # can't lose on spawning a new piece
    else:
        # drop current piece one space
        next_location = (current_location[0], current_location[1] - 1)
        if will_fit(current_piece, next_location, current_orientation):
            current_location = next_location
            return True  # can't lose if the piece cleanly descends
        else:
            # cannot drop piece, place onto field and iterate
            result = place_piece(current_piece, current_location, current_orientation)
            current_piece = None  # prepare to spawn new piece
            return result  # we might lose here


def move(direction):
    """attempt to move the current piece in this direction on the x axis"""
    global current_location

    if not current_piece:
        return

    next_location = current_location[0] + direction, current_location[1]
    # inspector complains that current_piece is none
    # noinspection PyTypeChecker
    if will_fit(current_piece, next_location, current_orientation):
        current_location = next_location


def rotate():
    """attempt to rotate the current piece"""
    global current_orientation

    if not current_piece:
        return

    next_orientation = (current_orientation + 1) % len(orientations)
    # inspector complains that current_piece is none
    # noinspection PyTypeChecker
    if will_fit(current_piece, current_location, next_orientation):
        current_orientation = next_orientation


def drop():
    """drop the current piece all the way to the bottom"""
    while current_piece:
        if not advance():
            return False
    return True


def will_fit(piece, location, orientation):
    """returns true if a piece can appear at this place in the game without collision"""
    return all(
        bx in range(WIDTH) and  # do not extend past sides
        by >= 0 and  # do not fall below bottom
        (
            (by >= HEIGHT) or  # being above the field is always OK
            not field[by][bx]  # do not collide with existing blocks
        )
        for bx, by in piece.get_blocks(location, orientation)
    )


def place_piece(piece, location, orientation):
    """place a piece onto the field. returns False if the game ended"""
    global score

    affected_lines = set()
    try:
        for block in piece.get_blocks(location, orientation):
            bx, by = block
            field[by][bx] = piece.id
            affected_lines.add(by)
    except IndexError:
        return False  # game over
    for line in sorted(affected_lines, reverse=True):
        if all(field[line]):  # line is filled
            del field[line]
            score += 1
    # add new lines at the top to fill in
    field.extend([None]*WIDTH for _ in range(HEIGHT - len(field)))
    return True


def draw_game():
    print()
    current_blocks = set(
        current_piece.get_blocks(current_location, current_orientation)
        if current_piece
        else ()
    )
    for y in range(HEIGHT - 1, -1, -1):
        # print line at y
        print("".join(
            APPEARANCE[value] for value in
            (0 if (x, y) in current_blocks else field[y][x] for x in range(WIDTH))
        ))
        print()
        print("SCORE: {}".format(score))


if __name__ == '__main__':
    main()
