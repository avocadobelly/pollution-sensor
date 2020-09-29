__version__ = '0.1.0'


def main():
    x = int(input("Integer: "))
    y = int(input("Another Integer: "))
    z = add_two_ints(x, y)
    print(z)


def add_two_ints(int1, int2):
    total = int1 + int2
    return total

