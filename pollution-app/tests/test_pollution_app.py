from pollution_app import __version__


def test_version():
    assert __version__ == '0.1.0'


def test_add_two_ints():
    # Arrange
    int1 = 2
    int2 = 4

    # Act
    total = int1 + int2

    # Assert
    assert total == 6

