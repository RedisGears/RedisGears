import pytest

def pytest_addoption(parser):
    parser.addoption(
        '--image',
        action='store',
        default=None,
        help='Docker image to use for testing',
    )

@pytest.fixture(scope='module')
def image(request):
    return request.config.getoption("--image")
