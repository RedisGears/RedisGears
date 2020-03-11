import pytest

def pytest_addoption(parser):
    parser.addoption(
        '--image',
        action='store',
        default='redislabs/redisgears:edge',
        help='Docker image to use for testing',
    )

@pytest.fixture(scope='module')
def image(request):
    return request.config.getoption("--image")
