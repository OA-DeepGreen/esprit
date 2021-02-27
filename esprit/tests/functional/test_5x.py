from unittest import TestCase
import esprit, time

TEST_CONN = esprit.raw.Connection("http://localhost:9200", "test")
esprit.raw.delete(TEST_CONN)


class TestDAO(esprit.dao.DomainObject):
    __type__ = 'index'
    __conn__ = TEST_CONN
    __es_version__ = "5.1.1"


class Test5x(TestCase):
    def setUp(self):
        super(Test5x, self).setUp()

    def tearDown(self):
        super(Test5x, self).tearDown()
        esprit.raw.delete(TEST_CONN)
        time.sleep(1)

    def test_01_dao(self):
        do = TestDAO({"whatever": "value"})
        do.save(blocking=True)

    def test_02_mapping(self):
        pass
