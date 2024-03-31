import os
import pytest

from flowsome.operators import ReadOperator, TransformOperator, WriteOperator
from flowsome.pipeline import Pipeline


@pytest.fixture
def _tmp_csv_file():
    file_path = r"tests\data\output.csv"
    yield file_path
    if os.path.isfile(file_path):
        os.remove(file_path)

def test_can_execute_simple_pipeline(_tmp_csv_file):
    read_op = ReadOperator("test", "csv", source= r"tests\data\sample.csv")
    t_filter = TransformOperator("test", "filter", Country="Cyprus")
    t_limit = TransformOperator("test", "limit", n=10)
    t_select = TransformOperator("test", "select", ["Last Name", "Country", "City","Phone 1","Email","Subscription Date"])
    write_op = WriteOperator("test", "csv", path= _tmp_csv_file, maintain_order=False)
    p = Pipeline(read_op, t_filter, t_limit, t_select, write_op)
    p.run()
    assert os.path.isfile(_tmp_csv_file)