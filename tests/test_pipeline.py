import pytest
from flowsome.pipeline import Pipeline
from flowsome.tasks import *
import os


def _create_single_path_pipeline(input_file_path, output_file_path):
    pipe = Pipeline()
    reader = ReadTask("r1", source=input_file_path)
    transformer1 = TransformTask("t1", "filter", Country="Cyprus")
    transformer2 = TransformTask("t2", "limit", n=1)
    writer = WriteTask("w1", file_path=output_file_path)
    pipe.add_edge(reader, transformer1)
    pipe.add_edge(transformer1, transformer2)
    pipe.add_edge(transformer2, writer)
    return pipe


def _create_multi_source_pipeline(input_file_path, output_file_path):
    p = Pipeline()
    r1 = ReadTask("r1", source=input_file_path)
    t1 = TransformTask("t1", "filter", Country="Cyprus")
    w1 = WriteTask("w1", file_path=output_file_path)

    r2 = ReadTask("r2", source=input_file_path)
    t2 = TransformTask("t2", "limit", n=44)

    m = MergeTask("m", on="Email", how="right")
    t3 = TransformTask("t3", "limit", n=41)

    r3 = ReadTask("r3", source=input_file_path)
    t4 = TransformTask("t4", "limit", n=3)
    m2 = MergeTask("m2", on="Email", how="right", suffix="m2")

    p.add_edge(r1, t1)
    p.add_edge(t1, m)

    p.add_edge(r2, t2)
    p.add_edge(t2, m)

    p.add_edge(m, t3)
    p.add_edge(t3, m2)

    p.add_edge(r3, t4)
    p.add_edge(t4, m2)
    p.add_edge(m2, w1)

    return p


@pytest.fixture(params=["csv", "parquet", "json", "ipc"])
def output_tmp_file(request, tmp_path):
    file_format = request.param
    output_path = tmp_path / f"output.{file_format}"
    yield output_path  # Return the file path to the test
    # Teardown - delete the file after the test finishes
    output_path.unlink(missing_ok=True)


@pytest.fixture
def input_test_file():
    return r"tests/data/sample.csv"


def test_should_run_single_pipeline(output_tmp_file, input_test_file):
    """
    Test that a single pipeline can be run and the output file exists and is not empty.
    """
    pipe = _create_single_path_pipeline(input_test_file, output_tmp_file)
    pipe.run()
    assert (
        os.path.exists(output_tmp_file)
        and os.path.isfile(output_tmp_file)
        and os.path.getsize(output_tmp_file) > 0
    )


def test_should_run_multi_source_pipeline(output_tmp_file, input_test_file):
    pipe = _create_multi_source_pipeline(input_test_file, output_tmp_file)
    pipe.run()
    assert (
        os.path.exists(output_tmp_file)
        and os.path.isfile(output_tmp_file)
        and os.path.getsize(output_tmp_file) > 0
    )
