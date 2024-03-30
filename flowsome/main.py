from flowsome.client import Pipeline, ReadOperation, TransformOperation, WriteOperation


if __name__ == "__main__":
    
    read_op = ReadOperation("test", "csv", source= r"tests\data\sample.csv")
    t_filter = TransformOperation("test", "filter", Country="Cyprus")
    t_limit = TransformOperation("test", "limit", n=10)
    write_op = WriteOperation("test", "csv", path= r"tests\data\output.csv")

    p = Pipeline(read_op, t_filter, t_limit, write_op)
    p.run()

    