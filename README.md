# polarflow
Framework for building pipelines with Polars in easy way.


How to start:

```bash
python -m venv .venv
source .venv/source/activate
pip install poetry
poetry install
```



## Usecases:

### Datasources
- Files 
- Databases
- Cloud object storage

### Common data operations
- Changes in schema
    - Select columns
    - Rename columns
    - Change data types
- Filtering - where filter
- Limiting number of rows - limit/indexing ?


### Base use cases

1. Convert data from one format to another
    - Direct conversion
    - With transformation/common data transformations

2. Join Different datasets
    - Join types
    - Join on [column/s or condition]
    - With transformation/common data transformations

3. Grouping
    - Groups and aggregations

