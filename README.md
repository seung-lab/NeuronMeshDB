# NeuronMeshDB
Stores neuron meshes and manifests for fast and cheap reads / writes. This database uses BigTable and hides the column logic. Each entry is a single value.

# Usage

## Create table

The first time you load a table set `is_new=True`, eg.:
```
from neuronmeshdb import meshdb
mdb = meshdb.MeshDB(table_id="my_unique_table_name", is_new=is_new)
```

`is_new` reduces performance and uses quota but does not do any damage if used multiple times.

Make sure that you are happy with the default values for `instance_id` and `project_id`.

## Write to table

Use either `write_data` or `write_cv_data` to write data efficiently to the database. If you want to generate your own writing logic take a look at those and adapt accordingly.

Example for a list of dicts obtained with cloudvolume:
```
from neuronmeshdb import meshdb
mdb = meshdb.MeshDB(table_id="my_great_table_name")

mdb.write_cv_data(self, data_list)
```

## Read from table

Use either `read_byte_row` or `read_byte_rows` to read data from the databse. 

Example:
```
from neuronmeshdb import meshdb
mdb = meshdb.MeshDB(table_id="my_great_table_name")

rows = mdb.read_byte_rows(row_keys=my_row_keys)
```
