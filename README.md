Running oeci_data_manager.py without args will produce usage information.

The fist step is to initialize a new project.

oeci_data_manager.py init --source /mount/data/DX1234

Next, the data can be processed.

oeci_data_manager.py process --project DX1234

Scan can be used to see what needs processing. This is useful when new data
is added to the source directory.

oeci_data_manager.py scan --project DX1234
