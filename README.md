Data pipelane built with PySpark, PostgreSQL to ingest,clean, transform, store and visualiza visualize sales data from CSV files

- Ingesting CSV files from a directory

- Cleaning and transforming the data using PySpark

- Calculating business metrics like monthly sales, top selling citys and states, best hours for sales, avarage quantity per product....

- Persisting results into a PostgreSQL database

- Generating visual reports (PNG) from aggregated metrics

All csv files to be ingested must be placed in the data folder.

After running the pipeline, all charts from the computed metrics will be automatically generated and saved as .png files in the output/ directory

Deployment
```bash
docker compose up
```

Run the pipeline

python src/main.py


