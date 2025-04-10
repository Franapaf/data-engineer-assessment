import utils.database.database as db
from transforms.transform import transform_data, obtain_metrics
from utils.spark_session import get_spark_session
from ingestion.loader import load_data
from visualizations.visualizer import visualize_metrics

def main():
    spark = get_spark_session()
    
    db.create_database_if_not_exists("sales_data")
    db.create_table_if_not_exists()
    
    raw_df = load_data("./data/", spark)
    df = raw_df.transform(transform_data)
    
    metrics_dict = obtain_metrics(df)

    db.insert_to_postgres(df, "ingested_sales")
    for metric_name, metric_df in metrics_dict.items():
        table_name = f"metrics_{metric_name}"
        db.insert_to_postgres(metric_df, table_name=table_name)

    
    visualize_metrics(metrics_dict)

    df.unpersist()

if __name__ == "__main__":
    main()


