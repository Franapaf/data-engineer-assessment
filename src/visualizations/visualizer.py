
import os
import matplotlib.pyplot as plt
import pandas as pd

def visualize_metrics(metrics_dict):
    output_dir = "./output"
    os.makedirs(output_dir, exist_ok=True)

    line_metrics = {
        "monthly_sales",
        "avg_sales_by_month",
        "best_hours"
    }

    for name, spark_df in metrics_dict.items():
        print(f"Generating {name} ...")
        try:
            pdf = pd.DataFrame(spark_df.collect(), columns=spark_df.columns)

            if pdf.empty or pdf.shape[1] < 2:
                print(f"Skipping '{name}': not enough data to plot.")
                continue

            x_col = pdf.columns[0]
            y_col = pdf.columns[1]

            plt.figure(figsize=(10, 5))

            if name in line_metrics:
                plt.plot(pdf[x_col], pdf[y_col], marker='o', linestyle='-', color='blue')
            else:
                plt.bar(pdf[x_col].astype(str), pdf[y_col], color='skyblue')

            plt.title(name.replace("_", " ").title())
            plt.xlabel(x_col)
            plt.ylabel(y_col)
            plt.xticks(rotation=45, ha="right")
            plt.tight_layout()

            file_path = os.path.join(output_dir, f"{name}.png")
            plt.savefig(file_path)
            print(f"Saved plot: {file_path}")

            plt.close()

        except Exception as e:
            print(f"Error visualizing '{name}': {e}")