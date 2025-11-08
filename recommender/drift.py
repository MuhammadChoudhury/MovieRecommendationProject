# import pandas as pd
from scipy.stats import ks_2samp
import matplotlib.pyplot as plt
from recommender.train import load_movielens_data

def detect_rating_drift():

    print("--- Running Drift Detection ---")
    data = load_movielens_data()
    data = data.sort_values('timestamp')

    split_point = int(len(data) * 0.5)
    reference_data = data.iloc[:split_point]
    current_data = data.iloc[split_point:]

    ks_statistic, p_value = ks_2samp(reference_data['rating'], current_data['rating'])

    print(f"K-S Statistic: {ks_statistic:.4f}")
    print(f"P-value: {p_value:.4f}")

    alpha = 0.05 
    if p_value < alpha:
        print(" Drift Detected! The distribution of ratings has changed significantly.")
    else:
        print("No significant drift detected.")

    plt.figure(figsize=(10, 6))
    reference_data['rating'].hist(bins=[0.5, 1.5, 2.5, 3.5, 4.5, 5.5], density=True, alpha=0.6, label='Reference Period')
    current_data['rating'].hist(bins=[0.5, 1.5, 2.5, 3.5, 4.5, 5.5], density=True, alpha=0.6, label='Current Period')
    plt.title('Distribution of Movie Ratings (Drift Check)')
    plt.xlabel('Rating')
    plt.ylabel('Proportion')
    plt.legend()
    plt.savefig('drift_chart.png')
    print("Drift chart saved to drift_chart.png")

if __name__ == "__main__":
    detect_rating_drift()