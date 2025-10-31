import os
import subprocess
import sys
import time

import matplotlib.pyplot as plt
import pandas as pd

# ---------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------
COUNTY_CODES = [55001, 55003, 55027, 55059, 55133]
CLIENT_CONTAINER = "p4-server-1"
HDFS_NN_CONTAINER = "p4-nn-1"

# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------
def cleanup_partitions():
    """Delete the /partitions directory in HDFS to ensure a clean start."""
    print("Attempting to delete HDFS directory '/partitions'...")
    command = ["docker", "exec", HDFS_NN_CONTAINER, "hdfs", "dfs", "-rm", "-r", "/partitions"]
    try:
        result = subprocess.run(
            command,
            check=True,
            capture_output=True,
            text=True,
            timeout=30
        )
        print("'/partitions' directory successfully deleted.")
        print(result.stdout.strip())
    except subprocess.CalledProcessError as e:
        if "No such file or directory" in e.stderr:
            print("'/partitions' directory does not exist, which is fine.")
        else:
            print(f"Error deleting '/partitions' directory:\n{e.stderr.strip()}", file=sys.stderr)
            sys.exit(1)
    except subprocess.TimeoutExpired:
        print("Timeout while trying to delete HDFS partitions.", file=sys.stderr)
        sys.exit(1)


def run_client_timed(county_code):
    """Run the client command and return the execution duration."""
    command = [
        "docker", "exec", CLIENT_CONTAINER,
        "python3", "/client.py", "CalcAvgLoan", "-c", str(county_code)
    ]
    start_time = time.monotonic()
    try:
        result = subprocess.run(
            command,
            check=True,
            capture_output=True,
            text=True,
            timeout=60
        )
        end_time = time.monotonic()
        duration = end_time - start_time
        print(f"  -> Success ({duration:.3f}s): {result.stdout.strip()}")
        return duration
    except subprocess.CalledProcessError as e:
        print(f"Client command failed for county {county_code}:\n{e.stderr.strip()}", file=sys.stderr)
        sys.exit(1)
    except subprocess.TimeoutExpired:
        print(f"Client command timed out for county {county_code}.", file=sys.stderr)
        sys.exit(1)

# ---------------------------------------------------------------------
# Main Execution
# ---------------------------------------------------------------------
def main():
    """Main script logic."""
    cleanup_partitions()

    create_times = []
    reuse_times = []

    print("\n--- Running Performance Analysis ---")
    for code in COUNTY_CODES:
        print(f"\nProcessing county code: {code}")
        
        # First call for a county code should create the partition
        print("Timing first call (source=create)...")
        create_duration = run_client_timed(code)
        create_times.append(create_duration)
        
        # Second call should reuse the existing partition
        print("Timing second call (source=reuse)...")
        reuse_duration = run_client_timed(code)
        reuse_times.append(reuse_duration)

    # Calculate averages
    avg_create = sum(create_times) / len(create_times)
    avg_reuse = sum(reuse_times) / len(reuse_times)

    print("\n--- Analysis Complete ---")
    print(f"Average 'create' time: {avg_create:.3f}s ({len(create_times)} samples)")
    print(f"Average 'reuse' time: {avg_reuse:.3f}s ({len(reuse_times)} samples)")
    
    # Ensure output directory exists
    os.makedirs('outputs', exist_ok=True)
    
    # Save results to CSV
    results_df = pd.DataFrame({
        'operation': ['create', 'reuse'],
        'time': [avg_create, avg_reuse]
    })
    
    csv_path = 'outputs/performance_results.csv'
    results_df.to_csv(csv_path, index=False, float_format='%.3f')
    print(f"Saved average timings to {csv_path}")

    # Generate and save bar chart
    png_path = 'outputs/performance_analysis.png'
    plt.style.use('ggplot')
    fig, ax = plt.subplots(figsize=(8, 6))

    operations = results_df['operation']
    times = results_df['time']
    bars = ax.bar(operations, times, color=['#e57373', '#81c784'])

    ax.set_ylabel('Average Time (seconds)')
    ax.set_title('Performance Comparison: Create vs. Reuse HDFS Partitions')
    ax.set_ylim(0, max(times) * 1.2)

    # Add labels on top of bars
    for bar in bars:
        yval = bar.get_height()
        ax.text(bar.get_x() + bar.get_width()/2.0, yval, f'{yval:.3f}s', va='bottom', ha='center', fontsize=12)
    
    fig.tight_layout()
    plt.savefig(png_path)
    print(f"Saved analysis chart to {png_path}")

if __name__ == "__main__":
    main()
