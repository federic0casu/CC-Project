import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

class Plots:
    def __init__(self, base_dir: str = None, base_dir_plots: str = None):
        self.__base_dir       = base_dir
        self.__base_dir_plots = base_dir_plots


    def process_naive_job(self, filepath: str, flag=True):
        df = pd.read_csv(self.__base_dir + filepath)

        # Sort the dataframe by 'dim-dataset' for better visualization
        df = df.sort_values(by='dim-dataset')

        # Get unique dimensions
        dimensions = df['dim-dataset'].unique()

        # Filter data for each custom-input-split
        times_split_0 = df[df['custom-input-split'] == 0]['time']
        times_split_1 = df[df['custom-input-split'] == 1]['time']


        if flag is True:
            fig, ax = plt.subplots(figsize=(12, 8))
            bar_width = 0.35

            # Positions of the bars on the x-axis
            r1 = range(len(dimensions))
            r2 = [x + bar_width for x in r1]

            # Create bars
            bars1 = ax.bar(r1, times_split_0, color='blue', width=bar_width, edgecolor='grey', label='NO custom-input-split')
            bars2 = ax.bar(r2, times_split_1, color='orange', width=bar_width, edgecolor='grey', label='custom-input-split')

            # Add titles and labels
            ax.set_xlabel('Dataset Dimension', fontweight='bold')
            ax.set_ylabel('Execution Time (seconds)', fontweight='bold')
            ax.set_title('Execution Time by Dataset Dimension and Custom Input Split', fontweight='bold')

            # Add xticks on the middle of the group bars
            ax.set_xticks([r + bar_width / 2 for r in range(len(dimensions))])
            ax.set_xticklabels(dimensions)

            # Add a legend
            ax.legend()

            # Add data labels on top of the bars
            for bars in [bars1, bars2]:
                for bar in bars:
                    height = bar.get_height()
                    ax.text(bar.get_x() + bar.get_width() / 2.0, height, f'{height:.2f}', ha='center', va='bottom')

            plt.savefig(f'{self.__base_dir_plots}/mean_exec_time_naive.png')
            plt.close()

        # Return mean execution times for both splits
        return dimensions, times_split_0, times_split_1


    def process_combiner_job(self, filepath: str, flag=True):
        df = pd.read_csv(self.__base_dir + filepath)

        # Sort the dataframe by 'dim-dataset' for better visualization
        df = df.sort_values(by='dim-dataset')

        # Calculate the mean execution time for each combination of custom-input-split and dim-dataset
        mean_times = df.groupby(['custom-input-split', 'dim-dataset'])['time'].mean().reset_index()

        if flag is True:
            # Plot the results
            plt.figure(figsize=(10, 6))

            for split in mean_times['custom-input-split'].unique():
                subset = mean_times[mean_times['custom-input-split'] == split]
                plt.plot(subset['dim-dataset'], subset['time'], marker='o', label=f'custom-input-split={split}')

            plt.xlabel('Dimension of Dataset', fontweight='bold')
            plt.ylabel('Mean Execution Time (seconds)', fontweight='bold')
            plt.title('Mean Execution Time by Dimension of Dataset',  fontweight='bold')
            plt.legend()

            plt.savefig(f'{self.__base_dir_plots}/mean_exec_time_combiner.png')
            plt.close()

        # Return mean execution time
        return mean_times['time']


    def process_python_job(self, flag=True):
        data = pd.DataFrame({
            'dim-dataset': ['200MB', '400MB', '800MB', '1.2GB'],
            'mean_execution_time': [222.98733452300075, 466.4437762669986, 943.1108568630007, 1491.6195998449984]
        })

        if flag is True:
            # Plot the data
            plt.figure(figsize=(10, 6))
            plt.bar(data['dim-dataset'], data['mean_execution_time'], color='b')

            # Add labels and title
            plt.xlabel('Dimension of Dataset', fontweight='bold')
            plt.ylabel('Mean Execution Time (seconds)', fontweight='bold')
            plt.title('Mean Execution Time by Dimension of Dataset',  fontweight='bold')

            # Save the plot
            plt.savefig(os.path.join(self.__base_dir_plots, 'mean_exec_time_python.png'))
            plt.close()

        # Return mean execution time
        return data['mean_execution_time']


    def plot_combined_mean_exec_time(self):
        dimensions, naive_no_split_means, naive_split_means = self.process_naive_job('/naive/exec-time.csv', False)
        combiner_means = self.process_combiner_job('/combiner/exec-time.csv', False)
        python_means = self.process_python_job(False)

        bar_width = 0.2
        r1 = np.arange(len(dimensions))
        r2 = [x + bar_width for x in r1]
        r3 = [x + bar_width for x in r2]
        r4 = [x + bar_width for x in r3]

        plt.figure(figsize=(14, 8))

        bars1 = plt.bar(r1, naive_no_split_means, color='blue', width=bar_width, edgecolor='grey', label='Naive No CustomInputSplit')
        bars2 = plt.bar(r2, naive_split_means, color='orange', width=bar_width, edgecolor='grey', label='Naive CustomInputSplit')
        bars3 = plt.bar(r3, combiner_means, color='green', width=bar_width, edgecolor='grey', label='Combiner')
        bars4 = plt.bar(r4, python_means, color='red', width=bar_width, edgecolor='grey', label='Python')

        plt.xlabel('Dataset Dimension', fontweight='bold')
        plt.ylabel('Mean Execution Time (seconds)', fontweight='bold')
        plt.title('Mean Execution Time by Dataset Dimension and Job Type', fontweight='bold')
        plt.xticks([r + 1.5 * bar_width for r in range(len(dimensions))], dimensions)
        plt.legend()

        for bars in [bars1, bars2, bars3, bars4]:
            for bar in bars:
                height = bar.get_height()
                plt.text(bar.get_x() + bar.get_width() / 2.0, height, f'{height:.2f}', ha='center', va='bottom')

        plt.savefig(f'{self.__base_dir_plots}/combined_mean_exec_time.png')
        plt.show()



def main():
    print("##########   NAIVE  ##########")
    plots = Plots(base_dir='../tests/naive', base_dir_plots='../plots/naive')
    plots.process_naive_job('/exec-time.csv')

    print("########## COMBINER ##########")
    plots = Plots(base_dir='../tests/combiner', base_dir_plots='../plots/combiner')
    plots.process_combiner_job('/exec-time.csv')

    print("##########  PYTHON  ##########")
    plots = Plots(base_dir='../tests', base_dir_plots='../plots/python-imp')
    plots.process_python_job()

    print("########## COMBINED ##########")
    plots = Plots(base_dir='../tests', base_dir_plots='../plots')
    plots.plot_combined_mean_exec_time()


if __name__ == '__main__':
    main()
