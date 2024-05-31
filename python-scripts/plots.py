import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

class Plots:
    def __init__(self, base_dir: str = None, base_dir_plots: str = None):
        self.__base_dir       = base_dir
        self.__base_dir_plots = base_dir_plots
        
        self.__count_job          = {}
        self.__frequency_job      = {}
        self.__count_mappers      = {}
        self.__frequency_mappers  = {}
        self.__count_reducers     = {}
        self.__frequency_reducers = {}

        self.__count_mappers_to_plot      = {}
        self.__frequency_mappers_to_plot  = {}
        self.__count_reducers_to_plot     = {}
        self.__frequency_reducers_to_plot = {}


    def process_job(self, job_type: str):
        """Processes a directory based on the job type (count or frequency).

        Args:
            job_type (str): The type of job directory to process ('count' or 'frequency').
        """

        if job_type != 'count' and job_type != 'frequency':
            print(f"job_type must be 'count' or 'frequency' ({job_type} instead)")
            return

        if os.path.isdir(self.__base_dir):
            for dir in os.listdir(self.__base_dir):
                # Check if subdirectory contains the specific job type subdirectory
                if os.path.isdir(os.path.join(self.__base_dir, dir)) and f"{job_type}_job" in os.listdir(os.path.join(self.__base_dir, dir)):
                    job_dir = os.path.join(self.__base_dir, dir, f"{job_type}_job")

                    for filename in os.listdir(job_dir):
                        if filename.endswith(".csv"):
                            filepath = os.path.join(job_dir, filename)

                            # Read CSV, create DataFrame, and store with filename (without extension) as key
                            df = pd.read_csv(filepath)

                            if job_type == 'count':
                                self.__count_job[filename[:-4]] = df
                            else:
                                self.__frequency_job[filename[:-4]] = df

    
    def process_mappers(self, job_type: str):
        """Processes a directory based on the job type (count or frequency).

        Args:
            job_type (str): The type of job directory to process ('count' or 'frequency').
        """

        if job_type != 'count' and job_type != 'frequency':
            print(f"job_type must be 'count' or 'frequency' ({job_type} instead)")
            return

        if os.path.isdir(self.__base_dir):
            for dir in os.listdir(self.__base_dir):
                if os.path.isdir(os.path.join(self.__base_dir, dir)) and f"{job_type}_mappers" in os.listdir(os.path.join(self.__base_dir, dir)):
                    job_dir = os.path.join(self.__base_dir, dir, f"{job_type}_mappers")

                    for filename in os.listdir(job_dir):
                        if filename.endswith(".csv"):
                            filepath = os.path.join(job_dir, filename)

                            df = pd.read_csv(filepath)
                            key = str(dir) + "_" + filename[-5:-4]

                            if job_type == 'count':
                                if key in self.__count_mappers:
                                    self.__count_mappers[key] = pd.concat([self.__count_mappers[key], df], ignore_index=True)
                                else:
                                    self.__count_mappers[key] = df
                            else:
                                if key in self.__frequency_mappers:
                                    self.__frequency_mappers[key] = pd.concat([self.__frequency_mappers[key], df], ignore_index=True)
                                else:
                                    self.__frequency_mappers[key] = df

    
    def process_reducers(self, job_type: str):
        """Processes a directory based on the job type (count or frequency).

        Args:
            job_type (str): The type of job directory to process ('count' or 'frequency').
        """

        if job_type != 'count' and job_type != 'frequency':
            print(f"job_type must be 'count' or 'frequency' ({job_type} instead)")
            return

        if os.path.isdir(self.__base_dir):
            for dir in os.listdir(self.__base_dir):
                if os.path.isdir(os.path.join(self.__base_dir, dir)) and f"{job_type}_reducers" in os.listdir(os.path.join(self.__base_dir, dir)):
                    job_dir = os.path.join(self.__base_dir, dir, f"{job_type}_reducers")

                    for filename in os.listdir(job_dir):
                        if filename.endswith(".csv"):
                            filepath = os.path.join(job_dir, filename)

                            df = pd.read_csv(filepath)
                            key = str(dir) + "_" + filename[-5:-4]

                            if job_type == 'count':
                                if key in self.__count_reducers:
                                    self.__count_reducers[key] = pd.concat([self.__count_reducers[key], df], ignore_index=True)
                                else:
                                    self.__count_reducers[key] = df
                            else:
                                if key in self.__frequency_reducers:
                                    self.__frequency_reducers[key] = pd.concat([self.__frequency_reducers[key], df], ignore_index=True)
                                else:
                                    self.__frequency_reducers[key] = df

    def mean_exec_time_mappers(self, job_type: str):
        """Calculate the mean execution time of the mappers based on the job type (count or frequency).

        Args:
            job_type (str): The type of job to process ('count' or 'frequency').
        """

        if job_type != 'count' and job_type != 'frequency':
            print(f"job_type must be 'count' or 'frequency' ({job_type} instead)")
            return

        if job_type == 'count':
            for df in self.__count_mappers:
                mean_exec_time = self.__count_mappers[df].iloc[:, 1].mean()  # Calculate mean of 'time' column
                key = df[:-2]
                if key in self.__count_mappers_to_plot:
                    self.__count_mappers_to_plot[key].append(mean_exec_time) 
                else:
                    self.__count_mappers_to_plot[key] = [mean_exec_time]
        else:
            for df in self.__frequency_mappers:
                mean_exec_time = self.__frequency_mappers[df].iloc[:, 1].mean()  # Calculate mean of 'time' column
                key = df[:-2]
                if key in self.__frequency_mappers_to_plot:
                    self.__frequency_mappers_to_plot[key].append(mean_exec_time) 
                else:
                    self.__frequency_mappers_to_plot[key] = [mean_exec_time]
    

    def mean_exec_time_reducers(self, job_type: str):
        """Calculate the mean execution time of the reducers based on the job type (count or frequency).

        Args:
            job_type (str): The type of job to process ('count' or 'frequency').
        """

        if job_type != 'count' and job_type != 'frequency':
            print(f"job_type must be 'count' or 'frequency' ({job_type} instead)")
            return

        if job_type == 'count':
            for df in self.__count_reducers:
                mean_exec_time = self.__count_reducers[df].iloc[:, 1].mean()  # Calculate mean of 'time' column
                key = df[:-2]
                if key in self.__count_reducers_to_plot:
                    self.__count_reducers_to_plot[key].append(mean_exec_time) 
                else:
                    self.__count_reducers_to_plot[key] = [mean_exec_time]
        else:
            for df in self.__frequency_reducers:
                mean_exec_time = self.__frequency_reducers[df].iloc[:, 1].mean()  # Calculate mean of 'time' column
                key = df[:-2]
                if key in self.__frequency_reducers_to_plot:
                    self.__frequency_reducers_to_plot[key].append(mean_exec_time) 
                else:
                    self.__frequency_reducers_to_plot[key] = [mean_exec_time]

    
    def plot_mean_exec_time_jobs(self):
        """Plot the mean execution time of count and frequency jobs using a stacked bar chart."""

        # Calculate mean execution time for count jobs
        mean_exec_time_count = {key: df.iloc[:, 1].mean() for key, df in self.__count_job.items()}

        # Calculate mean execution time for frequency jobs
        mean_exec_time_frequency = {key: df.iloc[:, 1].mean() for key, df in self.__frequency_job.items()}

        # Sort dictionaries by mean execution time
        sorted_mean_exec_time_count = dict(sorted(mean_exec_time_count.items(), key=lambda item: item[1]))
        sorted_mean_exec_time_frequency = dict(sorted(mean_exec_time_frequency.items(), key=lambda item: item[1]))

        # Prepare data for plotting
        labels_count = list(sorted_mean_exec_time_count.keys())
        means_count = list(sorted_mean_exec_time_count.values())
        labels_frequency = list(sorted_mean_exec_time_frequency.keys())
        means_frequency = list(sorted_mean_exec_time_frequency.values())

        # Create bar chart
        plt.figure(figsize=(10, 6))
        means_count_bars = plt.bar(labels_count, means_count, color='blue', label='Count Job')
        means_frequency_bars = plt.bar(labels_frequency, means_frequency, bottom=means_count, color='orange', label='Frequency Job')

        # Add title and labels
        plt.title('Mean Execution Time of Jobs', fontweight='bold')
        plt.ylabel('Mean Execution Time [s]', fontweight='bold')
        plt.xticks(rotation=45)
        plt.legend()

        # Add data labels
        for bar_count, bar_frequency in zip(means_count_bars, means_frequency_bars):
            height_count = bar_count.get_height()
            height_frequency = bar_frequency.get_height()
            plt.text(bar_count.get_x() + bar_count.get_width() / 2.0, height_count, f'{height_count:.2f}', ha='center', va='bottom')
            plt.text(bar_frequency.get_x() + bar_frequency.get_width() / 2.0, height_count + height_frequency, f'{height_frequency:.2f}', ha='center', va='bottom')

        # Save plot
        plt.savefig(f'{self.__base_dir_plots}/mean_exec_time_jobs.png')
        plt.close()

    
    def plot_mappers(self, job_type: str):
        if job_type != 'count' and job_type != 'frequency':
            print(f"job_type must be 'count' or 'frequency' ({job_type} instead)")
            return

        if job_type == 'count':
            # Calculate the mean of each list in the dictionary
            mean_exec_times = {key: np.mean(values) for key, values in self.__count_mappers_to_plot.items()}

            # Prepare the data for plotting
            labels = list(mean_exec_times.keys())
            means = list(mean_exec_times.values())

            # Create the bar chart
            plt.figure(figsize=(10, 6))
            plt.bar(labels, means, color=['blue', 'orange', 'green', 'red'])

            # Add title and labels
            plt.title('Job Count - Mappers - Mean Execution Time by Dataset Size', fontweight='bold')
            plt.xlabel('Dataset Size', fontweight='bold')
            plt.ylabel('Mean Execution Time [s]', fontweight='bold')

            plt.savefig(f'{self.__base_dir_plots}/mean_exec_time_count_mappers.png')
            plt.close()
        else:
            mean_exec_times = {key: np.mean(values) for key, values in self.__frequency_mappers_to_plot.items()}

            # Prepare the data for plotting
            labels = list(mean_exec_times.keys())
            means = list(mean_exec_times.values())

            # Create the bar chart
            plt.figure(figsize=(10, 6))
            plt.bar(labels, means, color=['blue', 'orange', 'green', 'red'])

            # Add title and labels
            plt.title('Job Frequency - Mappers - Mean Execution Time by Dataset Size', fontweight='bold')
            plt.xlabel('Dataset Size', fontweight='bold')
            plt.ylabel('Mean Execution Time [s]', fontweight='bold')

            plt.savefig(f'{self.__base_dir_plots}/mean_exec_time_frequency_mappers.png')
            plt.close()

    
    def plot_reducers(self, job_type: str):
        if job_type != 'count' and job_type != 'frequency':
            print(f"job_type must be 'count' or 'frequency' ({job_type} instead)")
            return

        if job_type == 'count':
            # Calculate the mean of each list in the dictionary
            mean_exec_times = {key: np.mean(values) for key, values in self.__count_reducers_to_plot.items()}

            # Prepare the data for plotting
            labels = list(mean_exec_times.keys())
            means = list(mean_exec_times.values())

            # Create the bar chart
            plt.figure(figsize=(10, 6))
            plt.bar(labels, means, color=['blue', 'orange', 'green', 'red'])

            # Add title and labels
            plt.title('Job Count - Reducers - Mean Execution Time by Dataset Size', fontweight='bold')
            plt.xlabel('Dataset Size', fontweight='bold')
            plt.ylabel('Mean Execution Time [s]', fontweight='bold')

            plt.savefig(f'{self.__base_dir_plots}/mean_exec_time_count_reducers.png')
            plt.close()
        else:
            mean_exec_times = {key: np.mean(values) for key, values in self.__frequency_reducers_to_plot.items()}

            # Prepare the data for plotting
            labels = list(mean_exec_times.keys())
            means = list(mean_exec_times.values())

            # Create the bar chart
            plt.figure(figsize=(10, 6))
            plt.bar(labels, means, color=['blue', 'orange', 'green', 'red'])

            # Add title and labels
            plt.title('Job Frequency - Reducers - Mean Execution Time by Dataset Size', fontweight='bold')
            plt.xlabel('Dataset Size', fontweight='bold')
            plt.ylabel('Mean Execution Time [s]', fontweight='bold')

            plt.savefig(f'{self.__base_dir_plots}/mean_exec_time_frequency_reducers.png')
            plt.close()


    def plot_mappers_and_reducers(self, job_type: str):
        if job_type != 'count' and job_type != 'frequency':
            print(f"job_type must be 'count' or 'frequency' ({job_type} instead)")
            return

        if job_type == 'count':
            mean_exec_times_mappers = {key: np.mean(values) for key, values in self.__count_mappers_to_plot.items()}
            mean_exec_times_reducers = {key: np.mean(values) for key, values in self.__count_reducers_to_plot.items()}
            title = 'Job Count - Mean Execution Time by Dataset Size'
            filename = 'mean_exec_time_count_mappers_reducers.png'
        else:
            mean_exec_times_mappers = {key: np.mean(values) for key, values in self.__frequency_mappers_to_plot.items()}
            mean_exec_times_reducers = {key: np.mean(values) for key, values in self.__frequency_reducers_to_plot.items()}
            title = 'Job Frequency - Mean Execution Time by Dataset Size'
            filename = 'mean_exec_time_frequency_mappers_reducers.png'

        # Sort dictionaries by mean execution time
        sorted_mean_exec_times_mappers = dict(sorted(mean_exec_times_mappers.items(), key=lambda item: item[1]))
        sorted_mean_exec_times_reducers = dict(sorted(mean_exec_times_reducers.items(), key=lambda item: item[1]))

        # Prepare the data for plotting
        labels = list(sorted_mean_exec_times_mappers.keys())
        means_mappers = list(sorted_mean_exec_times_mappers.values())
        means_reducers = list(sorted_mean_exec_times_reducers.values())

        x = np.arange(len(labels))  # Label locations
        width = 0.35  # Width of the bars

        # Create the bar chart
        fig, ax = plt.subplots(figsize=(12, 7))
        bars1 = ax.bar(x - width/2, means_mappers, width, label='Mappers', color='blue')
        bars2 = ax.bar(x + width/2, means_reducers, width, label='Reducers', color='orange')

        # Add some text for labels, title, and custom x-axis tick labels
        ax.set_xlabel('Dataset Size', fontweight='bold')
        ax.set_ylabel('Mean Execution Time [s]', fontweight='bold')
        ax.set_title(title, fontweight='bold')
        ax.set_xticks(x)
        ax.set_xticklabels(labels)
        ax.legend()

        # Add data labels on top of the bars
        for bars in [bars1, bars2]:
            for bar in bars:
                height = bar.get_height()
                ax.text(bar.get_x() + bar.get_width() / 2.0, height, f'{height:.2f}', ha='center', va='bottom')

        plt.xticks(rotation=45)
        plt.tight_layout()

        plt.savefig(f'{self.__base_dir_plots}/{filename}')
        plt.close()


#####################################################################################################################################################


def main():
    plots = Plots(base_dir='../tests/in-mapper', base_dir_plots='../plots/in-mapper')
    
    print("##########   JOB    ##########")
    plots.process_job('count')
    plots.process_job('frequency')
    plots.plot_mean_exec_time_jobs()
    
    print("########## MAPPERS  ##########")
    plots.process_mappers('count')
    plots.process_mappers('frequency')
    plots.mean_exec_time_mappers('count')
    plots.mean_exec_time_mappers('frequency')

    print("########## REDUCERS ##########")
    plots.process_reducers('count')
    plots.process_reducers('frequency')
    plots.mean_exec_time_reducers('count')
    plots.mean_exec_time_reducers('frequency')

    plots.plot_mappers('count')
    plots.plot_mappers('frequency')

    plots.plot_reducers('count')
    plots.plot_reducers('frequency')

    plots.plot_mappers_and_reducers('count')
    plots.plot_mappers_and_reducers('frequency')


#####################################################################################################################################################


if __name__ == '__main__':
    main()