from counter import Counter
import argparse
import os
import timeit
import csv

def append_words(directory, output_file_name):
    output_file_name = '../data/' + output_file_name
    directory = '../data/' + directory

    with open(output_file_name, 'a') as output_file:
        for file_name in os.listdir(directory):
            file_path = os.path.join(directory, file_name)
            if file_name.endswith('.txt'):
                with open(file_path, 'r') as in_file:
                    words = in_file.read().split()
                    for word in words:
                        output_file.write(word + '\n')

def load_words(file):
    with open(f'../data/{file}') as word_file:
        print(file)
        words = list(word_file.read().split())
        return words

def main():    
    parser = argparse.ArgumentParser(description='Word frequency calculation')

    parser.add_argument('-m', '--mode', type=str, help='password / books mode', default='books')
    parser.add_argument('-l', '--language', type=str, help='language', default='italian')
    
    # Parsing arguments
    args = parser.parse_args()
    append_words(args.mode+'/'+args.language, 'to_analyze.txt')
    to_analyze = load_words('to_analyze.txt')
    
    counter = Counter(to_analyze, args.language+"_"+args.mode)
    execution_time = timeit.timeit(lambda: counter.analyze(), number=10)
    print("Execution time:", execution_time, "seconds")
    csv_file_path = '../data/execution_time_python.csv'
    new_values = [os.path.getsize("../data/to_analyze.txt"), execution_time]

    with open(csv_file_path, mode='a', newline='') as file:
        writer = csv.writer(file)
        # Append the new values as a new row
        writer.writerow(new_values)
    
    counter.plot()
    os.remove("../data/to_analyze.txt")

if __name__ == '__main__':
    main()