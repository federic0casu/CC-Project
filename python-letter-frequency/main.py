from counter import Counter

import os
import timeit

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
        words = list(word_file.read().split())
        return words


def main():
    append_words('books/FICTION', 'to_analyze.txt')
    to_analyze = load_words('to_analyze.txt')
    
    counter = Counter(to_analyze)
    execution_time = timeit.timeit(lambda: counter.analyze(), number=10)
    print("Execution time:", execution_time, "seconds")

    counter.plot()


if __name__ == '__main__':
    main()