import matplotlib.pyplot as plt

class Counter:
    def __init__(self, to_analyze: list=None):
        self.__letters = {'a','b','c','d','e',
                          'f','g','h','i','j',
                          'k','l','m','n','o',
                          'p','q','r','s','t',
                          'u','v','w','x','y',
                          'z'}
        self.__frequency = [0] * 26
        self.__count = 0
        self.__to_analyze = to_analyze
    
    def analyze(self):
        for word in self.__to_analyze:
            for l in word.lower():
                if l in self.__letters:
                    self.__frequency[ord(l) - ord('a')] += 1
                    self.__count += 1
    
    def results(self):
        return [freq / self.__count for freq in self.__frequency]

    def plot(self):
        plt.figure(figsize=(10, 6))
        plt.bar(range(1, 27), self.results(), tick_label=[chr(ord('a') + i) for i in range(26)])
        plt.xlabel('Letters')
        plt.ylabel('Frequency')
        plt.title('Frequency of Letters')
        plt.savefig('../plots/frequency.png')