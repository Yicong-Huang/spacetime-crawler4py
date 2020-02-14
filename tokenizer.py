import re
import sys
from collections import defaultdict
from typing import List, Dict

pattern = re.compile("[\W_]+")


def tokenize(line: str) -> List[str]:
    """
    Method/Function: List<Token> tokenize(TextFilePath)
    Write a method/function that reads in a text file and returns a list of the tokens in that file. For the
    purposes of this project, a token is a sequence of alphanumeric characters, independent of capitalization
    (so Apple, apple are the same token).

    Complexity: O(N)
    """
    tokens = []
    try:
        tokens += pattern.sub(' ', line).lower().split()
    except Exception as err:
        print(err)
    finally:
        return [token for token in tokens if len(token) > 1]


def compute_word_frequencies(tokens: List[str]) -> Dict[str, int]:
    """
    Method:        Map<Token,Count> computeWordFrequencies(List<Token>)
    Write another method/function that counts the number of occurrences of each token in the token list.

    Complexity: O(N)/Θ(N)
    """
    count = defaultdict(int)
    try:
        for token in tokens:
            count[token] += 1
    except:
        pass
    finally:
        return count


def print_freq(frequencies: Dict[str, int], n=-1) -> None:
    """
    Method:         void print(Frequencies<Token, Count>)
    Finally, write a method that prints out the word frequency counts onto the screen. The print out should
    be ordered by decreasing frequency. (so, highest frequency words first)

    Complexity: O(N)/Θ(N)
    """

    try:
        for token, count in list(sorted(frequencies.items(), key=lambda x: -x[1]))[:n]:
            print(f"{token} -- {count}")
    except:
        pass


if __name__ == '__main__':
    print_freq(compute_word_frequencies(tokenize(sys.argv[1])))
