import numpy
from scipy.spatial import distance
import re

f = open('sentences.txt', 'r')
lines = ""
sentences = []

c = 0
for line in f:
	sentences.append(re.split('[^a-z]', line.lower()))
	#print(sentences)
	for i in range(sentences[c].count('')):
		sentences[c].remove('')
	lines += (line.lower())
	c += 1
# print(lines)

tokens = re.split('[^a-z]', lines)
for i in range(tokens.count('')):
	tokens.remove('')

word_ind = dict.fromkeys(tokens, -1)
c = 0
for num in tokens:
	if word_ind[num] == -1:
		word_ind[num] = c
		c += 1
#print(word_ind)

matrix = []
for i in range(22):
	matrix.append([0] * 254)

c = 0
for sentence in sentences:
	for word in sentence:
		matrix[c][word_ind[word]] += 1
	c += 1
print(matrix)

distances =[]
for vector in matrix:
	distances.append(distance.cosine(matrix[0], vector))
print(distances)

distances.pop(0)
f_answer = open("3.txt", "w")
f_answer.write(str(distances.index(min(distances)) + 1) + " ")
distances.pop(distances.index(min(distances)))
f_answer.write(str(distances.index(min(distances)) + 1))