import sys
import math
from pyspark import SparkContext

sc = SparkContext()

# important function definitions

def closestPoint(r, centers):
	dists = []

	for c in  centers:
		dists.append(distanceSquared(r,c))

	for i in range(len(dists)):
		if dists[i] == min(dists):
			return i

def addPoints(r, t):
	return [ r[0] + t[0], r[1] + t[1] ]

def distanceSquared(r, t):
	return (r[0] - t[0])**2 + (r[1] - t[1])**2

def average(r,a):
	return (r[0]/a,r[1]/a)


# create initial RDD using specified file

logfile = sys.argv[1]

data = sc.textFile(logfile)


# filter out irrelevant data

fData = data.filter(lambda x: "|" not in x and "/" not in x)


# create RDD containing latitude and longitude of geographical points, remove irrelevant points, and persist RDD for future use

latlong = fData.map(lambda x: (float(x.split(',')[12]),float(x.split(',')[13])))

latlong = latlong.filter(lambda x: x != (0,0))

latlong.persist()


# define k and the convergance distance, take a random sample to begin

K = 5

kPoints = latlong.takeSample(False, K, 42)

convergeDist = 0.1

tmp = float("inf")


# iteratively compute new k-means

while tmp > convergeDist:
	rdd1 = latlong.map(lambda x: (closestPoint(x,kPoints), (x, 1)))

	rdd2 = rdd1.reduceByKey(lambda x, y: (addPoints(x[0],y[0]), x[1] + y[1]))

	rdd3 = rdd2.map(lambda x: (x[0], average(x[1][0],x[1][1])))

	newCenters = rdd3.collect()
	
	tmp = 0.0

	for i in range(len(kPoints)):
		tmp += distanceSquared(newCenters[i][1],kPoints[i])
		kPoints[i] = newCenters[i][1]



print("\n\nThe final centers are: ")

for c in kPoints:
	print(c[0], c[1])

print("\n\n")
