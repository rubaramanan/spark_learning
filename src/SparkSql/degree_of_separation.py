from pyspark.sql import SparkSession

session = SparkSession.builder.appName('degree of separation').getOrCreate()

search_id = 5306
target_id = 14
sc = session.sparkContext
hitCounter = sc.accumulator(0)


def convertBDF(line: str):
    ids = line.split()
    hero_id = int(ids[0])
    connections = [int(id) for id in ids[1:]]

    ## WHITE - 2
    ## GRAY - 1
    ## BLACK - 0

    color = 2
    distance = 9999

    if hero_id == search_id:
        color = 1
        distance = 0
    return hero_id, (connections, distance, color)


def mapper(node: tuple):
    results = [node]
    character_id = node[0]
    connections, distance, color = node[1]

    # If this node needs to be expanded...
    if color == 1:
        for conn in connections:
            if conn == target_id:
                hitCounter.add(1)
            results.append((conn, ([], distance + 1, 1)))

        # We've processed this node, so color it black
        results[0] = (character_id, (connections, distance, 0))

    return results


def reducer(a, b):
    edges = list(set(a[0] + b[0]))
    distance = min(a[1], b[1])
    color = min(a[2], b[2])
    return edges, distance, color


lines = sc.textFile('/opt/bitnami/spark/data/Marvel+Graph')

bdfs = lines.map(convertBDF)
for i in range(10):
    print(f"BFS, iteration: {i+1}")
    mapped = bdfs.flatMap(mapper)

    # Note that mapped.count() action here forces the RDD to be evaluated, and
    # that's the only reason our accumulator is actually updated.
    print("Processing " + str(mapped.count()) + " values.")

    if (hitCounter.value > 0):
        print("Hit the target character! From " + str(hitCounter.value) \
              + " different direction(s).")
        break

    # Reducer combines data for each character ID, preserving the darkest
    # color and shortest path.
    reduced = mapped.reduceByKey(reducer)

session.stop()
