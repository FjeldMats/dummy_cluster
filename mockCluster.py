import ray
import time, sys
from ray.cluster_utils import Cluster

NUM_CPU = int(sys.argv[1])
# Starts a head-node for the cluster.
cluster = Cluster(initialize_head=True, head_node_args={
        "num_cpus": NUM_CPU})

ray.init(address=cluster.address, include_dashboard=None)

assert ray.cluster_resources()["CPU"] == NUM_CPU

@ray.remote(num_cpus=1)
def fast(x, t):
    time.sleep(2)
    print(f"finished fast {x} \t @ {round(time.time() - time_start,2)} s")
    return x

@ray.remote(num_cpus=2)
def slow(x, t):
    time.sleep(4)
    print(f"finished slow {x} \t @ {round(time.time() - time_start,2)} s")
    return x

@ray.remote(num_cpus=4)
def very_slow(x, t):
    time.sleep(8)
    print(f"finished very_slow {x} @ {round(time.time() - time_start,2)} s")
    return x

time_start = time.time()
results = []

start = 1
iter = int(sys.argv[2]) # length of short
end = iter

funcs = [fast, slow, very_slow]

for i in range(3):
    for j in range(start, end):
        results.append(funcs[i].remote(j, time_start))

    iter //= 2; start = end; end += iter

sum = sum([ray.get(i) for i in results]) # add up all returns of function calls

time_end = time.time() - time_start
time.sleep(5) # add some buffer time so all prints are shown

assert sum == ((start-1)*(start))/2 # validate the result (formula for triangular numbers n(n+1)/2 for sum of series of 1+2+3 ... + n)
print("duration =", time_end)
print("shutting down ...")
ray.shutdown()