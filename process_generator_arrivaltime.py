import csv
import random
import matplotlib.pyplot as plt

def customNormalDistribution(upper_bound, lower_bound, mean, std_deviation):
    value = int(random.gauss(mean, std_deviation))
    result = value % (upper_bound - lower_bound + 1) + lower_bound
    return result


def average(list):
    return sum(list) / len(list)

# Parameter Declarations
number_of_processes = 250

# CPU Cycle Distribution Parameters
cpu_upper_bound = 10 ** 12
cpu_lower_bound = 10 ** 6
cpu_mean = 5 * 10 ** 11
cpu_std_deviation = 3 * 10 ** 11

# Arrival time Distribution Parameters
arrival_upper_bound = 10 ** 12
arrival_lower_bound = 0
arrival_mean = 5 * 10 ** 11
arrival_std_deviation = 3 * 10 ** 11

# Memory Requirement Distribution Parameters
memory_upper_bound = 16
memory_lower_bound = 1
memory_mean = 15
memory_std_deviation = 3

process_list = []

for i in range(number_of_processes):
    cpu_cycles = customNormalDistribution(cpu_upper_bound, cpu_lower_bound, cpu_mean, cpu_std_deviation)
    memory = customNormalDistribution(memory_upper_bound, memory_lower_bound, memory_mean, memory_std_deviation)

    arrival = customNormalDistribution(arrival_upper_bound, arrival_lower_bound, arrival_mean, arrival_std_deviation)
    process = [i + 1, cpu_cycles, memory, arrival]
    process_list.append(process)

# Open or Create File to Write Data
with open('processes8_16_arrival.csv', 'w', newline='') as csvfile:
    # Initialize Writer Class
    writer = csv.writer(csvfile)

    # Write Column Headers
    writer.writerow(['Process ID', 'CPU Cycles', 'Memory Requirement', 'Arrival Time'])
    # Write Process Data
    writer.writerows(process_list)

# Select Values from CPU Cycles Column Only
cpu_cycles = [row[1] for row in process_list]

# Create Historgram of CPU Cycles
plt.hist(cpu_cycles, 10)
# Add Title and Axis Labels
plt.title("CPU Cycle Distribution")
plt.xlabel('CPU Cycles')
plt.ylabel('Frequency')
# Display Histogram
plt.show()

# Select Values from Memory Requirement Column Only
memory = [row[2] for row in process_list]

# Create Histogram of Memory Requirements
plt.hist(memory, 10)
# Add Title and Axis Labels
plt.title("Memory Distribution")
plt.xlabel('Memory Requirement')
plt.ylabel('Frequency')
# Display Histogram
plt.show()

arrival = [row[3] for row in process_list]

# Create Histogram of Memory Requirements
plt.hist(arrival, 10)
# Add Title and Axis Labels
plt.title("Arrival Distribution")
plt.xlabel('Arrival Requirement')
plt.ylabel('Frequency')
# Display Histogram
plt.show()

# Print Calculated Average, Min Value & Max Value
print("CPU Cycles")
print("Average: ", average(cpu_cycles))
print("Min Value: ", min(cpu_cycles))
print("Max Value: ", max(cpu_cycles))

# Print Calculated Average, Min Value & Max Value
print("\nMemory Requirement")
print("Average: ", average(memory))
print("Min Value: ", min(memory))
print("Max Value: ", max(memory))
