from asyncio import events
import csv
import random
from random import randint
from collections import deque


# Break the jobs into the following lists
# 1) 8 GB and less, can run on any CPU
# 2) over 8 but under 16 GB, can run only on 16GB CPUs,
# 3) over 16GB, cannot run anywhere and rejected. 
# Put jobs from list 1 on the 8GB CPUs, run them there as many as possible
# Put jobs from list 2 on 16 GB CPUs as they become available
# Put shorter jobs on slower CPUs if there are more jobs than fast CPUs
# As there are no more jobs from list 2, start placing jobs from list 1 on the 16GB CPUs


class Process:
    def __init__(self, PID, CPUCycles, MemorySize, Arrival):
        self.PID = PID
        self.CPUCycles = CPUCycles
        self.MemorySize = MemorySize
        self.RemCPUCycles = CPUCycles

        # statistics for each process
        self.arrivalTime = Arrival
        self.startTime = None
        self.completedTime = None

    def __str__(self):
        # returns a string for the process to be printed
        return "PID={}, Arrival={} CPUCycles={}/{}, Size={}, Started={}, Completed={}".format(self.PID, self.arrivalTime, self.RemCPUCycles, self.CPUCycles, self.MemorySize, self.startTime, self.completedTime)

class ProcessDoneEvent:
    def __init__(self, process, timestamp, cpu):
        self.process = process
        self.timestamp = timestamp
        self.cpu = cpu

class Simulation:
    def __init__(self):
        # 6 CPUs
        self.CPUs_8GB = ["Pa", "Pb", "Pc"]  # slow and small CPUs
        self.CPUs_16GB = ["Pd", "Pe", "Pf"]  # fast and large CPUs
        self.slowCPUFactor = 2 # a multiple for time to spend on slower CPU, a slow CPU is 2x slower, 2GHz vs 4GHz
        # need a round queue of CPUs
        self.idle8GBCPUs = deque(self.CPUs_8GB, maxlen=len(self.CPUs_8GB))
        self.idle16GBCPUs = deque(self.CPUs_16GB, maxlen=len(self.CPUs_16GB))

        self.incoming8GBProcesses = []
        self.incoming16GBProcesses = []

        self.contextSwitches = 0
        

        # list of completed processes
        self.completedProcess = []

        # a queue of events
        self.events = []
        # simulation time
        self.currentTime = 0

    def isDone(self):
        return len(self.incoming8GBProcesses) == 0 and \
                len(self.incoming16GBProcesses) == 0 and \
                len(self.idle8GBCPUs) == len(self.CPUs_8GB) and  len(self.idle16GBCPUs) == len(self.CPUs_16GB)

    def run(self):
        # sort incoming processes by time, shortest jobs first
        self.incoming8GBProcesses.sort(key=lambda p: (p.CPUCycles, p.PID))
        self.incoming16GBProcesses.sort(key=lambda p: (p.CPUCycles, p.PID))
        while not self.isDone():
            # check if 16GB jobs is ready and 16GB CPU is availaable
            if len(self.incoming16GBProcesses) > 0 and len(self.idle16GBCPUs) > 0:
                # try to load the fast CPU
                # get the CPU
                cpu = self.idle16GBCPUs.popleft()
                # get that shortest remaining process
                process = self.incoming16GBProcesses.pop(0)
                # set the time when the process is complete
                process.startTime = self.currentTime
                print("Simo time {}: Process {} starts on CPU {}".format(self.currentTime, process, cpu))
                # setup an event to get the job off the CPU
                completedEvent = ProcessDoneEvent(process, self.currentTime + process.RemCPUCycles, cpu)
                # put an event to the queue
                self.events.append(completedEvent)
                self.contextSwitches += 1
            # now assign 8GB jobs, can assing if 8GB CPU is availabe or 16GB CPU is available
            elif len(self.incoming8GBProcesses) > 0 and (len(self.idle16GBCPUs) > 0 or len(self.idle8GBCPUs) > 0):
                # get the CPU
                if len(self.idle16GBCPUs) > 0 and (len(self.incoming8GBProcesses) <= len(self.idle16GBCPUs) or len(self.idle8GBCPUs) == 0):
                    # this job can run on any CPU, but prefer the fast CPU
                    # but only if there are no more jobs than fast CPUs available, or slow slow CPUs avail
                    # if the fast CPU is available at this point, this means no more 16GB jobs
                    cpu = self.idle16GBCPUs.popleft()
                    cpuTimeMultiplier = 1
                else:
                    cpu = self.idle8GBCPUs.popleft()
                    cpuTimeMultiplier = self.slowCPUFactor
                # get that shortest remaining process
                process = self.incoming8GBProcesses.pop(0)
                # set the time when the process is complete
                process.startTime = self.currentTime
                print("Simo time {}: Process {} starts on CPU {}".format(self.currentTime, process, cpu))
                # setup the event when the job is done
                completedEvent = ProcessDoneEvent(process, self.currentTime + process.RemCPUCycles * cpuTimeMultiplier, cpu)
                # put an event to the queue
                self.events.append(completedEvent)
                self.contextSwitches += 1
            else:
                # no more idle CPUs or processes
                # check the the even when the next process is complete
                # find the event with the smallest time
                minTimeEventIndex = 0
                for i, ev in enumerate(self.events):
                    if ev.timestamp < self.events[minTimeEventIndex].timestamp:
                        minTimeEventIndex = i
                minTimeEvent = self.events[minTimeEventIndex]
                # remove the event from queue
                self.events.pop(minTimeEventIndex)
                # advance our simulation time to the event
                self.currentTime = minTimeEvent.timestamp
                process = minTimeEvent.process
                cpu = minTimeEvent.cpu

                # put the process to completed
                process.RemCPUCycles = 0
                # completed at this time of the simulation
                process.completedTime = self.currentTime 
                self.completedProcess.append(process)
                print("Simo time {}: Process {} completes on CPU {}".format(self.currentTime, process, cpu))
                # put the CPU back to appropriate idle
                if cpu in self.CPUs_8GB:
                    self.idle8GBCPUs.append(cpu)
                else:
                    self.idle16GBCPUs.append(cpu)
                    # fast CPU is available, see if there is anything on slow CPU to switch
                    # check if it possible to switch a context of a process from slower CPU
                    if len(self.incoming16GBProcesses) == 0 and len(self.idle8GBCPUs) < len(self.CPUs_8GB):
                        # something is running on the slow CPU, lets find it
                        slowCPUEventIndex = None
                        for i, ev in enumerate(self.events):
                            # if the event occurs after the current time, meaning the process is still running
                            if ev.timestamp > self.currentTime and ev.cpu in self.CPUs_8GB:
                                # found this event at index i
                                slowCPUEventIndex = i
                        # if found the latest possible event to offload
                        if slowCPUEventIndex is not None:
                            slowCPUEvent = self.events.pop(slowCPUEventIndex)
                            process = slowCPUEvent.process
                            cpu = slowCPUEvent.cpu
                            # calculate how many cycles are left
                            process.RemCPUCycles -= (self.currentTime - process.startTime) // self.slowCPUFactor
                            # return the slow CPU
                            self.idle8GBCPUs.append(cpu)
                            # get the fast CPU
                            cpu = self.idle16GBCPUs.popleft()
                            # assign the process to the fast CPU
                            print("Simo time {}: Process {} starts on CPU {}".format(self.currentTime, process, cpu))
                            # time is * by burstTimeMultiplier, to set the process for x2 time on slower CPU
                            completedEvent = ProcessDoneEvent(process, self.currentTime + process.RemCPUCycles, cpu)
                            self.events.append(completedEvent)
                            self.contextSwitches += 1






    def printStats(self):
        print("Statistics:")
        nProcesses = len(self.completedProcess)
        totalWaitTime = 0
        totalTurnAroundTime = 0
        for process in self.completedProcess:
            turnAroundTime = process.completedTime - process.arrivalTime
            totalTurnAroundTime += turnAroundTime
            waitTime = turnAroundTime - process.CPUCycles
            totalWaitTime += waitTime
        # print the averages
        print("Average wait time (seconds at 4GHz): {}".format(totalWaitTime / nProcesses / 4E9))
        print("Average turnaround time (seconds at 4GHz): {}".format(totalTurnAroundTime / nProcesses / 4E9))
        print("Context switches: {}".format(self.contextSwitches))





if __name__ == "__main__":
    # create a simulation instance
    simo = Simulation()
    with open('processes8_16_arrival.csv') as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        csvit = iter(csv_reader)
        csvHeader = next(csvit)
        for PID, CPUCycles, MemorySize, Arrival in csvit:
            PID = int(PID)
            CPUCycles = int(CPUCycles)
            MemorySize = int(MemorySize)
            Arrival = int(Arrival)
            # create a process, place in the queue
            if MemorySize <= 8:
                simo.incoming8GBProcesses.append(Process(PID, CPUCycles, MemorySize, Arrival))
            elif MemorySize <= 16:
                simo.incoming16GBProcesses.append(Process(PID, CPUCycles, MemorySize, Arrival))
            else:
                print("Rejecting Process {} with memory size {} exceeding 16 GB".format(PID, MemorySize))
    # processes are read, ready to run
    simo.run()
    simo.printStats()
          
