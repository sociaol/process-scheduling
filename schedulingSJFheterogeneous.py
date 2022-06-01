from asyncio import events
import csv
import random
from random import randint
from collections import deque


# Heterogeneous CPU scheduling
# based on the SJF algorithm
# if there are more jobs than fast CPUs, assign to the slow CPU, the first shorter jobs.
# otherwise, assign to the fast CPU.
# Overall slow CPUs get shorther jobs first, then fast CPUs get larger jobs after that
# if a fast CPU becomes available and no more jobs are incoming
# switch the job from a slow CPU context to the faster CPU.


class Process:
    def __init__(self, PID, CPUCycles, MemorySize):
        self.PID = PID
        self.CPUCycles = CPUCycles
        self.MemorySize = MemorySize
        self.RemCPUCycles = CPUCycles

        # statistics for each process
        self.arrivalTime = 0
        self.startTime = None
        self.completedTime = None

    def __str__(self):
        # returns a string for the process to be printed
        return "PID={}, CPUCycles={}/{}, Size={}, Started={}, Completed={}".format(self.PID, self.RemCPUCycles, self.CPUCycles, self.MemorySize, self.startTime, self.completedTime)

class ProcessDoneEvent:
    def __init__(self, process, timestamp, cpu):
        self.process = process
        self.timestamp = timestamp
        self.cpu = cpu

class Simulation:
    def __init__(self):
        # 6 CPUs
        self.slowCPUs = ["Pa", "Pb", "Pc"]
        self.fastCPUs = ["Pd", "Pe", "Pf"]
        # need a round queue of CPUs
        self.idleSlowCPUs = deque(self.slowCPUs, maxlen=len(self.slowCPUs))
        self.idleFastCPUs = deque(self.fastCPUs, maxlen=len(self.fastCPUs))
        # a multiple for time to spend on slower CPU, a slow CPU is 2x slower, 2GHz vs 4GHz
        self.slowCPUFactor = 2 
        # incoming process queue, to be assigned to CPUs
        self.incomingProcesses = []

        self.contextSwitches = 0
        

        # list of completed processes
        self.completedProcess = []

        # a queue of events
        self.events = []
        # simulation time
        self.currentTime = 0

    def isDone(self):
        return len(self.incomingProcesses) == 0 and len(self.idleSlowCPUs) == len(self.slowCPUs) and  len(self.idleFastCPUs) == len(self.fastCPUs)

    def run(self):
        # sort incoming processes by time, shortest jobs first
        self.incomingProcesses.sort(key=lambda p: (p.CPUCycles, p.PID))
        while not self.isDone():
            # check if there is any CPU idle and a process incoming
            # check if there are more jobs than fast CPUs, and put shorter jobs to slower CPUs first
            if len(self.incomingProcesses) > len(self.idleFastCPUs) and len(self.idleSlowCPUs) > 0:
                # try to load the fast CPU
                # get the CPU
                cpu = self.idleSlowCPUs.popleft()
                burstTimeMultiplier = self.slowCPUFactor  # 2 GHz
                # get that shortest remaining process
                process = self.incomingProcesses.pop(0)
                # set the time when the process is complete
                process.startTime = self.currentTime
                print("Simo time {}: Process {} starts on CPU {}".format(self.currentTime, process, cpu))
                # time is * by burstTimeMultiplier, to set the process for x2 time on slower CPU
                completedEvent = ProcessDoneEvent(process, self.currentTime + process.RemCPUCycles * burstTimeMultiplier, cpu)
                # put an event to the queue
                self.events.append(completedEvent)
                self.contextSwitches += 1
            # after all slow CPUs are busy with short jobs, put longer jobs to faster CPUs
            elif len(self.incomingProcesses) > 0 and len(self.idleFastCPUs) > 0:
                # try to load the fast CPU
                # get the CPU
                burstTimeMultiplier = 1  # 4 GHz
                cpu = self.idleFastCPUs.popleft()
                # get that shortest remaining process
                process = self.incomingProcesses.pop(0)
                # set the time when the process is complete
                process.startTime = self.currentTime
                print("Simo time {}: Process {} starts on CPU {}".format(self.currentTime, process, cpu))
                # time is * by burstTimeMultiplier, to set the process for x2 time on slower CPU
                completedEvent = ProcessDoneEvent(process, self.currentTime + process.RemCPUCycles * burstTimeMultiplier, cpu)
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
                if cpu in self.fastCPUs:
                    self.idleFastCPUs.append(cpu)
                    # fast CPU became available
                    # check if it possible to switch a context of a process from slower CPU
                    if len(self.incomingProcesses) == 0 and len(self.idleSlowCPUs) < len(self.slowCPUs):
                        slowCPUEventIndex = None
                        for i, ev in enumerate(self.events):
                            # if the event occurs after the current time, meaning the process is still running
                            if ev.timestamp > self.currentTime and ev.cpu in self.slowCPUs:
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
                            self.idleSlowCPUs.append(cpu)
                            # get the fast CPU
                            cpu = self.idleFastCPUs.popleft()
                            # assign the process to the fast CPU
                            print("Simo time {}: Process {} starts on CPU {}".format(self.currentTime, process, cpu))
                            # time is * by burstTimeMultiplier, to set the process for x2 time on slower CPU
                            completedEvent = ProcessDoneEvent(process, self.currentTime + process.RemCPUCycles, cpu)
                            self.events.append(completedEvent)
                            self.contextSwitches += 1
                else:
                    self.idleSlowCPUs.append(cpu)





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
            # create a process, place in the queue
            simo.incomingProcesses.append(Process(PID, CPUCycles, MemorySize))
    # processes are read, ready to run
    simo.run()
    simo.printStats()
                    